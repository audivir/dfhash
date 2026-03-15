use anyhow::Result;
use clap::Parser;
use colored::{Color, Colorize};
use dfhash::{compute_frame_hash, frame_to_csv, load_sorted_frame};
use mimalloc::MiMalloc;
use polars_core::prelude::DataFrame;
use similar::{ChangeTag, TextDiff};
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

#[cfg(test)]
mod main_tests;

#[derive(Debug, Clone, Copy)]
pub struct Context {
    pub equals: bool,
    pub print: bool,
    pub diff: bool,
    pub pager: bool,
    pub no_color: bool,
}

/// A deterministic content hasher for tabular data files.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Files to hash/check (CSV, Parquet, or gzip/zlib/zstd-compressed CSV).
    #[arg(required = true, num_args = 1..)]
    files: Vec<PathBuf>,

    /// Check if all files are semantically equal to each other.
    /// Returns 0 if all match, 1 otherwise.
    #[arg(long, short = 'e')]
    equals: bool,

    /// Print hashes additionally to checking equality. Only active with --equals.
    #[arg(long, short = 'p')]
    print: bool,

    /// Show a git-style diff between exactly two files.
    #[arg(long, short = 'd')]
    diff: bool,

    /// Pipe diff output through a pager (Unix only). Only active with --diff.
    /// Forces colored output if --no-color is not set (all OSes).
    #[arg(long)]
    pager: bool,

    /// Disable colored output
    #[arg(long)]
    no_color: bool,
}

pub fn run_diff(
    writer: &mut impl Write,
    error_writer: &mut impl Write,
    file_a: &Path,
    file_b: &Path,
    ctx: Context,
) -> Result<i32> {
    if ctx.pager {
        if !ctx.no_color {
            colored::control::set_override(true);
        }
        #[cfg(unix)]
        pager2::Pager::with_pager("less -R").setup();
    }

    let mut df1 = match load_sorted_frame(file_a) {
        Ok(df) => df,
        Err(e) => {
            writeln!(error_writer, "Error loading {}: {}", file_a.display(), e)?;
            return Ok(2);
        }
    };

    let mut df2 = match load_sorted_frame(file_b) {
        Ok(df) => df,
        Err(e) => {
            writeln!(error_writer, "Error loading {}: {}", file_b.display(), e)?;
            return Ok(2);
        }
    };

    let csv1 = String::from_utf8(frame_to_csv(&mut df1)?)?;
    let csv2 = String::from_utf8(frame_to_csv(&mut df2)?)?;

    let text_diff = TextDiff::from_lines(&csv1, &csv2);
    let mut has_diff = false;

    for change in text_diff.iter_all_changes() {
        match change.tag() {
            ChangeTag::Delete => {
                has_diff = true;
                // old_index refers to the line number in the first file (0-indexed)
                let line_num = change.old_index().unwrap() + 1;
                let formatted = format!("- {:<5} | {}", line_num, change.value());
                write!(writer, "{}", formatted.color(Color::Red))?;
            }
            ChangeTag::Insert => {
                has_diff = true;
                // new_index refers to the line number in the second file (0-indexed)
                let line_num = change.new_index().unwrap() + 1;
                let formatted = format!("+ {:<5} | {}", line_num, change.value());
                write!(writer, "{}", formatted.color(Color::Green))?;
            }
            ChangeTag::Equal => {
                // Explicitly ignore matching lines
            }
        }
    }

    Ok(if has_diff { 1 } else { 0 })
}

pub fn run(
    writer: &mut impl Write,
    error_writer: &mut impl Write,
    files: Vec<PathBuf>,
    ctx: Context,
) -> Result<i32> {
    if files.is_empty() {
        writeln!(error_writer, "Error: No files provided")?;
        return Ok(2);
    }

    if ctx.diff {
        if files.len() != 2 {
            writeln!(error_writer, "Error: --diff requires exactly two files.")?;
            return Ok(2);
        }
        return run_diff(writer, error_writer, &files[0], &files[1], ctx);
    }

    if ctx.equals && files.len() < 2 {
        writeln!(error_writer, "Error: --equals requires at least two files.")?;
        return Ok(2);
    }

    let mut exit_code = 0;
    let mut first_df: Option<DataFrame> = None;
    let mut files_match = true;

    let palette = [
        Color::Green,
        Color::Yellow,
        Color::Blue,
        Color::Magenta,
        Color::Cyan,
        Color::Red,
    ];
    let mut hash_colors: HashMap<String, Color> = HashMap::new();

    for path in &files {
        match load_sorted_frame(path) {
            Ok(mut df) => {
                if ctx.equals {
                    match &first_df {
                        Some(base_df) => {
                            if !base_df.equals_missing(&df) {
                                files_match = false;
                                if !ctx.print {
                                    break;
                                }
                            }
                        }
                        None => {
                            first_df = Some(df.clone());
                        }
                    }
                }

                if !ctx.equals || ctx.print {
                    match compute_frame_hash(&mut df) {
                        Ok(hash) => {
                            let output_line = format!("{}  {}", hash, path.display());
                            if ctx.no_color {
                                writeln!(writer, "{}", output_line)?;
                            } else {
                                let next_color_idx = hash_colors.len() % palette.len();
                                let color = *hash_colors
                                    .entry(hash.clone())
                                    .or_insert(palette[next_color_idx]);

                                writeln!(writer, "{}", output_line.color(color))?;
                            }
                        }
                        Err(e) => {
                            writeln!(error_writer, "Error hashing {}: {}", path.display(), e)?;
                            exit_code = 2;
                        }
                    }
                }
            }
            Err(e) => {
                writeln!(error_writer, "Error loading {}: {}", path.display(), e)?;
                exit_code = 2;
            }
        }
    }

    if ctx.equals {
        if exit_code != 0 {
            writeln!(
                error_writer,
                "Warning: Cannot check for equality due to previous errors."
            )?;
        } else if !files_match {
            writeln!(error_writer, "Error: Files do not match.")?;
            exit_code = 1;
        }
    }
    Ok(exit_code)
}

fn main() {
    let args = Args::parse();

    let ctx = Context {
        equals: args.equals,
        print: args.print,
        diff: args.diff,
        pager: args.pager,
        no_color: args.no_color,
    };

    let result = run(
        &mut std::io::stdout(),
        &mut std::io::stderr(),
        args.files,
        ctx,
    );

    match result {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            eprintln!("Unexpected error: {}", e);
            std::process::exit(2);
        }
    }
}
