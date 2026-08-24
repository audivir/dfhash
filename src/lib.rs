use anyhow::{Context as _, Result};
use colored::Color;
use polars_core::chunked_array::ops::SortMultipleOptions;
use polars_core::prelude::DataFrame;
use polars_io::prelude::CsvWriter;
use polars_io::SerWriter;
use polars_lazy::frame::ScanArgsParquet;
use polars_lazy::prelude::{LazyCsvReader, LazyFileListReader, LazyFrame};
use polars_utils::pl_path::PlRefPath;
use sha2::{Digest, Sha256};
use similar::{ChangeTag, TextDiff};
use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy)]
pub struct Context {
    pub equals: bool,
    pub print: bool,
    pub diff: bool,
    pub pager: bool,
    pub no_color: bool,
}

/// Load a file into a lazy frame
fn load_frame(path: &Path) -> Result<LazyFrame> {
    let path_str = path.to_str().context("Invalid path string")?;
    let pl_path = PlRefPath::from(path_str);

    if path_str.ends_with(".parquet") {
        let args = ScanArgsParquet::default();
        LazyFrame::scan_parquet(pl_path, args).context("Failed to scan parquet")
    } else {
        LazyCsvReader::new(pl_path)
            .finish()
            .context("Failed to scan CSV")
    }
}

/// Loads and sorts a file by all columns
pub fn load_sorted_frame(path: &Path) -> Result<DataFrame> {
    let mut lf = load_frame(path)?;

    // sort by every column to ensure deterministic order
    let schema = lf.collect_schema()?;
    let all_cols: Vec<String> = schema.iter_names().map(|name| name.to_string()).collect();
    let sorted_lf = lf.sort(
        &all_cols,
        SortMultipleOptions::default()
            .with_maintain_order(false)
            .with_multithreaded(true),
    );

    let df = sorted_lf.collect().context("Failed to collect DataFrame")?;
    Ok(df)
}

/// Serializes a DataFrame into a raw CSV byte vector
pub fn frame_to_csv(df: &mut DataFrame) -> Result<Vec<u8>> {
    let mut buffer = Cursor::new(Vec::new());
    CsvWriter::new(&mut buffer)
        .include_header(true)
        .finish(df)
        .context("Failed to serialize DataFrame")?;
    Ok(buffer.into_inner())
}

/// Computes the hash of a dataframe
pub fn compute_frame_hash(df: &mut DataFrame) -> Result<String> {
    let buffer = frame_to_csv(df)?;
    let mut hasher = Sha256::new();
    hasher.update(&buffer);
    Ok(hex::encode(hasher.finalize()))
}

/// Wraps `text` in `color`'s ANSI escape codes, unless `no_color` is set.
///
/// Deliberately avoids `colored`'s terminal auto-detection and global override, since both are
/// process-wide state that races when `run`/`run_diff` are called concurrently (e.g. in tests).
fn colorize(text: &str, color: Color, no_color: bool) -> String {
    if no_color {
        text.to_string()
    } else {
        format!("\x1b[{}m{}\x1b[0m", color.to_fg_str(), text)
    }
}

pub fn run_diff(
    writer: &mut impl Write,
    error_writer: &mut impl Write,
    file_a: &Path,
    file_b: &Path,
    ctx: Context,
) -> Result<i32> {
    if ctx.pager {
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
                write!(writer, "{}", colorize(&formatted, Color::Red, ctx.no_color))?;
            }
            ChangeTag::Insert => {
                has_diff = true;
                // new_index refers to the line number in the second file (0-indexed)
                let line_num = change.new_index().unwrap() + 1;
                let formatted = format!("+ {:<5} | {}", line_num, change.value());
                write!(writer, "{}", colorize(&formatted, Color::Green, ctx.no_color))?;
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

                                writeln!(writer, "{}", colorize(&output_line, color, false))?;
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
