use anyhow::Result;
use clap::Parser;
use dfhash::{compute_frame_hash, load_sorted_frame};
use polars::prelude::DataFrame;
use std::io::Write;
use std::path::PathBuf;

#[cfg(test)]
mod main_tests;

/// A deterministic content hasher for tabular data files.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Files to hash/check (CSV, Parquet, or Zstd-compressed CSV).
    #[arg(required = true, num_args = 1..)]
    files: Vec<PathBuf>,

    /// Check if all files are semantically equal to each other.
    /// Returns 0 if all match, 1 otherwise.
    #[arg(long, short = 'e', group = "action", verbatim_doc_comment)]
    equals: bool,
}

pub fn run(
    writer: &mut impl Write,
    error_writer: &mut impl Write,
    files: Vec<PathBuf>,
    equals: bool,
) -> Result<i32> {
    if files.is_empty() {
        writeln!(error_writer, "Error: No files provided")?;
        return Ok(2);
    }

    if equals && files.len() < 2 {
        writeln!(error_writer, "Error: --equal requires at least two files.")?;
        return Ok(1);
    }

    let mut exit_code = 0;

    let mut frames: Vec<(&PathBuf, DataFrame)> = Vec::with_capacity(files.len());
    for path in &files {
        match load_sorted_frame(path) {
            Ok(df) => frames.push((path, df)),
            Err(e) => {
                writeln!(error_writer, "Error loading {}: {}", path.display(), e)?;
                exit_code = 2;
            }
        }
    }

    if equals {
        if exit_code != 0 {
            writeln!(
                error_writer,
                "Warning: Cannot check for equality due to previous errors."
            )?;
        } else {
            let first_df = &frames[0].1;
            let all_equal = frames.iter().skip(1).all(|(_, df)| first_df.equals(df));

            if all_equal {
                return Ok(0);
            } else {
                writeln!(error_writer, "Error: Files do not match.")?;
                exit_code = 1;
            }
        }
    }

    for (path, mut df) in frames {
        match compute_frame_hash(&mut df) {
            Ok(hash) => {
                writeln!(writer, "{}  {}", hash, path.display())?;
            }
            Err(e) => {
                writeln!(error_writer, "Error hashing {}: {}", path.display(), e)?;
                exit_code = 2;
            }
        }
    }

    Ok(exit_code)
}

fn main() {
    let args = Args::parse();
    let result = run(
        &mut std::io::stdout(),
        &mut std::io::stderr(),
        args.files,
        args.equals,
    );
    match result {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            eprintln!("Unexpected error: {}", e);
            std::process::exit(2);
        }
    }
}
