use anyhow::{Context, Result};
use polars::prelude::*;
use sha2::{Digest, Sha256};
use std::io::{Cursor, Write};
use std::path::Path;
/// load a file into a lazy frame
fn load_frame(path: &Path) -> Result<LazyFrame> {
    let path_str = path.to_str().context("Invalid path string")?;
    let pl_path = PlPath::from_str(path_str);

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

/// Computes the hash of a dataframe
pub fn compute_frame_hash(df: &mut DataFrame) -> Result<String> {
    let mut buffer = Cursor::new(Vec::new());
    CsvWriter::new(&mut buffer)
        .include_header(true)
        .finish(df)
        .context("Failed to serialize DataFrame")?;

    let mut hasher = Sha256::new();
    hasher.write_all(buffer.get_ref())?;
    Ok(hex::encode(hasher.finalize()))
}
