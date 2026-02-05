use super::run;
use anyhow::Result;
use flate2::write::GzEncoder;
use flate2::Compression;
use polars::prelude::*;
use regex::Regex;
use rstest::rstest;
use std::fs::File;
use std::io::{Cursor, Write};
use std::path::PathBuf;
use tempfile::TempDir;
enum FileFormat {
    Csv,
    Parquet,
    CsvZst,
    CsvGz,
}

type Content = (&'static str, FileFormat);
type PathElem = (&'static str, Content);

const BASE: Content = ("a,b\n1,2\n3,4", FileFormat::Csv);
const REORDERED: Content = ("a,b\n3,4\n1,2", FileFormat::Csv);
const DIFFERENT: Content = ("a,b\n1,2\n9,9", FileFormat::Csv);
const BASE_PARQUET: Content = ("a,b\n1,2\n3,4", FileFormat::Parquet);
const BASE_CSVZST: Content = ("a,b\n1,2\n3,4", FileFormat::CsvZst);
const BASE_CSVGZ: Content = ("a,b\n1,2\n3,4", FileFormat::CsvGz);

const BASE_A: PathElem = ("a.csv", BASE);
const BASE_B: PathElem = ("b.csv", BASE);
const REORDERED_B: PathElem = ("b.csv", REORDERED);
const DIFFERENT_B: PathElem = ("b.csv", DIFFERENT);
const BASE_PARQUET_B: PathElem = ("b.parquet", BASE_PARQUET);
const BASE_CSVZST_B: PathElem = ("b.csv.zst", BASE_CSVZST);
const BASE_CSVGZ_B: PathElem = ("b.csv.gz", BASE_CSVGZ);
const NONEXISTENT: PathElem = ("nonexistent", ("", FileFormat::Csv));

/// Create different file formats from the same CSV content
fn create_file(dir: &TempDir, name: &str, content: &str, format: FileFormat) -> std::path::PathBuf {
    let path = dir.path().join(name);
    let mut file = File::create(&path).unwrap();

    match format {
        FileFormat::Csv => {
            // write raw csv string to file
            file.write_all(content.as_bytes()).unwrap();
        }
        FileFormat::CsvZst => {
            // compress raw csv string into zstd
            let mut encoder = zstd::Encoder::new(file, 0).unwrap();
            encoder.write_all(content.as_bytes()).unwrap();
            encoder.finish().unwrap();
        }
        FileFormat::CsvGz => {
            // compress raw csv string into gzip
            let mut encoder = GzEncoder::new(file, Compression::default());
            encoder.write_all(content.as_bytes()).unwrap();
            encoder.finish().unwrap();
        }
        FileFormat::Parquet => {
            // load content into Polars DataFrame write out as Parquet
            let cursor = Cursor::new(content.as_bytes());
            let mut df = CsvReader::new(cursor).finish().unwrap();
            ParquetWriter::new(&mut file).finish(&mut df).unwrap();
        }
    }
    path
}

#[rstest]
#[case(vec![], false, 2, "", "^Error: No files provided\\n$")]
#[case(vec![BASE_A], false, 0, "^b94\\w+ [\\w/\\\\. ]+/a.csv\\n$", "")]
#[case(vec![NONEXISTENT], false, 2, "", "^Error loading nonexistent: [\\w():/\\\\.' ]+\\n$")]
#[case(vec![BASE_A], true, 1, "", "^Error: --equal requires at least two files\\.\\n$")]
#[case(vec![BASE_A, BASE_B], false, 0, "^b94\\w+ [\\w/\\\\. ]+/a.csv\\nb94\\w+ [\\w/\\\\. ]+/b.csv\\n$", "")]
#[case(vec![BASE_A, BASE_B], true, 0, "", "")]
#[case(vec![BASE_A, BASE_B, NONEXISTENT], false, 2, "^b94\\w+ [\\w/\\\\. ]+/a.csv\\nb94\\w+ [\\w/\\\\. ]+/b.csv\\n$", "^Error loading nonexistent: [\\w():/\\\\.' ]+\\n$")]
#[case(vec![BASE_A, BASE_B, NONEXISTENT], true, 2, "^b94\\w+ [\\w/\\\\. ]+/a.csv\\nb94\\w+ [\\w/\\\\. ]+/b.csv\\n$", "^Error loading nonexistent: [\\w():/\\\\.' ]+\\nWarning: Cannot check for equality due to previous errors.\\n$")]
#[case(vec![BASE_A, REORDERED_B], false, 0, "^b94\\w+ [\\w/\\\\. ]+/a.csv\\nb94\\w+ [\\w/\\\\. ]+/b.csv\\n$", "")]
#[case(vec![BASE_A, REORDERED_B], true, 0, "", "")]
#[case(vec![BASE_A, DIFFERENT_B], false, 0, "^b94\\w+ [\\w/\\\\. ]+/a.csv\\n3ac\\w+ [\\w/\\\\. ]+/b.csv\\n$", "")]
#[case(vec![BASE_A, DIFFERENT_B], true, 1, "^b94\\w+ [\\w/\\\\. ]+/a.csv\\n3ac\\w+ [\\w/\\\\. ]+/b.csv\\n$", "^Error: Files do not match.\\n$")]
#[case(vec![BASE_A, BASE_PARQUET_B], false, 0, "^b94\\w+ [\\w/\\\\. ]+/a.csv\\nb94\\w+ [\\w/\\\\. ]+/b.parquet\\n$", "")]
#[case(vec![BASE_A, BASE_PARQUET_B], true, 0, "", "")]
#[case(vec![BASE_A, BASE_CSVZST_B], false, 0, "^b94\\w+ [\\w/\\\\. ]+/a.csv\\nb94\\w+ [\\w/\\\\. ]+/b.csv.zst\\n$", "")]
#[case(vec![BASE_A, BASE_CSVZST_B], true, 0, "", "")]
#[case(vec![BASE_A, BASE_CSVGZ_B], false, 0, "^b94\\w+ [\\w/\\\\. ]+/a.csv\\nb94\\w+ [\\w/\\\\. ]+/b.csv.gz\\n$", "")]
#[case(vec![BASE_A, BASE_CSVGZ_B], true, 0, "", "")]
fn test_run(
    #[case] args: Vec<PathElem>,
    #[case] equals: bool,
    #[case] expected_code: i32,
    #[case] expected_stdout: &str,
    #[case] expected_stderr: &str,
) -> Result<()> {
    let temp = TempDir::new()?;

    let mut file_paths: Vec<PathBuf> = Vec::new();
    for (name, (content, format)) in args {
        if name == "nonexistent" {
            file_paths.push(PathBuf::from("nonexistent"));
            continue;
        }
        let p = create_file(&temp, name, content, format);
        file_paths.push(p);
    }

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let exit_code = run(&mut stdout, &mut stderr, file_paths, equals)?;

    let stdout_str = String::from_utf8(stdout)?;
    let stderr_str = String::from_utf8(stderr)?;

    assert_eq!(exit_code, expected_code, "Exit code mismatch");

    if !expected_stdout.is_empty() {
        assert!(
            Regex::new(expected_stdout).unwrap().is_match(&stdout_str),
            "Stdout missing content.\nGot: {}",
            stdout_str
        );
    } else {
        assert_eq!(
            stdout_str, "",
            "Stdout should be empty.\nGot: {}",
            stdout_str
        );
    }

    if !expected_stderr.is_empty() {
        assert!(
            Regex::new(expected_stderr).unwrap().is_match(&stderr_str),
            "Stderr missing content.\nGot: {}",
            stderr_str
        );
    } else {
        assert_eq!(
            stderr_str, "",
            "Stderr should be empty.\nGot: {}",
            stderr_str
        );
    }

    Ok(())
}
