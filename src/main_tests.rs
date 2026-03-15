use super::{run, Context};
use anyhow::Result;
use regex::Regex;
use rstest::rstest;
use std::fs;
use std::path::{Path, PathBuf};

const A_BASE: &str = "fixtures/a_base.csv";
const A_BASE_GZ: &str = "fixtures/a_base.csv.gz";
const A_BASE_ZST: &str = "fixtures/a_base.csv.zst";
const A_BASE_PARQUET: &str = "fixtures/a_base.parquet";
const B_DIFF: &str = "fixtures/b_diff.csv";
const C_ORDER: &str = "fixtures/c_order.csv";
const D_NULLS: &str = "fixtures/d_nulls.csv";
const E_NULLS_ORDER: &str = "fixtures/e_nulls_order.csv";
const NONEXISTENT: &str = "/nonexistent";
const NODATAFRAME: &str = "fixtures/no_dataframe";

// Get the corresponding .hash file for each input
fn get_expected_hash(file_path: &str) -> String {
    let path = Path::new(file_path);
    let file_stem = path.file_name().unwrap().to_str().unwrap();

    // Extract the base name before the first dot (e.g., "a_base")
    let base_name = file_stem.split('.').next().unwrap();
    let hash_path = path.with_file_name(format!("{}.hash", base_name));

    fs::read_to_string(&hash_path)
        .unwrap_or_else(|_| panic!("Missing expected hash file: {}", hash_path.display()))
        .trim()
        .to_string()
}

#[rstest]
#[case(vec![], false, 2, "^Error: No files provided\n$")]
#[case(vec![A_BASE], false, 0, "")]
#[case(vec![A_BASE], true, 2, "^Error: --equals requires at least two files\\.\n$")]
#[case(vec![NONEXISTENT], false, 2, "^Error loading [/\\\\]nonexistent: .*\n$")]
#[case(vec![NODATAFRAME], false, 2, "^Error loading fixtures[/\\\\]no_dataframe: Failed to collect DataFrame\n$")]
#[case(vec![A_BASE, A_BASE], false, 0, "")]
#[case(vec![A_BASE, A_BASE], true, 0, "")]
#[case(vec![A_BASE, C_ORDER], false, 0, "")] // Different row order, same semantic content
#[case(vec![A_BASE, C_ORDER], true, 0, "")]
#[case(vec![D_NULLS, E_NULLS_ORDER], false, 0, "")] // Nulls handled correctly
#[case(vec![D_NULLS, E_NULLS_ORDER], true, 0, "")]
#[case(vec![A_BASE, B_DIFF], false, 0, "")]
#[case(vec![A_BASE, B_DIFF], true, 1, "^Error: Files do not match\\.\n$")] // Mismatch
#[case(vec![A_BASE, A_BASE_PARQUET, A_BASE_GZ, A_BASE_ZST], false, 0, "")]
#[case(vec![A_BASE, A_BASE_PARQUET, A_BASE_GZ, A_BASE_ZST], true, 0, "")]
#[case(vec![A_BASE, NONEXISTENT], false, 2, "^Error loading [/\\\\]nonexistent: .*\n$")]
#[case(vec![A_BASE, NONEXISTENT], true, 2, "^Error loading [/\\\\]nonexistent: .*\nWarning: Cannot check for equality due to previous errors\\.\n$")]
fn test_run(
    #[values(true, false)] print: bool,
    #[case] files: Vec<&str>,
    #[case] equals: bool,
    #[case] expected_code: i32,
    #[case] expected_stderr: &str,
) -> Result<()> {
    let file_paths: Vec<PathBuf> = files.iter().map(PathBuf::from).collect();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let ctx = Context {
        equals,
        print,
        diff: false,
        pager: false,
        no_color: true,
    };

    let exit_code = run(&mut stdout, &mut stderr, file_paths, ctx)?;

    let stdout_str = String::from_utf8(stdout)?;
    let stderr_str = String::from_utf8(stderr)?;

    assert_eq!(exit_code, expected_code, "Exit code mismatch");

    if files.is_empty() || (equals && files.len() < 2) {
        assert!(
            stdout_str.is_empty(),
            "Stdout should be empty on arg errors"
        );
    } else if equals && !print {
        assert!(
            stdout_str.is_empty(),
            "Stdout should be empty when equals=true and print=false"
        );
    } else {
        let mut expected_stdout = String::new();
        for f in &files {
            if matches!(*f, NONEXISTENT | NODATAFRAME) {
                continue;
            }
            let hash = get_expected_hash(f);
            let display_path = PathBuf::from(f);
            expected_stdout.push_str(&format!("{}  {}\n", hash, display_path.display()));
        }
        assert_eq!(
            stdout_str, expected_stdout,
            "Stdout did not match expected hash file outputs"
        );
    }

    // Evaluate expected stderr (still using regex since error messages contain variable data/paths)
    if expected_stderr.is_empty() {
        assert!(
            stderr_str.is_empty(),
            "Stderr should be empty, got:\n{}",
            stderr_str
        );
    } else {
        let re = Regex::new(expected_stderr).unwrap();
        assert!(
            re.is_match(&stderr_str),
            "Stderr did not match expected pattern:\nRegex: {}\nStderr: {}",
            expected_stderr,
            stderr_str
        );
    }

    Ok(())
}

#[rstest]
#[case(vec![A_BASE, B_DIFF], 1, true)] // Mismatching content -> diff printed, code 1
#[case(vec![A_BASE, A_BASE], 0, false)] // Identical content -> nothing printed, code 0
#[case(vec![A_BASE, C_ORDER], 0, false)] // Diff respects polars sorting -> nothing printed, code 0
#[case(vec![A_BASE, A_BASE_PARQUET], 0, false)] // Formats abstracted away -> nothing printed, code 0
fn test_diff_mode(
    #[case] files: Vec<&str>,
    #[case] expected_code: i32,
    #[case] expect_stdout: bool,
) -> Result<()> {
    let file_paths: Vec<PathBuf> = files.iter().map(PathBuf::from).collect();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let ctx = Context {
        equals: false,
        print: false,
        diff: true,
        pager: false,
        no_color: true,
    };

    let exit_code = run(&mut stdout, &mut stderr, file_paths, ctx)?;

    assert_eq!(exit_code, expected_code);

    if expect_stdout {
        assert!(
            !stdout.is_empty(),
            "Expected diff output in stdout, got empty"
        );
    } else {
        assert!(
            stdout.is_empty(),
            "Expected empty stdout, got:\n{}",
            String::from_utf8(stdout)?
        );
    }

    Ok(())
}

#[test]
fn test_hash_palette_coloring() -> Result<()> {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();

    let file_paths: Vec<std::path::PathBuf> = vec![A_BASE, A_BASE_PARQUET, B_DIFF]
        .into_iter()
        .map(std::path::PathBuf::from)
        .collect();

    let ctx = Context {
        equals: false,
        print: false,
        diff: false,
        pager: false,
        no_color: false,
    };

    let exit_code = run(&mut stdout, &mut stderr, file_paths, ctx)?;

    assert_eq!(exit_code, 0, "Expected exit code 0");

    let stdout_str = String::from_utf8(stdout)?;
    let lines: Vec<&str> = stdout_str.trim().lines().collect();

    assert_eq!(lines.len(), 3, "Expected exactly three lines of output");

    // first two files: green, third file: yellow
    assert!(
        lines[0].starts_with("\x1b[32m"),
        "First line did not start with Green ANSI code"
    );
    assert!(
        lines[1].starts_with("\x1b[32m"),
        "Second line did not start with Green ANSI code"
    );
    assert!(
        lines[2].starts_with("\x1b[33m"),
        "Third line did not start with Green ANSI code"
    );

    Ok(())
}
