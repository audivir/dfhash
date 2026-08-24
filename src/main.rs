use clap::Parser;
use dfhash::{run, Context};
use mimalloc::MiMalloc;
use std::path::PathBuf;

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

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

    /// Print hashes additionally to checking equality. Only active with `--equals`.
    #[arg(long, short = 'p')]
    print: bool,

    /// Show a git-style diff between exactly two files.
    #[arg(long, short = 'd')]
    diff: bool,

    /// Pipe diff output through a pager (Unix only). Only active with `--diff`.
    /// Forces colored output if `--no-color` is not set (all OSes).
    #[arg(long)]
    pager: bool,

    /// Disable colored output
    #[arg(long)]
    no_color: bool,
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
