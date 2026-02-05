# dfhash

A deterministic content hasher for tabular data files.

**dfhash** is a CLI tool designed to verify data integrity across different DataFrame file formats.
It loads dataframes using **Polars**, sorts them by all columns to ensure determinism, and computes a stable SHA256 hash.
It supports CSV, Parquet, and gzip/zlib/zstd-compressed CSV files.
For consistency with CSV, null values are treated as equal to each other.

## Installation

### From Source

Ensure you have Rust installed.

```bash
git clone https://github.com/audivir/dfhash
cd dfhash
# https://github.com/pola-rs/polars/issues/26348
rustup default nightly-2026-01-28
RUSTFLAGS="-C target-cpu=native" cargo build --release -j$(nproc)
cp target/release/dfhash ~/.local/bin/
```

## Usage

```bash
# compute hash for a single file
dfhash data.csv

# compute hashes for multiple files (mixed formats supported)
dfhash data.csv backup.parquet archived.csv.zst

# check if files are semantically equal
# (returns 0 if all match, 1 otherwise)
dfhash --equals source.csv derived.parquet
```

### Options

| Flag             | Description                                                                                   |
| ---------------- | --------------------------------------------------------------------------------------------- |
| `-e`, `--equals` | Check if all files are semantically equal to each other. Returns 0 if all match, 1 otherwise. |
| `-p`, `--print`  | Print hashes additionally to checking equality (ignored if `--equals` is not set).            |
| `<files>`        | Files to hash/check. (CSV, Parquet, or gzip/zlib/zstd-compressed CSV).                        |

## License

MIT License. See [LICENSE](LICENSE) for details.

## TODO

- add a fast hash method which randomly (with seed!) samples rows and columns to compute hash
