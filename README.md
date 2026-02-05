# dfhash

A deterministic content hasher for tabular data files.

**dfhash** is a CLI tool designed to verify data integrity across different DataFrame file formats.
It loads dataframes using **Polars**, sorts them by all columns to ensure determinism, and computes a stable SHA256 hash.
It supports CSV, Parquet, and Zstd-compressed CSV files.

## Installation

### From Source

Ensure you have Rust installed.

```bash
git clone https://github.com/audivir/dfhash
cd dfhash
cargo build --release
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
| `<files>`        | Files to hash/check. (CSV, Parquet, or Zstd-compressed CSV).                                  |

## License

MIT License. See [LICENSE](LICENSE) for details.
