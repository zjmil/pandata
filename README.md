# pandata

pandata is like pandoc for data. We keep format conversion simple so you can move between common data formats with one command.

## How to install

Use Cargo so anyone can install the CLI locally.

```
cargo install pandata
```

## Usage

Convert files by extension when possible.

```
pandata input.csv output.parquet
```

Use `-` for stdin/stdout, and specify formats when extensions are missing.

```
pandata --from json --to csv - output.csv
pandata input.parquet - --to json
```
