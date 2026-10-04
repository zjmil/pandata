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

## Arrow IPC / Feather V2

Convert Arrow IPC files using `ipc`, `arrow`, or `feather` extensions, or specify
those names with `--from` and `--to`. The optional `ipc` feature is enabled by
default. IPC record batch streams and legacy Feather V1 are unsupported.
Reads materialize data in memory; writes use streaming sinks.

```sh
pandata input.csv output.feather
pandata input.arrow output.parquet
cargo build --no-default-features --features csv,parquet,ipc
```

Include `parquet` in custom feature builds: Polars 0.44's existing cloud/IPC
feature combination fails to compile without it.
