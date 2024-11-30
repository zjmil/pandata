use clap::Parser;
use std::ffi::OsStr;
use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::Result;
use polars::prelude::*;

enum Format {
    Csv,
    Json,
    Parquet,
    Sqlite,
}

impl Format {
    fn from_string(s: impl AsRef<str>) -> Option<Format> {
        match s.as_ref().to_lowercase().as_str() {
            "csv" => Some(Format::Csv),
            "json" => Some(Format::Json),
            "parquet" => Some(Format::Parquet),
            "sqlite" => Some(Format::Sqlite),
            _ => None
        }
    }

    fn from_path(p: impl AsRef<Path>) -> Option<Format> {
        p.as_ref()
            .extension()
            .map(OsStr::to_str)
            .flatten()
            .map(Self::from_string)
            .flatten()
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The file to read
    #[arg(value_name = "FROM_FILE")]
    from_file: Option<String>,

    /// The file to write
    #[arg(value_name = "TO_FILE")]
    to_file: Option<String>,

    /// Force the input format
    #[arg(short, long, value_name = "FORMAT")]
    from: Option<String>,

    /// Force the output format
    #[arg(short, long, value_name = "FORMAT")]
    to: Option<String>,
}


fn read_csv(path: &str) -> Result<LazyFrame> {
    // TODO: this crashes
    // let lf = LazyCsvReader::new(path)
    //     .with_has_header(true)
    //     .finish()?;
    let lf = CsvReadOptions::default()
        .try_into_reader_with_file_path(Some(PathBuf::from(path)))?
        .finish()?
        .lazy();
    Ok(lf)
}

fn read_json(path: &str) -> Result<LazyFrame> {
    let file = File::open(path)?;
    let lf = JsonReader::new(file).finish()?.lazy();
    Ok(lf)
}

fn read_parquet(path: &str) -> Result<LazyFrame> {
    let args = ScanArgsParquet::default();
    let lf = LazyFrame::scan_parquet(path, args)?;
    Ok(lf)
}

fn write_csv(path: &str, lf: LazyFrame) -> Result<()> {
    let options = CsvWriterOptions::default();
    lf.sink_csv(path, options)?;
    Ok(())
}

fn write_json(path: &str, lf: LazyFrame) -> Result<()> {
    let options = JsonWriterOptions::default();
    lf.sink_json(path, options)?;
    Ok(())
}

fn write_parquet(path: &str, lf: LazyFrame) -> Result<()> {
    let options = ParquetWriteOptions::default();
    lf.sink_parquet(path, options)?;
    Ok(())
}

fn read(format: Format, path: &str) -> Result<LazyFrame> {
    match format {
        Format::Csv => read_csv(path),
        Format::Json => read_json(path),
        Format::Parquet => read_parquet(path),
        _ => todo!(),
    }
}

fn write(format: Format, path: &str, lf: LazyFrame) -> Result<()> {
    match format {
        Format::Csv => write_csv(path, lf),
        Format::Json => write_json(path, lf),
        Format::Parquet => write_parquet(path, lf),
        _ => todo!(),
    }
}

fn convert(from_path: &str, to_path: &str, from_format: Format, to_format: Format) -> Result<()> {
    let df = read(from_format, from_path)?;
    write(to_format, to_path, df)
}

fn parse_format(format: Option<&String>, input_path: &str) -> Option<Format> {
    format.map(Format::from_string).flatten().or_else(|| Format::from_path(&input_path))
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let from_file = match cli.from_file {
        Some(ref x) if x != "-" => x,
        _ => "/dev/stdin"
    };
    let to_file = match cli.to_file {
        Some(ref x) if x != "-" => x,
        _ => "/dev/stdout"
    };

    let from_format = parse_format(cli.from.as_ref(), &from_file).expect("Unable to parse input format. Must be explicit if reading from stdin.");
    let to_format = parse_format(cli.to.as_ref(), &to_file).expect("Unable to parse output format. Must be explicit if writing to stdout.");

    println!("pandata: {:?}", cli);

    convert(from_file, to_file, from_format, to_format)?;

    Ok(())
}
