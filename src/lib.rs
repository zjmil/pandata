use std::ffi::OsStr;
use std::path::Path;

mod csv;
mod json;
mod pandata;
mod parquet;

pub use csv::CsvFormat;
pub use json::JsonFormat;
pub use pandata::{Args, Format, FormatOptions, Pandata};
pub use parquet::ParquetFormat;

pub fn build_pandata() -> Pandata {
    let csv_format = CsvFormat::new();
    let json_format = JsonFormat::new();
    let parquet_format = ParquetFormat::new();

    let mut pandata = Pandata::new();
    pandata.add_format(Box::new(csv_format));
    pandata.add_format(Box::new(json_format));
    pandata.add_format(Box::new(parquet_format));

    pandata
}

fn parse_format_path(p: impl AsRef<Path>) -> Option<String> {
    p.as_ref()
        .extension()
        .map(OsStr::to_str)
        .flatten()
        .map(str::to_owned)
}

pub fn parse_format(format: Option<String>, input_path: &str) -> Option<String> {
    format.or_else(|| parse_format_path(&input_path))
}
