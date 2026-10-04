use std::ffi::OsStr;
use std::path::Path;

#[cfg(feature = "csv")]
mod csv;
#[cfg(feature = "ipc")]
mod ipc;
#[cfg(feature = "json")]
mod json;
#[cfg(feature = "ndjson")]
mod ndjson;
mod pandata;
#[cfg(feature = "parquet")]
mod parquet;
#[cfg(feature = "tsv")]
mod tsv;

#[cfg(feature = "avro")]
mod avro;

#[cfg(feature = "csv")]
pub use csv::CsvFormat;
#[cfg(feature = "ipc")]
pub use ipc::IpcFormat;
#[cfg(feature = "json")]
pub use json::JsonFormat;
#[cfg(feature = "ndjson")]
pub use ndjson::NdjsonFormat;
pub use pandata::{Args, Format, FormatOptions, Pandata};
#[cfg(feature = "parquet")]
pub use parquet::ParquetFormat;
#[cfg(feature = "tsv")]
pub use tsv::TsvFormat;

#[cfg(feature = "avro")]
pub use avro::AvroFormat;

pub fn build_pandata() -> Pandata {
    let mut pandata = Pandata::new();

    #[cfg(feature = "csv")]
    pandata.add_format(Box::new(CsvFormat::new()));
    #[cfg(feature = "json")]
    pandata.add_format(Box::new(JsonFormat::new()));
    #[cfg(feature = "parquet")]
    pandata.add_format(Box::new(ParquetFormat::new()));
    #[cfg(feature = "tsv")]
    pandata.add_format(Box::new(TsvFormat::new()));
    #[cfg(feature = "avro")]
    pandata.add_format(Box::new(AvroFormat::new()));

    #[cfg(feature = "ipc")]
    pandata.add_format(Box::new(IpcFormat::new()));
    #[cfg(feature = "ndjson")]
    pandata.add_format(Box::new(NdjsonFormat::new()));

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
