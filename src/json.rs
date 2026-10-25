use std::fs::File;
use std::io::BufWriter;

use anyhow::{Context, Result};
use polars::prelude::{
    IntoLazy, JsonFormat as PolarsJsonFormat, JsonReader, JsonWriter, LazyFrame, SerReader,
    SerWriter,
};

use crate::pandata::{Args, Format, FormatOptions};

pub struct JsonFormat;

impl JsonFormat {
    pub fn new() -> Self {
        JsonFormat {}
    }
}

impl Format for JsonFormat {
    fn canonical_name(&self) -> &'static str {
        "json"
    }

    fn read_options(&self) -> FormatOptions {
        FormatOptions::new()
    }

    // Polars has no lazy scan for JSON arrays, so reads load the whole file.
    fn read(&self, path: &str, _args: &Args) -> Result<LazyFrame> {
        let file = File::open(path).with_context(|| format!("Unable to open JSON file: {path}"))?;
        Ok(JsonReader::new(file)
            .with_json_format(PolarsJsonFormat::Json)
            .finish()
            .with_context(|| format!("Unable to read JSON file: {path}"))?
            .lazy())
    }

    // Polars has no JSON array sink, so writes collect before serializing.
    fn write(&self, path: &str, _args: &Args, lf: LazyFrame) -> Result<()> {
        let mut df = lf
            .collect()
            .with_context(|| format!("Unable to collect data for JSON file: {path}"))?;
        let file =
            File::create(path).with_context(|| format!("Unable to create JSON file: {path}"))?;
        JsonWriter::new(BufWriter::new(file))
            .with_json_format(PolarsJsonFormat::Json)
            .finish(&mut df)
            .with_context(|| format!("Unable to write JSON file: {path}"))?;
        Ok(())
    }
}
