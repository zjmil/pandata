use std::fs::File;

use anyhow::{Context, Result};
use polars::prelude::{IntoLazy, JsonFormat, JsonReader, JsonWriterOptions, LazyFrame, SerReader};

use crate::pandata::{Args, Format, FormatOptions};

#[derive(Default)]
pub struct NdjsonFormat;

impl NdjsonFormat {
    pub fn new() -> Self {
        Self
    }
}

impl Format for NdjsonFormat {
    fn canonical_name(&self) -> &'static str {
        "ndjson"
    }

    fn read_options(&self) -> FormatOptions {
        FormatOptions::new()
    }

    fn read(&self, path: &str, _args: &Args) -> Result<LazyFrame> {
        // Polars 0.44 cannot stream an NDJSON scan into its streaming sinks.
        let file =
            File::open(path).with_context(|| format!("Unable to open NDJSON file: {path}"))?;
        Ok(JsonReader::new(file)
            .with_json_format(JsonFormat::JsonLines)
            .finish()
            .with_context(|| format!("Unable to read NDJSON file: {path}"))?
            .lazy())
    }

    fn write(&self, path: &str, _args: &Args, lf: LazyFrame) -> Result<()> {
        let options = JsonWriterOptions {
            maintain_order: true,
            ..Default::default()
        };
        lf.sink_json(path, options)
            .with_context(|| format!("Unable to write NDJSON file: {path}"))
    }
}
