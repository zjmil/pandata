use crate::pandata::Format;
use crate::pandata::{Args, FormatOptions};
use polars::io::SerReader;
use polars::prelude::{IntoLazy, JsonReader, JsonWriterOptions, LazyFrame};
use std::fs::File;

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

    fn read(&self, path: &str, _args: &Args) -> anyhow::Result<LazyFrame> {
        let file = File::open(path)?;

        let lf = JsonReader::new(file).finish()?.lazy();
        Ok(lf)
    }

    fn write(&self, path: &str, _args: &Args, lf: LazyFrame) -> anyhow::Result<()> {
        let options = JsonWriterOptions::default();
        lf.sink_json(path, options)?;
        Ok(())
    }
}
