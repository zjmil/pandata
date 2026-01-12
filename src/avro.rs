use crate::pandata::{Args, Format, FormatOptions};
use polars::io::avro::{AvroReader, AvroWriter};
use polars::io::{SerReader, SerWriter};
use polars::prelude::{IntoLazy, LazyFrame};
use std::fs::File;

pub struct AvroFormat;

impl AvroFormat {
    pub fn new() -> Self {
        AvroFormat {}
    }
}

impl Format for AvroFormat {
    fn canonical_name(&self) -> &'static str {
        "avro"
    }

    fn read_options(&self) -> FormatOptions {
        FormatOptions::new()
    }

    fn read(&self, path: &str, _args: &Args) -> anyhow::Result<LazyFrame> {
        let file = File::open(path)?;
        let df = AvroReader::new(file).finish()?;
        Ok(df.lazy())
    }

    fn write(&self, path: &str, _args: &Args, lf: LazyFrame) -> anyhow::Result<()> {
        let mut file = File::create(path)?;
        let mut df = lf.collect()?;
        df.as_single_chunk_par();
        AvroWriter::new(&mut file)
            .with_name("pandata".to_string())
            .finish(&mut df)?;
        Ok(())
    }
}
