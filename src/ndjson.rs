use std::fs::File;

use anyhow::{Context, Result};
use polars::prelude::{
    IntoLazy, JsonFormat, JsonReader, LazyFrame, NDJsonWriterOptions, SerReader,
};
use polars_plan::dsl::{FileWriteFormat, SinkDestination, SinkTarget, UnifiedSinkArgs};

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
        let file =
            File::open(path).with_context(|| format!("Unable to open NDJSON file: {path}"))?;
        Ok(JsonReader::new(file)
            .with_json_format(JsonFormat::JsonLines)
            .finish()
            .with_context(|| format!("Unable to read NDJSON file: {path}"))?
            .lazy())
    }

    fn write(&self, path: &str, _args: &Args, lf: LazyFrame) -> Result<()> {
        lf.sink(
            SinkDestination::File {
                target: SinkTarget::Path(path.into()),
            },
            FileWriteFormat::NDJson(NDJsonWriterOptions::default()),
            UnifiedSinkArgs::default(),
        )
        .and_then(|sink| sink.collect_with_engine(polars::prelude::Engine::Streaming))
        .with_context(|| format!("Unable to write NDJSON file: {path}"))?;
        Ok(())
    }
}
