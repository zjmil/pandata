use crate::pandata::Format;
use crate::pandata::{Args, FormatOptions};
use polars::io::SerReader;
use polars::prelude::{
    IntoLazy, JsonFormat as PolarsJsonFormat, JsonReader, LazyFrame, NDJsonWriterOptions,
};
use polars_plan::dsl::{FileWriteFormat, SinkDestination, SinkTarget, UnifiedSinkArgs};
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

        let lf = JsonReader::new(file)
            .with_json_format(PolarsJsonFormat::JsonLines)
            .finish()?
            .lazy();
        Ok(lf)
    }

    fn write(&self, path: &str, _args: &Args, lf: LazyFrame) -> anyhow::Result<()> {
        let options = NDJsonWriterOptions::default();
        lf.sink(
            SinkDestination::File {
                target: SinkTarget::Path(path.into()),
            },
            FileWriteFormat::NDJson(options),
            UnifiedSinkArgs::default(),
        )?
        .collect_with_engine(polars::prelude::Engine::Streaming)?;
        Ok(())
    }
}
