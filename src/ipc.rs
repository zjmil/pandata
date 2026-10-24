use std::fs::File;

use anyhow::{Context, Result};
use polars::prelude::{IntoLazy, IpcReader, IpcWriterOptions, LazyFrame, SerReader};
use polars_plan::dsl::{FileWriteFormat, SinkDestination, SinkTarget, UnifiedSinkArgs};

use crate::pandata::{Args, Format, FormatOptions};

#[derive(Default)]
pub struct IpcFormat;

impl IpcFormat {
    pub fn new() -> Self {
        Self
    }
}

impl Format for IpcFormat {
    fn canonical_name(&self) -> &'static str {
        "ipc"
    }

    fn read_options(&self) -> FormatOptions {
        FormatOptions::new()
    }

    fn read(&self, path: &str, _args: &Args) -> Result<LazyFrame> {
        let file = File::open(path).with_context(|| format!("Unable to open IPC file: {path}"))?;
        Ok(IpcReader::new(file)
            .finish()
            .with_context(|| format!("Unable to read IPC file: {path}"))?
            .lazy())
    }

    fn write(&self, path: &str, _args: &Args, lf: LazyFrame) -> Result<()> {
        lf.sink(
            SinkDestination::File {
                target: SinkTarget::Path(path.into()),
            },
            FileWriteFormat::Ipc(IpcWriterOptions::default()),
            UnifiedSinkArgs::default(),
        )
        .and_then(|sink| sink.collect_with_engine(polars::prelude::Engine::Streaming))
        .with_context(|| format!("Unable to write IPC file: {path}"))?;
        Ok(())
    }
}
