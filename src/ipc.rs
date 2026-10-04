use std::fs::File;

use anyhow::{Context, Result};
use polars::prelude::{IntoLazy, IpcReader, IpcWriterOptions, LazyFrame, SerReader};

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
        // Polars 0.44 cannot stream an IPC scan into its streaming sinks.
        let file = File::open(path).with_context(|| format!("Unable to open IPC file: {path}"))?;
        Ok(IpcReader::new(file)
            .finish()
            .with_context(|| format!("Unable to read IPC file: {path}"))?
            .lazy())
    }

    fn write(&self, path: &str, _args: &Args, lf: LazyFrame) -> Result<()> {
        let options = IpcWriterOptions {
            maintain_order: true,
            ..Default::default()
        };
        lf.sink_ipc(path, options)
            .with_context(|| format!("Unable to write IPC file: {path}"))
    }
}
