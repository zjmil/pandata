use crate::pandata::{Args, Format, FormatOptions};
use polars::io::SerReader;
use polars::prelude::{CsvParseOptions, CsvReadOptions, CsvWriterOptions, IntoLazy, LazyFrame};
use std::path::PathBuf;

pub struct TsvFormat;

impl TsvFormat {
    pub fn new() -> Self {
        TsvFormat {}
    }
}

impl Format for TsvFormat {
    fn canonical_name(&self) -> &'static str {
        "tsv"
    }

    fn read_options(&self) -> FormatOptions {
        FormatOptions::from_keys(["separator", "quote-char"])
    }

    fn read(&self, path: &str, args: &Args) -> anyhow::Result<LazyFrame> {
        let mut parse_options = CsvParseOptions::default().with_separator(b'\t');
        if let Some(sep) = args.char("separator") {
            parse_options = parse_options.with_separator(sep)
        }
        if let Some(quote_char) = args.char("quote-char") {
            parse_options = parse_options.with_quote_char(Some(quote_char))
        }
        let read_options = CsvReadOptions::default().with_parse_options(parse_options);
        let lf = read_options
            .try_into_reader_with_file_path(Some(PathBuf::from(path)))?
            .finish()?
            .lazy();
        Ok(lf)
    }

    fn write(&self, path: &str, _args: &Args, lf: LazyFrame) -> anyhow::Result<()> {
        let mut options = CsvWriterOptions::default();
        options.maintain_order = true;
        options.serialize_options.separator = b'\t';
        lf.sink_csv(path, options)?;
        Ok(())
    }
}
