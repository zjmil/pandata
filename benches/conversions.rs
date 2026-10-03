use std::fs;
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, ensure, Context, Result};
use polars::prelude::{Column, DataFrame, IntoLazy, NamedFrom, Series};

use pandata::{build_pandata, Args, CsvFormat, Format, JsonFormat, ParquetFormat};

const ROWS: usize = 100_000;
const SAMPLES: usize = 15;
const CASES: [(&str, &str); 6] = [
    ("csv", "json"),
    ("csv", "parquet"),
    ("json", "csv"),
    ("json", "parquet"),
    ("parquet", "csv"),
    ("parquet", "json"),
];

struct TempDir(PathBuf);
impl TempDir {
    fn new() -> Result<Self> {
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path =
            std::env::temp_dir().join(format!("pandata-bench-{}-{stamp}", std::process::id()));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn format(name: &str) -> Result<Box<dyn Format>> {
    match name {
        "csv" => Ok(Box::new(CsvFormat::new())),
        "json" => Ok(Box::new(JsonFormat::new())),
        "parquet" => Ok(Box::new(ParquetFormat::new())),
        _ => bail!("Unknown format: {name}"),
    }
}

fn main() -> Result<()> {
    let mut cases = None;
    let mut result_path = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bench" => {} // Cargo supplies this argument.
            "--cases" => {
                cases = Some(
                    args.next()
                        .context("--cases needs a comma-separated list")?,
                )
            }
            "--output" => result_path = Some(args.next().context("--output needs a path")?),
            _ => bail!("Unknown argument: {arg}"),
        }
    }
    let selected: Vec<String> = match cases {
        Some(value) => value.split(',').map(str::to_owned).collect(),
        None => CASES.iter().map(|(a, b)| format!("{a}-to-{b}")).collect(),
    };
    for case in &selected {
        ensure!(
            CASES.iter().any(|(a, b)| *case == format!("{a}-to-{b}")),
            "Unknown case: {case}"
        );
    }
    let strings: Vec<&str> = include_str!("../tests/fixtures/benchmark-strings.txt")
        .lines()
        .collect();
    let expected = DataFrame::new(vec![
        Column::from(Series::new(
            "id".into(),
            (0..ROWS as i64).collect::<Vec<_>>(),
        )),
        Column::from(Series::new(
            "value".into(),
            (0..ROWS)
                .map(|i| i as f64 * 0.25 - 100.0)
                .collect::<Vec<_>>(),
        )),
        Column::from(Series::new(
            "active".into(),
            (0..ROWS).map(|i| i % 2 == 0).collect::<Vec<_>>(),
        )),
        Column::from(Series::new(
            "label".into(),
            (0..ROWS)
                .map(|i| strings[i % strings.len()])
                .collect::<Vec<_>>(),
        )),
    ])?;
    let temp = TempDir::new()?;
    for name in ["csv", "json", "parquet"] {
        let path = temp.0.join(format!("input.{name}"));
        format(name)?.write(
            path.to_str().context("Invalid temporary path")?,
            &Args::new(),
            expected.clone().lazy(),
        )?;
    }
    let pandata = build_pandata();
    let mut results = Vec::new();
    for (from, to) in CASES {
        let case = format!("{from}-to-{to}");
        if !selected.contains(&case) {
            continue;
        }
        let input = temp.0.join(format!("input.{from}"));
        let output = temp.0.join(format!("output.{to}"));
        let input = input.to_str().context("Invalid input path")?;
        let output = output.to_str().context("Invalid output path")?;
        let validate = || -> Result<()> {
            let actual = format(to)?.read(output, &Args::new())?.collect()?;
            ensure!(
                expected.equals_missing(&actual),
                "Invalid output for {case}"
            );
            Ok(())
        };
        pandata.convert(input, output, from, to)?;
        validate()?;
        let mut samples = Vec::new();
        for _ in 0..SAMPLES {
            let start = Instant::now();
            pandata.convert(input, output, from, to)?;
            let seconds = start.elapsed().as_secs_f64();
            ensure!(
                seconds.is_finite() && seconds > 0.0,
                "Invalid timing for {case}"
            );
            samples.push(seconds);
            validate()?;
        }
        let mut sorted = samples.clone();
        sorted.sort_by(f64::total_cmp);
        let median = sorted[SAMPLES / 2];
        let throughput = ROWS as f64 / median;
        ensure!(throughput.is_finite(), "Invalid throughput for {case}");
        println!("{case:20} {median:.6} s  {throughput:.0} rows/s");
        results.push(format!("\"{case}\":{{\"median_seconds\":{median},\"rows_per_second\":{throughput},\"samples_seconds\":{samples:?}}}"));
    }
    if let Some(path) = result_path {
        fs::write(
            path,
            format!("{{\"rows\":{ROWS},\"cases\":{{{}}}}}\n", results.join(",")),
        )?;
    }
    Ok(())
}
