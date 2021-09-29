use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::time::Instant;

use clap::{Clap, ValueHint};
use itertools::Itertools;
use lz4_flex::{compress_prepend_size, decompress_size_prepended};

use common::{Event, Link, Span};
use serde_json::Value;
use std::collections::HashMap;
use comfy_table::Table;
use std::fmt::{Display, Formatter};

mod arrow;
mod protobuf;

#[derive(Clap, Debug)]
#[clap(name = "trace")]
struct Opt {
    /// Maximum batch size
    #[clap(short, long, default_value = "1000")]
    batch_size: usize,

    /// JSON files to process
    #[clap(name = "FILE", parse(from_os_str), value_hint = ValueHint::AnyPath)]
    files: Vec<PathBuf>,
}

#[derive(Debug)]
pub struct BenchmarkResult {
    batch_count: usize,
    row_count: usize,
    total_infer_schema_ns: u128,
    total_buffer_creation_ns: u128,
    total_buffer_size: usize,
    total_buffer_serialization_ns: u128,
    total_buffer_compression_ns: u128,
    total_compressed_buffer_size: usize,
    total_buffer_decompression_ns: u128,
    total_buffer_deserialization_ns: u128,
}

#[derive(Debug)]
pub struct ArrowVsProto {
    file: String,
    arrow: BenchmarkResult,
    proto: BenchmarkResult
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::parse();
    let mut bench_results = vec![];

    opt.files.iter().for_each(|file| {
        let filename = file.as_path().display().to_string();
        let mut arrow_result = BenchmarkResult::new();
        let mut proto_result = BenchmarkResult::new();

        print!("Processing file '{}'...", filename);
        let reader = BufReader::new(File::open(file).unwrap());

        serde_json::Deserializer::from_reader(reader)
            .into_iter::<Span>()
            .flat_map(|span| span.ok())
            .chunks(opt.batch_size)
            .into_iter()
            .for_each(|chunk| {
                let spans: Vec<_> = chunk.collect();

                let result = bench_arrow(&spans, &mut arrow_result);
                if result.is_err() {
                    panic!("{:?}", result);
                } else {
                    arrow_result.batch_count += 1;
                    arrow_result.row_count += spans.len();
                }

                let result = bench_protobuf(&spans, &mut proto_result);
                if result.is_err() {
                    panic!("{:?}", result);
                } else {
                    proto_result.batch_count += 1;
                    proto_result.row_count += spans.len();
                }
            });

        bench_results.push(ArrowVsProto {
            file: filename,
            arrow: arrow_result,
            proto: proto_result
        });

        println!("DONE.");
    });

    render_benchmark_results(bench_results);

    if opt.files.is_empty() {
        dump_sample_data();
    }

    Ok(())
}

impl BenchmarkResult {
    pub fn new() -> Self {
        Self {
            batch_count: 0,
            row_count: 0,
            total_infer_schema_ns: 0,
            total_buffer_creation_ns: 0,
            total_buffer_size: 0,
            total_buffer_serialization_ns: 0,
            total_buffer_compression_ns: 0,
            total_compressed_buffer_size: 0,
            total_buffer_decompression_ns: 0,
            total_buffer_deserialization_ns: 0
        }
    }
}

impl Display for BenchmarkResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let result = format!(" \n{}\n{}\n{:.2}\n{:.2}\n{}\n{:.2}\n{:.2}\n{}\n{:.2}\n{:.2}",
                             self.batch_count,
                             self.row_count,
                             (self.total_infer_schema_ns as f64 / 1000000.0),
                             (self.total_buffer_creation_ns as f64 / 1000000.0),
                             self.total_buffer_size,
                             (self.total_buffer_serialization_ns as f64 / 1000000.0),
                             (self.total_buffer_compression_ns as f64 / 1000000.0),
                             self.total_compressed_buffer_size,
                             (self.total_buffer_decompression_ns as f64 / 1000000.0),
                             (self.total_buffer_deserialization_ns as f64 / 1000000.0)
        );
        f.write_str(&result)
    }
}

fn bench_arrow(spans: &[Span], bench_result: &mut BenchmarkResult) -> Result<(), Box<dyn std::error::Error>> {
    let buf = arrow::serialize(spans, bench_result)?;
    bench_result.total_buffer_size += buf.len();
    let start = Instant::now();
    let compressed_buf = compress_prepend_size(&buf);
    let elapse_time = Instant::now() - start;
    bench_result.total_compressed_buffer_size += compressed_buf.len();
    bench_result.total_buffer_compression_ns += elapse_time.as_nanos();
    let start = Instant::now();
    let buf = decompress_size_prepended(&compressed_buf).unwrap();
    let elapse_time = Instant::now() - start;
    bench_result.total_buffer_decompression_ns += elapse_time.as_nanos();
    arrow::deserialize(buf, bench_result);
    Ok(())
}

fn bench_protobuf(spans: &[Span], bench_result: &mut BenchmarkResult) -> Result<(), Box<dyn std::error::Error>> {
    let buf = protobuf::serialize(spans, bench_result)?;
    bench_result.total_buffer_size += buf.len();
    let start = Instant::now();
    let compressed_buf = compress_prepend_size(&buf);
    let elapse_time = Instant::now() - start;
    bench_result.total_compressed_buffer_size += compressed_buf.len();
    bench_result.total_buffer_compression_ns += elapse_time.as_nanos();
    let start = Instant::now();
    let buf = decompress_size_prepended(&compressed_buf).unwrap();
    let elapse_time = Instant::now() - start;
    bench_result.total_buffer_decompression_ns += elapse_time.as_nanos();
    protobuf::deserialize(buf, bench_result);
    Ok(())
}

fn dump_sample_data() {
    let mut attributes = HashMap::new();
    attributes.insert("label_1".into(), Value::String("<text>".into()));
    attributes.insert("label_2".into(), Value::String("<bool>".into()));
    attributes.insert("label_3".into(), Value::String("<number>".into()));

    let span = Span {
        trace_id: "<id>".to_string(),
        span_id: "<id>".to_string(),
        trace_state: Some("<state>".to_string()),
        parent_span_id: Some("<id>".to_string()),
        name: "<name>".to_string(),
        kind: Some(0),
        start_time_unix_nano: 1626371667388918000,
        end_time_unix_nano: Some(1626371667388918010),
        attributes: Some(attributes.clone()),
        dropped_attributes_count: Some(0),
        events: Some(vec![Event {
            time_unix_nano: 1626371667388918000,
            name: "<event_name>".to_string(),
            attributes: attributes.clone(),
            dropped_attributes_count: Some(0),
        }]),
        dropped_events_count: Some(0),
        links: Some(vec![Link {
            trace_id: "<id>".to_string(),
            span_id: "<id>".to_string(),
            trace_state: Some("<state>".into()),
            attributes,
            dropped_attributes_count: Some(0),
        }]),
        dropped_links_count: Some(0),
    };

    println!();
    println!("No argument file provided!");
    println!();
    println!("Please specify one or several line delimited JSON files containing span entities following the format below.");
    println!();
    println!("{}", serde_json::to_string(&span).unwrap());
    println!("{}", serde_json::to_string(&span).unwrap());
    println!("...");
    println!();
    println!("The following fields are optionals:");
    println!("- trace_state");
    println!("- parent_span_id");
    println!("- kind");
    println!("- end_time_unix_nano");
    println!("- attributes");
    println!("- dropped_attributes_count (any level)");
    println!("- events");
    println!("- dropped_events_count");
    println!("- links");
    println!("- dropped_links_count");
}

fn render_benchmark_results(results: Vec<ArrowVsProto>) {
    let metric_labels = r#"  batch count
  row count
  total schema inferrence (ms)
  total buffer creation (ms)
  total buffer size (bytes)
  total buffer serialization (ms)
  total buffer compression (ms)
  total compressed buffer size (bytes)
  total buffer decompression (ms)
  total buffer deserialization (ms)"#;
    let mut table = Table::new();
    table.set_header(vec!["File/Metrics", "Protobuf", "Arrow", "Analysis"]);

    for result in results {
        let mut columns = vec![];

        columns.push(format!("{}\n{}", result.file, metric_labels));
        columns.push(result.proto.to_string());
        columns.push(result.arrow.to_string());
        columns.push("".into());

        table.add_row(columns);
    }

    println!("{}", table);
}

/*
            batch_count: 0,
            row_count: 0,
            total_infer_schema_ms: 0,
            total_buffer_creation_ms: 0,
            total_buffer_size: 0,
            total_buffer_serialization_ms: 0,
            total_buffer_compression_ms: 0,
            total_compressed_buffer_size: 0,
            total_buffer_decompression_ms: 0,
            total_buffer_deserialization_ms: 0

 */