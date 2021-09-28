use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::time::Instant;

use clap::{Clap, ValueHint};
use itertools::Itertools;
use lz4_flex::{compress_prepend_size, decompress_size_prepended};

use common::{Span, Event, Link};
use std::collections::HashMap;
use serde_json::Value;

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

pub struct BenchmarkResult {}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::parse();

    opt.files.iter().for_each(|file| {
        println!("Processing file '{}'", file.as_path().display().to_string());
        let reader = BufReader::new(File::open(file).unwrap());

        serde_json::Deserializer::from_reader(reader)
            .into_iter::<Span>()
            .flat_map(|span| span.ok())
            .chunks(opt.batch_size)
            .into_iter()
            .for_each(|chunk| {
                let spans: Vec<_> = chunk.collect();
                println!("Processing batch of {} spans", spans.len());
                println!("Arrow");
                let result = bench_arrow(&spans);
                if result.is_err() {
                    println!("{:?}", result);
                }
                println!("Protobuf");
                let result = bench_protobuf(&spans);
                if result.is_err() {
                    println!("{:?}", result);
                }

                println!("====================================================================================================")
            });
    });

    if opt.files.is_empty() {
        dump_sample_data();
    }

    Ok(())
}

fn bench_arrow(spans: &[Span]) -> Result<(), Box<dyn std::error::Error>> {
    let buf = arrow::serialize(&spans)?;
    println!("\tuncompressed size: {}", buf.len());
    let start = Instant::now();
    let compressed_buf = compress_prepend_size(&buf);
    let elapse_time = Instant::now() - start;
    println!(
        "\tcompressed size: {} ({}ms)",
        compressed_buf.len(),
        elapse_time.as_millis()
    );
    let start = Instant::now();
    let buf = decompress_size_prepended(&compressed_buf).unwrap();
    let elapse_time = Instant::now() - start;
    println!(
        "\tuncompressed size: {} ({}ms)",
        buf.len(),
        elapse_time.as_millis()
    );
    arrow::deserialize(buf);
    Ok(())
}

fn bench_protobuf(spans: &[Span]) -> Result<(), Box<dyn std::error::Error>> {
    let buf = protobuf::serialize(&spans)?;
    println!("\tuncompressed size: {}", buf.len());
    let start = Instant::now();
    let compressed_buf = compress_prepend_size(&buf);
    let elapse_time = Instant::now() - start;
    println!(
        "\tcompressed size: {} ({}ms)",
        compressed_buf.len(),
        elapse_time.as_millis()
    );
    let start = Instant::now();
    let buf = decompress_size_prepended(&compressed_buf).unwrap();
    let elapse_time = Instant::now() - start;
    println!(
        "\tuncompressed size: {} ({}ms)",
        buf.len(),
        elapse_time.as_millis()
    );
    protobuf::deserialize(buf);
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
        events: Some(vec![
            Event {
                time_unix_nano: 1626371667388918000,
                name: "<event_name>".to_string(),
                attributes: attributes.clone(),
                dropped_attributes_count: Some(0)
            }
        ]),
        dropped_events_count: Some(0),
        links: Some(vec![
            Link {
                trace_id: "<id>".to_string(),
                span_id: "<id>".to_string(),
                trace_state: Some("<state>".into()),
                attributes,
                dropped_attributes_count: Some(0)
            }
        ]),
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