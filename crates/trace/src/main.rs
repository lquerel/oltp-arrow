use std::fs::File;
use std::io::{BufReader};
use lz4_flex::{compress_prepend_size, decompress_size_prepended};

use common::Span;
use std::time::Instant;

mod arrow;
mod protobuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let reader = BufReader::new(File::open("data/trace_samples.json").unwrap());
    // let traces: Vec<Value> = serde_json::from_reader(reader).unwrap();
    //
    // let f = File::create("data/trace_samples2.json").expect("Unable to create file");
    // let mut f = BufWriter::new(f);
    //
    // for trace in traces.iter() {
    //     let json = format!("{}\n", serde_json::to_string(&extract_span(trace)).unwrap());
    //     f.write_all(json.as_bytes()).expect("Unable to write data");
    // }

    let reader = BufReader::new(File::open("data/trace_samples2.json").unwrap());
    //
    // let s: Result<Span, _> = serde_json::from_reader(reader);
    // dbg!(&s);

    let spans: Vec<_> = serde_json::Deserializer::from_reader(reader)
        .into_iter::<Span>()
        .flat_map(|span| span.ok())
        .take(5000)
        .collect();

    let buf = arrow::serialize(&spans)?;
    println!("Arrow uncompressed size: {}", buf.len());
    let start = Instant::now();
    let compressed_buf = compress_prepend_size(&buf);
    let elapse_time = Instant::now() - start;
    println!("Arrow compressed size: {} ({}ms)", compressed_buf.len(), elapse_time.as_millis());
    let start = Instant::now();
    let buf = decompress_size_prepended(&compressed_buf).unwrap();
    let elapse_time = Instant::now() - start;
    println!("Arrow uncompressed size: {} ({}ms)", buf.len(), elapse_time.as_millis());
    arrow::deserialize(buf);

    println!();

    let buf = protobuf::serialize(&spans)?;
    println!("Protobuf uncompressed size: {}", buf.len());
    let start = Instant::now();
    let compressed_buf = compress_prepend_size(&buf);
    let elapse_time = Instant::now() - start;
    println!("Protobuf compressed size: {} ({}ms)", compressed_buf.len(), elapse_time.as_millis());
    let start = Instant::now();
    let buf = decompress_size_prepended(&compressed_buf).unwrap();
    let elapse_time = Instant::now() - start;
    println!("Protobuf uncompressed size: {} ({}ms)", buf.len(), elapse_time.as_millis());
    protobuf::deserialize(buf);

    Ok(())
}