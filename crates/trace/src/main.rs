use std::fs::File;
use std::io::BufReader;
use std::time::Instant;

use lz4_flex::{compress_prepend_size, decompress_size_prepended};

use common::Span;

mod arrow;
mod protobuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
   let reader = BufReader::new(File::open("data/trace_samples2.json").unwrap());

    let spans: Vec<_> = serde_json::Deserializer::from_reader(reader)
        .into_iter::<Span>()
        .flat_map(|span| span.ok())
//        .take(5000)
        .collect();

    let buf = arrow::serialize(&spans)?;
    println!("Arrow uncompressed size: {}", buf.len());
    let start = Instant::now();
    let compressed_buf = compress_prepend_size(&buf);
    let elapse_time = Instant::now() - start;
    println!(
        "Arrow compressed size: {} ({}ms)",
        compressed_buf.len(),
        elapse_time.as_millis()
    );
    let start = Instant::now();
    let buf = decompress_size_prepended(&compressed_buf).unwrap();
    let elapse_time = Instant::now() - start;
    println!(
        "Arrow uncompressed size: {} ({}ms)",
        buf.len(),
        elapse_time.as_millis()
    );
    arrow::deserialize(buf);

    println!();

    let buf = protobuf::serialize(&spans)?;
    println!("Protobuf uncompressed size: {}", buf.len());
    let start = Instant::now();
    let compressed_buf = compress_prepend_size(&buf);
    let elapse_time = Instant::now() - start;
    println!(
        "Protobuf compressed size: {} ({}ms)",
        compressed_buf.len(),
        elapse_time.as_millis()
    );
    let start = Instant::now();
    let buf = decompress_size_prepended(&compressed_buf).unwrap();
    let elapse_time = Instant::now() - start;
    println!(
        "Protobuf uncompressed size: {} ({}ms)",
        buf.len(),
        elapse_time.as_millis()
    );
    protobuf::deserialize(buf);

    Ok(())
}
