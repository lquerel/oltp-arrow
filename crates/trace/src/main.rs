mod arrow;
mod span;
mod event;
mod attribute;
mod link;

use std::io::BufReader;
use std::fs::File;
use common::Span;
use crate::arrow::arrow_buffer;

fn main() -> Result<(), Box<dyn std::error::Error>>{
    let reader = BufReader::new(File::open("data/trace.json").unwrap());
    let spans = serde_json::Deserializer::from_reader(reader).into_iter::<Span>();

    arrow_buffer(spans)?;
    Ok(())
}