mod arrow;
mod attribute;
mod event;
mod link;
mod span;

use crate::arrow::arrow_buffer;
use common::Span;
use std::fs::File;
use std::io::BufReader;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reader = BufReader::new(File::open("data/trace.json").unwrap());
    let spans = serde_json::Deserializer::from_reader(reader).into_iter::<Span>();

    arrow_buffer(spans)?;
    Ok(())
}
