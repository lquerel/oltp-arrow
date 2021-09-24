mod arrow;
mod span;
mod event;
mod attribute;
mod link;

use std::io::BufReader;
use std::fs::File;
use common::Span;
use crate::arrow::arrow_buffer;

fn main() {
    let reader = BufReader::new(File::open("data/trace.json").unwrap());
    let spans = serde_json::Deserializer::from_reader(reader).into_iter::<Span>();

    let result = arrow_buffer(spans);
}