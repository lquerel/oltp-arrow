use serde_json::{StreamDeserializer};
use serde_json::de::IoRead;
use common::{Span};
use std::io::Read;
use arrow::datatypes::{Schema};
use arrow::error::ArrowError;
use std::sync::Arc;
use std::collections::HashMap;
use crate::span::serialize_spans;
use crate::event::serialize_events;
use crate::link::serialize_links;

#[derive(PartialEq, Debug)]
pub enum FieldType {
    U64,
    I64,
    F64,
    String,
    Bool,
}

pub struct EntitySchema {
    pub schema: Arc<Schema>,
    pub attribute_fields: HashMap<String, FieldType>,
}

pub fn arrow_buffer<R: Read>(spans: StreamDeserializer<IoRead<R>, Span>) -> Result<(), ArrowError> {
    let spans: Vec<Span> = spans.flat_map(|span| span.ok()).collect();

    let spans_buf = serialize_spans(&spans)?;
    let events_buf = serialize_events(&spans)?;
    let links_buf = serialize_links(&spans)?;

    println!("{}", spans_buf.len());
    println!("{}", events_buf.len());
    println!("{}", links_buf.len());

    Ok(())
}