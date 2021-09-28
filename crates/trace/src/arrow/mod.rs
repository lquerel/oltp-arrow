use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::Schema;

use common::Span;
use event::serialize_events;
use link::serialize_links;
use span::serialize_spans;
use oltp::opentelemetry::proto::events::v1::{ResourceEvents, InstrumentationLibraryEvents};
use prost::{Message};
use std::time::Instant;
use arrow::ipc::reader::StreamReader;
use crate::arrow::span::infer_span_schema;
use twox_hash::RandomXxHashBuilder64;
use crate::arrow::link::infer_link_schema;
use crate::arrow::event::infer_event_schema;

mod attribute;
mod event;
mod link;
mod span;

#[derive(PartialEq, Debug)]
pub struct FieldInfo {
    non_null_count: usize,
    field_type: FieldType,
    cardinality: HashSet<String>,
}

#[derive(PartialEq, Debug)]
pub enum FieldType {
    U64,
    I64,
    F64,
    String,
    Bool,
}

#[derive(Debug)]
pub struct EntitySchema {
    pub schema: Arc<Schema>,
    pub attribute_fields: HashMap<String, FieldInfo, RandomXxHashBuilder64>,
}

pub fn serialize(spans: &[Span]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let event_schema = infer_event_schema(spans);
    let link_schema = infer_link_schema(spans);

    let events_buf = serialize_events(event_schema, spans)?;
    let links_buf = serialize_links(link_schema, spans)?;

    let gen_id_column = events_buf.is_empty() && links_buf.is_empty();
    let span_schema = infer_span_schema(spans, gen_id_column);
    let start = Instant::now();
    let spans_buf = serialize_spans(span_schema, spans, events_buf.is_empty() && links_buf.is_empty())?;

    let resource_events = ResourceEvents {
        resource: None,
        instrumentation_library_events: vec![
            InstrumentationLibraryEvents {
                instrumentation_library: None,
                spans: spans_buf,
                events: events_buf,
                links: links_buf,
            }
        ],
        schema_url: "".to_string(),
    };

    let elapse_time = Instant::now() - start;
    println!("Arrow buffer creation: {}ms", elapse_time.as_millis());

    let start = Instant::now();
    let mut buf: Vec<u8> = Vec::new();
    resource_events.encode(&mut buf)?;
    let elapse_time = Instant::now() - start;
    println!("Arrow buffer serialization: {}ms", elapse_time.as_millis());

    Ok(buf)
}

pub fn deserialize(buf: Vec<u8>) {
    let start = Instant::now();
    let resource_events = ResourceEvents::decode(bytes::Bytes::from(buf)).unwrap();
    let mut reader = StreamReader::try_new(&resource_events.instrumentation_library_events[0].spans as &[u8]).expect("stream reader error");
    let batch = reader.next().unwrap().unwrap();
    assert!(batch.num_columns() > 0);
    let mut reader = StreamReader::try_new(&resource_events.instrumentation_library_events[0].events as &[u8]).expect("stream reader error");
    let batch = reader.next().unwrap().unwrap();
    assert!(batch.num_columns() > 0);
    let mut reader = StreamReader::try_new(&resource_events.instrumentation_library_events[0].links as &[u8]).expect("stream reader error");
    let batch = reader.next().unwrap().unwrap();
    assert!(batch.num_columns() > 0);
    let elapse_time = Instant::now() - start;
    println!("Arrow deserialize {}ms", elapse_time.as_millis());
}

impl FieldInfo {
    pub fn is_dictionary(&self) -> bool {
        (self.cardinality.len() as f64 / self.non_null_count as f64) < 0.4
    }
}