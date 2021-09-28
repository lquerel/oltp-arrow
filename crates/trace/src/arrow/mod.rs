use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use prost::Message;
use twox_hash::RandomXxHashBuilder64;

use common::Span;
use event::serialize_events;
use link::serialize_links;
use oltp::opentelemetry::proto::events::v1::{InstrumentationLibraryEvents, ResourceEvents};
use span::serialize_spans;

use crate::arrow::event::infer_event_schema;
use crate::arrow::link::infer_link_schema;
use crate::arrow::span::infer_span_schema;
use crate::BenchmarkResult;

mod attribute;
mod event;
mod link;
mod span;

#[derive(PartialEq, Debug)]
pub struct FieldInfo {
    non_null_count: usize,
    field_type: FieldType,
    dictionary_values: HashSet<String>,
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

pub fn serialize(spans: &[Span], bench_result: &mut BenchmarkResult) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let (event_schema, event_count) = infer_event_schema(spans);
    let (link_schema, link_count) = infer_link_schema(spans);
    let gen_id_column = (event_count + link_count) > 0;
    let span_schema = infer_span_schema(spans, gen_id_column);
    let elapse_time = Instant::now() - start;
    bench_result.total_infer_schema_ms += elapse_time.as_millis();

    let start = Instant::now();
    let events_buf = serialize_events(event_schema, spans)?;
    let links_buf = serialize_links(link_schema, spans)?;
    let spans_buf = serialize_spans(
        span_schema,
        spans,
        gen_id_column,
    )?;

    let resource_events = ResourceEvents {
        resource: None,
        instrumentation_library_events: vec![InstrumentationLibraryEvents {
            instrumentation_library: None,
            spans: spans_buf,
            events: events_buf,
            links: links_buf,
        }],
        schema_url: "".to_string(),
    };

    let elapse_time = Instant::now() - start;
    bench_result.total_buffer_creation_ms += elapse_time.as_millis();

    let start = Instant::now();
    let mut buf: Vec<u8> = Vec::new();
    resource_events.encode(&mut buf)?;
    let elapse_time = Instant::now() - start;
    bench_result.total_buffer_serialization_ms += elapse_time.as_millis();

    Ok(buf)
}

pub fn deserialize(
    buf: Vec<u8>,
    bench_result: &mut BenchmarkResult
) {
    let start = Instant::now();
    let resource_events = ResourceEvents::decode(bytes::Bytes::from(buf)).unwrap();
    let mut reader =
        StreamReader::try_new(&resource_events.instrumentation_library_events[0].spans as &[u8])
            .expect("stream reader error");
    let batch = reader.next().unwrap().unwrap();
    assert!(batch.num_columns() > 0);
    let mut reader =
        StreamReader::try_new(&resource_events.instrumentation_library_events[0].events as &[u8])
            .expect("stream reader error");
    let batch = reader.next().unwrap().unwrap();
    assert!(batch.num_columns() > 0);
    let mut reader =
        StreamReader::try_new(&resource_events.instrumentation_library_events[0].links as &[u8])
            .expect("stream reader error");
    let batch = reader.next().unwrap().unwrap();
    assert!(batch.num_columns() > 0);
    let elapse_time = Instant::now() - start;
    bench_result.total_buffer_deserialization_ms += elapse_time.as_millis();
}

impl FieldInfo {
    pub fn is_dictionary(&self) -> bool {
        (self.dictionary_values.len() as f64 / self.non_null_count as f64) < 0.4
    }
}
