use std::collections::{HashMap};
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use prost::Message;
use twox_hash::RandomXxHashBuilder64;

use common::{Attributes, Span};
use event::serialize_events_from_row_oriented_data_source;
use link::serialize_links_from_row_oriented_data_source;
use oltp::opentelemetry::proto::events::v1::{InstrumentationLibraryEvents, ResourceEvents};
use schema::{FieldInfo, FieldType};
use span::serialize_spans_from_row_oriented_data_source;

use crate::arrow::attribute::{infer_event_attribute_schema, infer_link_attribute_schema, infer_span_attribute_schema};
use crate::arrow::event::{infer_event_schema, serialize_events_from_column_oriented_data_source};
use crate::arrow::link::{infer_link_schema, serialize_links_from_column_oriented_data_source};
use crate::arrow::span::{infer_span_schema, serialize_spans_from_column_oriented_data_source};
use crate::BenchmarkResult;
use crate::arrow::statistics::BatchStatistics;

mod attribute;
mod event;
mod link;
mod span;
pub (crate) mod statistics;
pub (crate) mod schema;

#[derive(Debug)]
pub struct EntitySchema {
    pub schema: Arc<Schema>,
    pub attribute_fields: HashMap<String, FieldInfo, RandomXxHashBuilder64>,
}

#[derive(Debug)]
pub struct DataColumns {
    spans: SpanDataColumns,
    events: EventDataColumns,
    links: LinkDataColumns,
}

#[derive(Debug)]
pub struct SpanDataColumns {
    trace_id_column: Vec<String>,
    span_id_column: Vec<String>,
    trace_state_column: Vec<Option<String>>,
    parent_span_id_column: Vec<Option<String>>,
    name_column: Vec<String>,
    kind_column: Vec<Option<i32>>,
    start_time_unix_nano_column: Vec<u64>,
    end_time_unix_nano_column: Vec<Option<u64>>,
    attributes_column: HashMap<String, DataColumn>,
    dropped_attributes_count_column: Vec<Option<u32>>,
    dropped_events_count_column: Vec<Option<u32>>,
    dropped_links_count_column: Vec<Option<u32>>,
}

impl SpanDataColumns {
    pub fn new(inferred_attributes: HashMap<String, FieldInfo, RandomXxHashBuilder64>) -> Self {
        Self {
            attributes_column: build_attribute_columns(inferred_attributes),
            ..Default::default()
        }
    }
}

impl Default for SpanDataColumns {
    fn default() -> Self {
        Self {
            trace_id_column: vec![],
            span_id_column: vec![],
            trace_state_column: vec![],
            parent_span_id_column: vec![],
            name_column: vec![],
            kind_column: vec![],
            start_time_unix_nano_column: vec![],
            end_time_unix_nano_column: vec![],
            attributes_column: Default::default(),
            dropped_attributes_count_column: vec![],
            dropped_events_count_column: vec![],
            dropped_links_count_column: vec![],
        }
    }
}

#[derive(Debug)]
pub struct EventDataColumns {
    id_column: Vec<usize>,
    time_unix_nano_column: Vec<u64>,
    name_column: Vec<String>,
    attributes_column: HashMap<String, DataColumn>,
    dropped_attributes_count_column: Vec<Option<u32>>,
}

impl EventDataColumns {
    pub fn new(inferred_attributes: HashMap<String, FieldInfo, RandomXxHashBuilder64>) -> Self {
        Self {
            attributes_column: build_attribute_columns(inferred_attributes),
            ..Default::default()
        }
    }
}

impl Default for EventDataColumns {
    fn default() -> Self {
        Self {
            id_column: vec![],
            time_unix_nano_column: vec![],
            name_column: vec![],
            attributes_column: Default::default(),
            dropped_attributes_count_column: vec![],
        }
    }
}

#[derive(Debug)]
pub struct LinkDataColumns {
    id_column: Vec<usize>,
    trace_id_column: Vec<String>,
    span_id_column: Vec<String>,
    trace_state_column: Vec<Option<String>>,
    attributes_column: HashMap<String, DataColumn>,
    dropped_attributes_count_column: Vec<Option<u32>>,
}

impl Default for LinkDataColumns {
    fn default() -> Self {
        Self {
            id_column: vec![],
            trace_id_column: vec![],
            span_id_column: vec![],
            trace_state_column: vec![],
            attributes_column: HashMap::default(),
            dropped_attributes_count_column: vec![],
        }
    }
}

impl LinkDataColumns {
    pub fn new(inferred_attributes: HashMap<String, FieldInfo, RandomXxHashBuilder64>) -> Self {
        Self {
            attributes_column: build_attribute_columns(inferred_attributes),
            ..Default::default()
        }
    }
}

#[derive(Debug)]
pub enum DataColumn {
    U64Column {
        missing: usize,
        values: Vec<Option<u64>>,
    },
    I64Column {
        missing: usize,
        values: Vec<Option<i64>>,
    },
    F64Column {
        missing: usize,
        values: Vec<Option<f64>>,
    },
    StringColumn {
        missing: usize,
        values: Vec<Option<String>>,
    },
    BoolColumn {
        missing: usize,
        values: Vec<Option<bool>>,
    },
}

pub fn serialize_row_oriented_data_source(batch_stats: &mut BatchStatistics, spans: &[Span], bench_result: &mut BenchmarkResult) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let (event_schema, event_count) = infer_event_schema(spans);
    let (link_schema, link_count) = infer_link_schema(spans);
    let gen_id_column = (event_count + link_count) > 0;
    let span_schema = infer_span_schema(spans, gen_id_column);
    let elapse_time = Instant::now() - start;
    bench_result.total_infer_schema_ns += elapse_time.as_nanos();

    let start = Instant::now();
    let events_buf = serialize_events_from_row_oriented_data_source(batch_stats.event_stats(), event_schema, spans)?;
    let links_buf = serialize_links_from_row_oriented_data_source(batch_stats.link_stats(), link_schema, spans)?;
    let spans_buf = serialize_spans_from_row_oriented_data_source(
        batch_stats.span_stats(),
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
    bench_result.total_buffer_creation_ns += elapse_time.as_nanos();

    let start = Instant::now();
    let mut buf: Vec<u8> = Vec::new();
    resource_events.encode(&mut buf)?;
    let elapse_time = Instant::now() - start;
    bench_result.total_buffer_serialization_ns += elapse_time.as_nanos();

    Ok(buf)
}

pub fn serialize_column_oriented_data_source(batch_stats: &mut BatchStatistics, spans: &[Span], bench_result: &mut BenchmarkResult) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let data_columns = to_data_columns(spans);

    let start = Instant::now();
    let events_buf = serialize_events_from_column_oriented_data_source(batch_stats.event_stats(), &data_columns)?;
    let links_buf = serialize_links_from_column_oriented_data_source(batch_stats.link_stats(), &data_columns)?;
    let spans_buf = serialize_spans_from_column_oriented_data_source(batch_stats.span_stats(), &data_columns)?;

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
    bench_result.total_buffer_creation_ns += elapse_time.as_nanos();

    let start = Instant::now();
    let mut buf: Vec<u8> = Vec::new();
    resource_events.encode(&mut buf)?;
    let elapse_time = Instant::now() - start;
    bench_result.total_buffer_serialization_ns += elapse_time.as_nanos();

    Ok(buf)
}

pub fn deserialize(
    buf: Vec<u8>,
    bench_result: &mut BenchmarkResult,
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
    bench_result.total_buffer_deserialization_ns += elapse_time.as_nanos();
}

fn to_data_columns(spans: &[Span]) -> DataColumns {
    let mut data_columns = DataColumns {
        spans: SpanDataColumns::new(infer_span_attribute_schema(spans)),
        events: EventDataColumns::new(infer_event_attribute_schema(spans).1),
        links: LinkDataColumns::new(infer_link_attribute_schema(spans).1),
    };

    spans.iter().enumerate().for_each(|(id, span)| {
        // process span fields
        data_columns.spans.trace_id_column.push(span.trace_id.clone());
        data_columns.spans.span_id_column.push(span.span_id.clone());
        data_columns.spans.trace_state_column.push(span.trace_state.clone());
        data_columns.spans.parent_span_id_column.push(span.parent_span_id.clone());
        data_columns.spans.name_column.push(span.name.clone());
        data_columns.spans.kind_column.push(span.kind.clone());
        data_columns.spans.start_time_unix_nano_column.push(span.start_time_unix_nano);
        data_columns.spans.end_time_unix_nano_column.push(span.end_time_unix_nano.clone());
        attributes_to_data_columns(span.attributes.as_ref(), &mut data_columns.spans.attributes_column);
        data_columns.spans.dropped_attributes_count_column.push(span.dropped_attributes_count.clone());
        data_columns.spans.dropped_events_count_column.push(span.dropped_events_count.clone());
        data_columns.spans.dropped_links_count_column.push(span.dropped_links_count.clone());


        // process event fields
        if let Some(events) = &span.events {
            events.iter().for_each(|event| {
                data_columns.events.id_column.push(id);
                data_columns.events.time_unix_nano_column.push(event.time_unix_nano);
                data_columns.events.name_column.push(event.name.clone());
                attributes_to_data_columns(Some(&event.attributes), &mut data_columns.events.attributes_column);
                data_columns.events.dropped_attributes_count_column.push(event.dropped_attributes_count.clone());
            });
        }

        // process link fields
        if let Some(links) = &span.links {
            links.iter().for_each(|link| {
                data_columns.links.id_column.push(id);
                data_columns.links.trace_id_column.push(link.trace_id.clone());
                data_columns.links.span_id_column.push(link.span_id.clone());
                data_columns.links.trace_state_column.push(link.trace_state.clone());
                attributes_to_data_columns(Some(&link.attributes), &mut data_columns.links.attributes_column);
                data_columns.links.dropped_attributes_count_column.push(link.dropped_attributes_count.clone());
            });
        }
    });

    data_columns
}

fn attributes_to_data_columns(attributes: Option<&Attributes>, attributes_column: &mut HashMap<String, DataColumn>) {
    match attributes {
        None => {
            attributes_column.iter_mut().for_each(|(_, data_column)| match data_column {
                DataColumn::U64Column { missing, values } => {
                    *missing += 1;
                    values.push(None);
                }
                DataColumn::I64Column { missing, values } => {
                    *missing += 1;
                    values.push(None);
                }
                DataColumn::F64Column { missing, values } => {
                    *missing += 1;
                    values.push(None);
                }
                DataColumn::StringColumn { missing, values } => {
                    *missing += 1;
                    values.push(None);
                }
                DataColumn::BoolColumn { missing, values } => {
                    *missing += 1;
                    values.push(None);
                }
            });
        }
        Some(attributes) => {
            let mut max_row_count = 0;

            attributes.iter().for_each(|(name, value)| {
                if !value.is_null() {
                    let data_column = attributes_column.get_mut(name)
                        .expect("missing attribute column, should have been created based on the inference schema mechanism");

                    match data_column {
                        DataColumn::U64Column { values, .. } => {
                            let value = value.as_u64().expect("should be a u64 value based on the inference schema");
                            values.push(Some(value));
                            max_row_count = usize::max(max_row_count, values.len());
                        }
                        DataColumn::I64Column { values, .. } => {
                            let value = value.as_i64().expect("should be a i64 value based on the inference schema");
                            values.push(Some(value));
                            max_row_count = usize::max(max_row_count, values.len());
                        }
                        DataColumn::F64Column { values, .. } => {
                            let value = value.as_f64().expect("should be a f64 value based on the inference schema");
                            values.push(Some(value));
                            max_row_count = usize::max(max_row_count, values.len());
                        }
                        DataColumn::StringColumn { values, .. } => {
                            let value = value.as_str().expect("should be a string value based on the inference schema");
                            values.push(Some(value.into()));
                            max_row_count = usize::max(max_row_count, values.len());
                        }
                        DataColumn::BoolColumn { values, .. } => {
                            let value = value.as_bool().expect("should be a boolean value based on the inference schema");
                            values.push(Some(value));
                            max_row_count = usize::max(max_row_count, values.len());
                        }
                    }
                }
            });

            attributes_column.iter_mut()
                .for_each(|(_name, data_column)| {
                    match data_column {
                        DataColumn::U64Column { values, .. }=> {
                            for _ in 0..(max_row_count-values.len()) {
                                values.push(None);
                            }
                        }
                        DataColumn::I64Column { values, .. }=> {
                            for _ in 0..(max_row_count-values.len()) {
                                values.push(None);
                            }
                        }
                        DataColumn::F64Column { values, .. }=> {
                            for _ in 0..(max_row_count-values.len()) {
                                values.push(None);
                            }
                        }
                        DataColumn::StringColumn { values, .. }=> {
                            for _ in 0..(max_row_count-values.len()) {
                                values.push(None);
                            }
                        }
                        DataColumn::BoolColumn { values, .. } => {
                            for _ in 0..(max_row_count-values.len()) {
                                values.push(None);
                            }
                        }
                    }
                });
        }
    }
}

fn build_attribute_columns(inferred_attributes: HashMap<String, FieldInfo, RandomXxHashBuilder64>) -> HashMap<String, DataColumn> {
    inferred_attributes.iter()
        .map(|(field_name, field)| {
            (
                field_name.clone(),
                match field.field_type {
                    FieldType::U64 => DataColumn::U64Column { missing: 0, values: vec![] },
                    FieldType::I64 => DataColumn::I64Column { missing: 0, values: vec![] },
                    FieldType::F64 => DataColumn::F64Column { missing: 0, values: vec![] },
                    FieldType::String => DataColumn::StringColumn { missing: 0, values: vec![] },
                    FieldType::Bool => DataColumn::BoolColumn { missing: 0, values: vec![] },
                }
            )
        })
        .collect()
}
