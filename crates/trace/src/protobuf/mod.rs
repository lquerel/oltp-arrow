use std::time::Instant;

use prost::{EncodeError, Message};
use serde_json::Value;

use common::{Attributes, Span};
use oltp::opentelemetry::proto::common::v1::{AnyValue, KeyValue};
use oltp::opentelemetry::proto::common::v1::any_value;
use oltp::opentelemetry::proto::trace;
use oltp::opentelemetry::proto::trace::v1::{InstrumentationLibrarySpans, ResourceSpans};
use oltp::opentelemetry::proto::trace::v1::span::{Event, Link};

pub fn serialize(spans: &[Span]) -> Result<Vec<u8>, EncodeError> {
    let start = Instant::now();

    let resource_spans = ResourceSpans {
        resource: None,
        instrumentation_library_spans: vec![InstrumentationLibrarySpans {
            instrumentation_library: None,
            spans: spans
                .iter()
                .map(|span| trace::v1::Span {
                    trace_id: span.trace_id.clone().into_bytes(),
                    span_id: span.span_id.clone().into_bytes(),
                    trace_state: span.trace_state.clone().unwrap_or("".into()),
                    parent_span_id: span
                        .parent_span_id
                        .clone()
                        .unwrap_or("".into())
                        .into_bytes(),
                    name: span.name.clone(),
                    kind: span.kind.unwrap_or(0),
                    start_time_unix_nano: span.start_time_unix_nano,
                    end_time_unix_nano: span.end_time_unix_nano.unwrap_or(0),
                    attributes: attributes(span.attributes.as_ref()),
                    dropped_attributes_count: span.dropped_attributes_count.unwrap_or(0),
                    events: span
                        .events
                        .as_ref()
                        .unwrap_or(&vec![])
                        .iter()
                        .map(|evt| Event {
                            time_unix_nano: evt.time_unix_nano,
                            name: evt.name.clone(),
                            attributes: attributes(Some(&evt.attributes)),
                            dropped_attributes_count: evt.dropped_attributes_count.unwrap_or(0),
                        })
                        .collect(),
                    dropped_events_count: span.dropped_events_count.unwrap_or(0),
                    links: span
                        .links
                        .as_ref()
                        .unwrap_or(&vec![])
                        .iter()
                        .map(|link| Link {
                            trace_id: link.trace_id.clone().into_bytes(),
                            span_id: link.span_id.clone().into_bytes(),
                            trace_state: link.trace_state.clone().unwrap_or("".into()),
                            attributes: attributes(Some(&link.attributes)),
                            dropped_attributes_count: link.dropped_attributes_count.unwrap_or(0),
                        })
                        .collect(),
                    dropped_links_count: span.dropped_links_count.unwrap_or(0),
                    status: None,
                })
                .collect(),
            schema_url: "".to_string(),
        }],
        schema_url: "".to_string(),
    };

    // dbg!(&resource_spans);
    let elapse_time = Instant::now() - start;
    println!("Protocol buffer creation: {}ms", elapse_time.as_millis());

    let start = Instant::now();
    let mut buf: Vec<u8> = Vec::new();
    resource_spans.encode(&mut buf)?;
    let elapse_time = Instant::now() - start;
    println!(
        "Protocol buffer serialization: {}ms",
        elapse_time.as_millis()
    );

    Ok(buf)
}

pub fn deserialize(buf: Vec<u8>) {
    let start = Instant::now();
    let resource_spans = ResourceSpans::decode(bytes::Bytes::from(buf)).unwrap();
    assert_eq!(resource_spans.instrumentation_library_spans.len(), 1);
    let elapse_time = Instant::now() - start;
    println!("Protobuf deserialize {}ms", elapse_time.as_millis());
}

fn attributes(attributes: Option<&Attributes>) -> Vec<KeyValue> {
    attributes
        .iter()
        .flat_map(|attributes| {
            attributes
                .iter()
                .filter(|(_, value)| !value.is_null())
                .map(|(key, value)| KeyValue {
                    key: key.clone(),
                    value: match value {
                        Value::Null => None,
                        Value::Bool(v) => Some(AnyValue {
                            value: Some(any_value::Value::BoolValue(*v)),
                        }),
                        Value::Number(v) => Some(AnyValue {
                            value: Some(if v.is_i64() {
                                any_value::Value::IntValue(v.as_i64().unwrap_or(0))
                            } else {
                                any_value::Value::DoubleValue(v.as_f64().unwrap_or(0.0))
                            }),
                        }),
                        Value::String(v) => Some(AnyValue {
                            value: Some(any_value::Value::StringValue(v.clone())),
                        }),
                        Value::Array(_) => unimplemented!("attribute array value not supported"),
                        Value::Object(_) => {
                            println!("{} -> {}", key, value);
                            unimplemented!("attribute object value not supported")
                        }
                    },
                })
        })
        .collect()
}
