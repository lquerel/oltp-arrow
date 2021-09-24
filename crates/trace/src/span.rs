use serde_json::Value;
use common::{Attributes, Span};
use std::collections::HashMap;
use std::sync::Arc;
use arrow::datatypes::{Schema, Field, DataType};
use crate::arrow::{EntitySchema, FieldType};
use crate::attribute::{infer_attribute_types, add_attribute_fields};

pub fn infer_span_schema(spans: &[Span]) -> EntitySchema {
    let mut fields = vec![
        Field::new("start_time_unix_nano", DataType::UInt64, false),
        Field::new("end_time_unix_nano", DataType::UInt64, true),
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("trace_state", DataType::Utf8, true),
        Field::new("parent_span_id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, false),
        Field::new("kind", DataType::UInt32, true),
        Field::new("dropped_attributes_count", DataType::UInt32, true),
        Field::new("dropped_events_count", DataType::UInt32, true),
        Field::new("dropped_links_count", DataType::UInt32, true),
    ];

    let mut attribute_types = HashMap::<String, FieldType>::new();

    for span in spans {
        if let Some(attributes) = &span.attributes {
            infer_attribute_types(attributes, &mut attribute_types);
        }
    }

    add_attribute_fields(&attribute_types, &mut fields);

    EntitySchema { schema: Arc::new(Schema::new(fields)), attribute_fields: attribute_types }
}