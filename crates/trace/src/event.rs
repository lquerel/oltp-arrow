use serde_json::Value;
use common::{Attributes, Span};
use std::collections::HashMap;
use std::sync::Arc;
use arrow::datatypes::{Schema, Field, DataType};
use crate::arrow::{EntitySchema, FieldType};
use crate::attribute::{infer_attribute_types, add_attribute_fields};

pub fn infer_event_schema(spans: &[Span]) -> EntitySchema {
    let mut fields = vec![
        Field::new("time_unix_nano", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("dropped_attributes_count", DataType::UInt32, true),
    ];

    let mut attribute_types = HashMap::<String, FieldType>::new();

    for span in spans {
        if let Some(events) = &span.events {
            for event in events {
                infer_attribute_types(&event.attributes, &mut attribute_types);
            }
        }
    }

    add_attribute_fields(&attribute_types, &mut fields);

    EntitySchema { schema: Arc::new(Schema::new(fields)), attribute_fields: attribute_types }
}