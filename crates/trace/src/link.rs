use serde_json::Value;
use common::{Attributes, Span};
use std::collections::HashMap;
use std::sync::Arc;
use arrow::datatypes::{Schema, Field, DataType};
use crate::arrow::{EntitySchema, FieldType};
use crate::attribute::{infer_attribute_types, add_attribute_fields};

pub fn infer_link_schema(spans: &[Span]) -> EntitySchema {
    let mut fields = vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("trace_state", DataType::Utf8, true),
        Field::new("dropped_attributes_count", DataType::UInt32, true),
    ];

    let mut attribute_types = HashMap::<String, FieldType>::new();

    for span in spans {
        if let Some(links) = &span.links {
            for link in links {
                infer_attribute_types(&link.attributes, &mut attribute_types);
            }
        }
    }

    add_attribute_fields(&attribute_types, &mut fields);

    EntitySchema { schema: Arc::new(Schema::new(fields)), attribute_fields: attribute_types }
}