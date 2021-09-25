use crate::arrow::{EntitySchema, FieldType};
use crate::attribute::{add_attribute_columns, add_attribute_fields, infer_attribute_types};
use arrow::array::{ArrayRef, StringArray, UInt32Array, UInt32Builder, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use common::{Event, Span};
use std::collections::HashMap;
use std::sync::Arc;

pub fn serialize_events(spans: &[Span]) -> Result<Vec<u8>, ArrowError> {
    let event_schema = infer_event_schema(spans);
    let events: Vec<(usize, &Event)> = spans
        .iter()
        .enumerate()
        .filter(|(_, span)| span.events.is_some())
        .flat_map(|(id, span)| {
            span.events
                .as_ref()
                .unwrap()
                .iter()
                .map(move |event| (id, event))
        })
        .collect();

    let mut dropped_attributes_count = UInt32Builder::new(events.len());

    for (_, event) in events.iter() {
        match event.dropped_attributes_count {
            Some(value) => dropped_attributes_count.append_value(value),
            None => dropped_attributes_count.append_null(),
        }?;
    }

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from_iter_values(
            events.iter().map(|(id, _)| *id as u32),
        )),
        Arc::new(UInt64Array::from_iter_values(
            events.iter().map(|(_, event)| event.time_unix_nano),
        )),
        Arc::new(StringArray::from_iter_values(
            events.iter().map(|(_, event)| event.name.clone()),
        )),
        Arc::new(dropped_attributes_count.finish()),
    ];

    add_attribute_columns(
        events
            .iter()
            .map(|(_, event)| Some(&event.attributes))
            .collect(),
        &event_schema,
        &mut columns,
    );

    let batch = RecordBatch::try_new(event_schema.schema.clone(), columns)?;

    let mut writer = StreamWriter::try_new(Vec::new(), event_schema.schema.as_ref())?;
    writer.write(&batch)?;
    writer.finish()?;
    writer.into_inner()
}

pub fn infer_event_schema(spans: &[Span]) -> EntitySchema {
    let mut fields = vec![
        Field::new("id", DataType::UInt32, false),
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

    EntitySchema {
        schema: Arc::new(Schema::new(fields)),
        attribute_fields: attribute_types,
    }
}
