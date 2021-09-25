use common::{Span};
use std::collections::HashMap;
use std::sync::Arc;
use arrow::datatypes::{Schema, Field, DataType};
use crate::arrow::{EntitySchema, FieldType};
use crate::attribute::{infer_attribute_types, add_attribute_fields, add_attribute_columns};
use arrow::error::ArrowError;
use arrow::array::{UInt64Builder, StringBuilder, UInt32Builder, ArrayRef, UInt64Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow::ipc::writer::StreamWriter;

// ToDo add column id

pub fn serialize_spans(spans: &[Span]) -> Result<Vec<u8>, ArrowError> {
    let span_schema = infer_span_schema(&spans);

    let mut end_time_unix_nano = UInt64Builder::new(spans.len());
    let mut trace_state = StringBuilder::new(spans.len());
    let mut parent_span_id = StringBuilder::new(spans.len());
    let mut kind = UInt32Builder::new(spans.len());
    let mut dropped_attributes_count = UInt32Builder::new(spans.len());
    let mut dropped_events_count = UInt32Builder::new(spans.len());
    let mut dropped_links_count = UInt32Builder::new(spans.len());

    for span in spans.iter() {
        match span.end_time_unix_nano {
            Some(value) => end_time_unix_nano.append_value(value),
            None => end_time_unix_nano.append_null()
        }?;

        match &span.trace_state {
            Some(value) => trace_state.append_value(value),
            None => trace_state.append_null()
        }?;

        match &span.parent_span_id {
            Some(value) => parent_span_id.append_value(value.clone()),
            None => parent_span_id.append_null()
        }?;

        match span.kind {
            Some(value) => kind.append_value(value as u32),
            None => kind.append_null()
        }?;

        match span.dropped_attributes_count {
            Some(value) => dropped_attributes_count.append_value(value),
            None => dropped_attributes_count.append_null()
        }?;

        match span.dropped_events_count {
            Some(value) => dropped_events_count.append_value(value),
            None => dropped_events_count.append_null()
        }?;

        match span.dropped_links_count {
            Some(value) => dropped_links_count.append_value(value),
            None => dropped_links_count.append_null()
        }?;
    }

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from_iter_values(spans.iter().map(|span| span.start_time_unix_nano))),
        Arc::new(end_time_unix_nano.finish()),
        Arc::new(StringArray::from_iter_values(spans.iter().map(|span| span.trace_id.clone()))),
        Arc::new(StringArray::from_iter_values(spans.iter().map(|span| span.span_id.clone()))),
        Arc::new(trace_state.finish()),
        Arc::new(parent_span_id.finish()),
        Arc::new(StringArray::from_iter_values(spans.iter().map(|span| span.name.clone()))),
        Arc::new(kind.finish()),
        Arc::new(dropped_attributes_count.finish()),
        Arc::new(dropped_events_count.finish()),
        Arc::new(dropped_links_count.finish()),
    ];

    add_attribute_columns(
        spans.iter().map(|span| span.attributes.as_ref()).collect(),
        &span_schema,
        &mut columns,
    );

    let batch = RecordBatch::try_new(span_schema.schema.clone(), columns).unwrap();

    let mut writer = StreamWriter::try_new(Vec::new(), span_schema.schema.as_ref())?;
    writer.write(&batch)?;
    writer.finish()?;

    // let mut buf = Vec::new();
    // {
    //     let mut writer = LineDelimitedWriter::new(&mut buf);
    //     writer.write_batches(&[batch]).unwrap();
    // }
    //
    // println!("{}", String::from_utf8(buf).unwrap());

    writer.into_inner()
}

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