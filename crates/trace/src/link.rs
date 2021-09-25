use common::{Span, Link};
use std::collections::HashMap;
use std::sync::Arc;
use arrow::datatypes::{Schema, Field, DataType};
use crate::arrow::{EntitySchema, FieldType};
use crate::attribute::{infer_attribute_types, add_attribute_fields, add_attribute_columns};
use arrow::error::ArrowError;
use arrow::array::{UInt32Builder, ArrayRef, UInt32Array, StringArray, StringBuilder};
use arrow::record_batch::RecordBatch;
use arrow::ipc::writer::StreamWriter;

pub fn serialize_links(spans: &[Span]) -> Result<Vec<u8>, ArrowError> {
    let link_schema = infer_link_schema(&spans);
    let links: Vec<(usize, &Link)> = spans.iter().enumerate()
        .filter(|(_, link)| link.links.is_some())
        .flat_map(|(id, link)| link.links.as_ref().unwrap().iter().map(move |link| (id, link)))
        .collect();

    let mut trace_state = StringBuilder::new(links.len());
    let mut dropped_attributes_count = UInt32Builder::new(links.len());

    for (_, link) in links.iter() {
        match link.dropped_attributes_count {
            Some(value) => dropped_attributes_count.append_value(value),
            None => dropped_attributes_count.append_null()
        }?;
        match &link.trace_state {
            Some(value) => trace_state.append_value(value),
            None => trace_state.append_null()
        }?;
    }

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from_iter_values(links.iter().map(|(id, _)| *id as u32))),
        Arc::new(StringArray::from_iter_values(links.iter().map(|(_, link)| link.trace_id.clone()))),
        Arc::new(StringArray::from_iter_values(links.iter().map(|(_, link)| link.span_id.clone()))),
        Arc::new(trace_state.finish()),
        Arc::new(dropped_attributes_count.finish()),
    ];

    add_attribute_columns(
        links.iter().map(|(_, link)| Some(&link.attributes)).collect(),
        &link_schema,
        &mut columns,
    );

    let batch = RecordBatch::try_new(link_schema.schema.clone(), columns)?;

    let mut writer = StreamWriter::try_new(Vec::new(), link_schema.schema.as_ref())?;
    writer.write(&batch)?;
    writer.finish()?;
    writer.into_inner()
}

pub fn infer_link_schema(spans: &[Span]) -> EntitySchema {
    let mut fields = vec![
        Field::new("id", DataType::UInt32, false),
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