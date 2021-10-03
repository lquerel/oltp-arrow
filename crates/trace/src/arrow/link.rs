use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, StringBuilder, UInt32Array, UInt32Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use common::{Link, Span};

use crate::arrow::attribute::{add_attribute_columns, add_attribute_fields, infer_link_attribute_schema, add_attribute_data_columns};
use crate::arrow::{EntitySchema, DataColumns};
use crate::arrow::statistics::{ColumnsStatistics};

pub fn serialize_links_from_row_oriented_data_source(stats: &mut ColumnsStatistics, link_schema: EntitySchema, spans: &[Span]) -> Result<Vec<u8>, ArrowError> {
    let links: Vec<(usize, &Link)> = spans
        .iter()
        .enumerate()
        .filter(|(_, link)| link.links.is_some())
        .flat_map(|(id, link)| {
            link.links
                .as_ref()
                .unwrap()
                .iter()
                .map(move |link| (id, link))
        })
        .collect();

    let mut trace_state = StringBuilder::new(links.len());
    let mut dropped_attributes_count = UInt32Builder::new(links.len());

    for (_, link) in links.iter() {
        match link.dropped_attributes_count {
            Some(value) => dropped_attributes_count.append_value(value),
            None => dropped_attributes_count.append_null(),
        }?;
        match &link.trace_state {
            Some(value) => trace_state.append_value(value),
            None => trace_state.append_null(),
        }?;
    }

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from_iter_values(
            links.iter().map(|(id, _)| *id as u32),
        )),
        Arc::new(StringArray::from_iter_values(
            links.iter().map(|(_, link)| link.trace_id.clone()),
        )),
        Arc::new(StringArray::from_iter_values(
            links.iter().map(|(_, link)| link.span_id.clone()),
        )),
        Arc::new(trace_state.finish()),
        Arc::new(dropped_attributes_count.finish()),
    ];

    add_attribute_columns(
        links
            .iter()
            .map(|(_, link)| Some(&link.attributes))
            .collect(),
        &link_schema,
        &mut columns,
    );

    stats.report(link_schema.schema.clone(), &columns);

    let batch = RecordBatch::try_new(link_schema.schema.clone(), columns)?;

    let mut writer = StreamWriter::try_new(Vec::new(), link_schema.schema.as_ref())?;
    writer.write(&batch)?;
    writer.finish()?;
    writer.into_inner()
}

pub fn serialize_links_from_column_oriented_data_source(stats: &mut ColumnsStatistics, data_columns: &DataColumns) -> Result<Vec<u8>, ArrowError> {
    let mut fields = vec![
        Field::new("id", DataType::UInt32, false),
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("trace_state", DataType::Utf8, true),
        Field::new("dropped_attributes_count", DataType::UInt32, true),
    ];

    let mut dropped_attributes_count_builder = UInt32Builder::new(data_columns.links.dropped_attributes_count_column.len());
    data_columns.links.dropped_attributes_count_column.iter().for_each(|value| match value {
        None => dropped_attributes_count_builder.append_null(),
        Some(value) => dropped_attributes_count_builder.append_value(*value)
    }.expect("append data into dropped_attributes_count_builder failed"));

    let mut trace_state_builder = StringBuilder::new(data_columns.links.trace_state_column.len());
    data_columns.links.trace_state_column.iter().for_each(|value| match value {
        None => trace_state_builder.append_null(),
        Some(value) => trace_state_builder.append_value(value.clone())
    }.expect("append data into trace_state_builder failed"));

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from_iter_values(
            data_columns.links.id_column.iter().map(|id| *id as u32),
        )),
        Arc::new(StringArray::from_iter_values(
            data_columns.links.trace_id_column.iter().map(|name| name.clone()),
        )),
        Arc::new(StringArray::from_iter_values(
            data_columns.links.span_id_column.iter().map(|name| name.clone()),
        )),
        Arc::new(trace_state_builder.finish()),
        Arc::new(dropped_attributes_count_builder.finish()),
    ];

    add_attribute_data_columns(&mut fields, &mut columns, &data_columns.links.attributes_column);

    let schema = Arc::new(Schema::new(fields));
    stats.report(schema.clone(), &columns);
    let batch = RecordBatch::try_new(schema.clone(), columns)?;

    let mut writer = StreamWriter::try_new(Vec::new(), schema.as_ref())?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(writer.into_inner()?)
}

pub fn infer_link_schema(spans: &[Span]) -> (EntitySchema, usize) {
    let mut fields = vec![
        Field::new("id", DataType::UInt32, false),
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("trace_state", DataType::Utf8, true),
        Field::new("dropped_attributes_count", DataType::UInt32, true),
    ];

    let (link_count, attribute_types) = infer_link_attribute_schema(spans);

    add_attribute_fields(&attribute_types, &mut fields);

    (
        EntitySchema {
            schema: Arc::new(Schema::new(fields)),
            attribute_fields: attribute_types,
        },
        link_count,
    )
}
