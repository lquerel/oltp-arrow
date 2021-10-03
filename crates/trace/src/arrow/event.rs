use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, UInt32Array, UInt32Builder, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use common::{Event, Span};

use crate::arrow::attribute::{add_attribute_columns, add_attribute_fields, infer_event_attribute_schema, add_attribute_data_columns};
use crate::arrow::{EntitySchema, DataColumns};
use crate::arrow::statistics::{ColumnsStatistics};

pub fn serialize_events_from_row_oriented_data_source(stats: &mut ColumnsStatistics, event_schema: EntitySchema, spans: &[Span]) -> Result<Vec<u8>, ArrowError> {
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

    stats.report(event_schema.schema.clone(), &columns);

    let batch = RecordBatch::try_new(event_schema.schema.clone(), columns)?;

    let mut writer = StreamWriter::try_new(Vec::new(), event_schema.schema.as_ref())?;
    writer.write(&batch)?;
    writer.finish()?;
    writer.into_inner()
}

pub fn serialize_events_from_column_oriented_data_source(stats: &mut ColumnsStatistics, data_columns: &DataColumns) -> Result<Vec<u8>, ArrowError> {
    let mut fields = vec![
        Field::new("id", DataType::UInt32, false),
        Field::new("time_unix_nano", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("dropped_attributes_count", DataType::UInt32, true),
    ];

    let mut dropped_attributes_count_builder = UInt32Builder::new(data_columns.events.dropped_attributes_count_column.len());
    data_columns.events.dropped_attributes_count_column.iter().for_each(|value| match value {
        None => dropped_attributes_count_builder.append_null(),
        Some(value) => dropped_attributes_count_builder.append_value(*value)
    }.expect("append data into dropped_attributes_count_builder failed"));

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from_iter_values(
            data_columns.events.id_column.iter().map(|id| *id as u32),
        )),
        Arc::new(UInt64Array::from_iter_values(
            data_columns.events.time_unix_nano_column.iter().map(|time_unix_nano| *time_unix_nano),
        )),
        Arc::new(StringArray::from_iter_values(
            data_columns.events.name_column.iter().map(|name| name.clone()),
        )),
        Arc::new(dropped_attributes_count_builder.finish()),
    ];

    add_attribute_data_columns(&mut fields, &mut columns, &data_columns.events.attributes_column);

    let schema = Arc::new(Schema::new(fields));
    stats.report(schema.clone(), &columns);
    let batch = RecordBatch::try_new(schema.clone(), columns)?;

    let mut writer = StreamWriter::try_new(Vec::new(), schema.as_ref())?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(writer.into_inner()?)
}

pub fn infer_event_schema(spans: &[Span]) -> (EntitySchema, usize) {
    let mut fields = vec![
        Field::new("id", DataType::UInt32, false),
        Field::new("time_unix_nano", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("dropped_attributes_count", DataType::UInt32, true),
    ];

    let (event_count, attribute_types) = infer_event_attribute_schema(spans);

    add_attribute_fields(&attribute_types, &mut fields);

    (
        EntitySchema {
            schema: Arc::new(Schema::new(fields)),
            attribute_fields: attribute_types,
        },
        event_count,
    )
}
