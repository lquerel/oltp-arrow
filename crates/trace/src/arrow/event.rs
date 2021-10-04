use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray, UInt32Array, UInt32Builder, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use common::{Event, Span};

use crate::arrow::attribute::{add_attribute_columns, add_attribute_fields, attribute_fields, infer_event_attribute_schema};
use crate::arrow::statistics::ColumnsStatistics;
use crate::arrow::{serialize, string_non_nullable_field, u32_non_nullable_field, u32_nullable_field, u64_non_nullable_field, DataColumns, EntitySchema};

pub fn serialize_events_from_row_oriented_data_source(
    stats: &mut ColumnsStatistics,
    event_schema: EntitySchema,
    spans: &[Span],
) -> Result<Vec<u8>, ArrowError> {
    let events: Vec<(usize, &Event)> = spans
        .iter()
        .enumerate()
        .filter(|(_, span)| span.events.is_some())
        .flat_map(|(id, span)| span.events.as_ref().unwrap().iter().map(move |event| (id, event)))
        .collect();

    let mut dropped_attributes_count = UInt32Builder::new(events.len());

    for (_, event) in events.iter() {
        match event.dropped_attributes_count {
            Some(value) => dropped_attributes_count.append_value(value),
            None => dropped_attributes_count.append_null(),
        }?;
    }

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from_iter_values(events.iter().map(|(id, _)| *id as u32))),
        Arc::new(UInt64Array::from_iter_values(events.iter().map(|(_, event)| event.time_unix_nano))),
        Arc::new(StringArray::from_iter_values(events.iter().map(|(_, event)| event.name.clone()))),
        Arc::new(dropped_attributes_count.finish()),
    ];

    add_attribute_columns(events.iter().map(|(_, event)| Some(&event.attributes)).collect(), &event_schema, &mut columns);

    stats.report(event_schema.schema.clone(), &columns);

    let batch = RecordBatch::try_new(event_schema.schema.clone(), columns)?;

    let mut writer = StreamWriter::try_new(Vec::new(), event_schema.schema.as_ref())?;
    writer.write(&batch)?;
    writer.finish()?;
    writer.into_inner()
}

pub fn serialize_events_from_column_oriented_data_source(stats: &mut ColumnsStatistics, data_columns: &DataColumns) -> Result<Vec<u8>, ArrowError> {
    let mut fields = vec![];
    let mut columns = vec![];
    let events = &data_columns.events;

    u32_non_nullable_field("id", &events.id_column, &mut fields, &mut columns);
    u64_non_nullable_field("time_unix_nano", &events.time_unix_nano_column, &mut fields, &mut columns);
    string_non_nullable_field("name", &events.name_column, &mut fields, &mut columns);
    u32_nullable_field("dropped_attributes_count", &events.dropped_attributes_count_column, &mut fields, &mut columns);

    attribute_fields("attributes_", &data_columns.events.attributes_column, &mut fields, &mut columns);

    serialize(stats, fields, columns)
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
