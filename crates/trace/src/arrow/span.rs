use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, BinaryBuilder, StringArray, StringBuilder, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use common::Span;

use crate::arrow::attribute::{add_attribute_columns, add_attribute_fields, infer_span_attribute_schema, add_attribute_data_columns};
use crate::arrow::{EntitySchema, DataColumns};
use crate::arrow::statistics::{ColumnsStatistics};

pub fn serialize_spans_from_row_oriented_data_source(
    stats: &mut ColumnsStatistics,
    span_schema: EntitySchema,
    spans: &[Span],
    gen_id_column: bool,
) -> Result<Vec<u8>, ArrowError> {
    let mut end_time_unix_nano = UInt64Builder::new(spans.len());
    let mut trace_state = StringBuilder::new(spans.len());
    let mut parent_span_id = BinaryBuilder::new(spans.len());
    let mut kind = UInt32Builder::new(spans.len());
    let mut dropped_attributes_count = UInt32Builder::new(spans.len());
    let mut dropped_events_count = UInt32Builder::new(spans.len());
    let mut dropped_links_count = UInt32Builder::new(spans.len());

    for span in spans.iter() {
        match span.end_time_unix_nano {
            Some(value) => end_time_unix_nano.append_value(value),
            None => end_time_unix_nano.append_null(),
        }?;

        match &span.trace_state {
            Some(value) => trace_state.append_value(value),
            None => trace_state.append_null(),
        }?;

        match &span.parent_span_id {
            Some(value) => parent_span_id.append_value(value.clone()),
            None => parent_span_id.append_null(),
        }?;

        match span.kind {
            Some(value) => kind.append_value(value as u32),
            None => kind.append_null(),
        }?;

        match span.dropped_attributes_count {
            Some(value) => dropped_attributes_count.append_value(value),
            None => dropped_attributes_count.append_null(),
        }?;

        match span.dropped_events_count {
            Some(value) => dropped_events_count.append_value(value),
            None => dropped_events_count.append_null(),
        }?;

        match span.dropped_links_count {
            Some(value) => dropped_links_count.append_value(value),
            None => dropped_links_count.append_null(),
        }?;
    }

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from_iter_values(
            spans.iter().map(|span| span.start_time_unix_nano),
        )),
        Arc::new(end_time_unix_nano.finish()),
        Arc::new(BinaryArray::from(
            spans
                .iter()
                .map(|span| span.trace_id.as_bytes())
                .collect::<Vec<&[u8]>>(),
        )),
        Arc::new(BinaryArray::from(
            spans
                .iter()
                .map(|span| span.span_id.as_bytes())
                .collect::<Vec<&[u8]>>(),
        )),
        Arc::new(trace_state.finish()),
        Arc::new(parent_span_id.finish()),
        Arc::new(StringArray::from_iter_values(
            spans.iter().map(|span| span.name.clone()),
        )),
        Arc::new(kind.finish()),
        Arc::new(dropped_attributes_count.finish()),
        Arc::new(dropped_events_count.finish()),
        Arc::new(dropped_links_count.finish()),
    ];

    if gen_id_column {
        columns.push(Arc::new(UInt32Array::from_iter_values(
            0..spans.len() as u32,
        )));
    }

    add_attribute_columns(
        spans.iter().map(|span| span.attributes.as_ref()).collect(),
        &span_schema,
        &mut columns,
    );

    stats.report(span_schema.schema.clone(), &columns);

    let batch = RecordBatch::try_new(span_schema.schema.clone(), columns).unwrap();

    // dbg!(&batch);

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

pub fn serialize_spans_from_column_oriented_data_source(stats: &mut ColumnsStatistics, data_columns: &DataColumns) -> Result<Vec<u8>, ArrowError> {
    let mut fields = vec![
        Field::new("start_time_unix_nano", DataType::UInt64, false),
        Field::new("end_time_unix_nano", DataType::UInt64, true),
        Field::new("trace_id", DataType::Binary, false),
        Field::new("span_id", DataType::Binary, false),
        Field::new("trace_state", DataType::Utf8, true),
        Field::new("parent_span_id", DataType::Binary, true),
        Field::new("name", DataType::Utf8, false),
        Field::new("kind", DataType::UInt32, true),
        Field::new("dropped_attributes_count", DataType::UInt32, true),
        Field::new("dropped_events_count", DataType::UInt32, true),
        Field::new("dropped_links_count", DataType::UInt32, true),
    ];

    let mut end_time_unix_nano_builder = UInt64Builder::new(data_columns.spans.end_time_unix_nano_column.len());
    data_columns.spans.end_time_unix_nano_column.iter().for_each(|value| match value {
        None => end_time_unix_nano_builder.append_null(),
        Some(value) => end_time_unix_nano_builder.append_value(*value)
    }.expect("append data into dropped_attributes_count_builder failed"));

    let mut dropped_attributes_count_builder = UInt32Builder::new(data_columns.spans.dropped_attributes_count_column.len());
    data_columns.spans.dropped_attributes_count_column.iter().for_each(|value| match value {
        None => dropped_attributes_count_builder.append_null(),
        Some(value) => dropped_attributes_count_builder.append_value(*value)
    }.expect("append data into dropped_attributes_count_builder failed"));

    let mut dropped_events_count_builder = UInt32Builder::new(data_columns.spans.dropped_events_count_column.len());
    data_columns.spans.dropped_events_count_column.iter().for_each(|value| match value {
        None => dropped_events_count_builder.append_null(),
        Some(value) => dropped_events_count_builder.append_value(*value)
    }.expect("append data into dropped_attributes_count_builder failed"));

    let mut dropped_links_count_builder = UInt32Builder::new(data_columns.spans.dropped_links_count_column.len());
    data_columns.spans.dropped_links_count_column.iter().for_each(|value| match value {
        None => dropped_links_count_builder.append_null(),
        Some(value) => dropped_links_count_builder.append_value(*value)
    }.expect("append data into dropped_attributes_count_builder failed"));

    let mut trace_state_builder = StringBuilder::new(data_columns.spans.trace_state_column.len());
    data_columns.spans.trace_state_column.iter().for_each(|value| match value {
        None => trace_state_builder.append_null(),
        Some(value) => trace_state_builder.append_value(value.clone())
    }.expect("append data into trace_state_builder failed"));

    let mut parent_span_id_builder = BinaryBuilder::new(data_columns.spans.parent_span_id_column.len());
    data_columns.spans.parent_span_id_column.iter().for_each(|value| match value {
        None => parent_span_id_builder.append_null(),
        Some(value) => parent_span_id_builder.append_value(value.as_bytes())
    }.expect("append data into parent_span_id_builder failed"));

    let mut kind_builder = UInt32Builder::new(data_columns.spans.kind_column.len());
    data_columns.spans.kind_column.iter().for_each(|value| match value {
        None => kind_builder.append_null(),
        Some(value) => kind_builder.append_value(*value as u32)
    }.expect("append data into dropped_attributes_count_builder failed"));

    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from_iter_values(
            data_columns.spans.start_time_unix_nano_column.iter().map(|id| *id),
        )),
        Arc::new(end_time_unix_nano_builder.finish()),
        Arc::new(BinaryArray::from(
            data_columns.spans.trace_id_column
                .iter()
                .map(|trace_id| trace_id.as_bytes())
                .collect::<Vec<&[u8]>>(),
        )),
        Arc::new(BinaryArray::from(
            data_columns.spans.span_id_column
                .iter()
                .map(|span_id| span_id.as_bytes())
                .collect::<Vec<&[u8]>>(),
        )),
        Arc::new(trace_state_builder.finish()),
        Arc::new(parent_span_id_builder.finish()),
        Arc::new(StringArray::from_iter_values(
            data_columns.spans.name_column.iter().map(|name| name.clone()),
        )),
        Arc::new(kind_builder.finish()),
        Arc::new(dropped_attributes_count_builder.finish()),
        Arc::new(dropped_events_count_builder.finish()),
        Arc::new(dropped_links_count_builder.finish()),
    ];

    add_attribute_data_columns(&mut fields, &mut columns, &data_columns.spans.attributes_column);

    let schema = Arc::new(Schema::new(fields));
    stats.report(schema.clone(), &columns);
    let batch = RecordBatch::try_new(schema.clone(), columns)?;

    let mut writer = StreamWriter::try_new(Vec::new(), schema.as_ref())?;
    writer.write(&batch)?;
    writer.finish()?;

    // let mut buf = Vec::new();
    // {
    //     let mut writer = LineDelimitedWriter::new(&mut buf);
    //     writer.write_batches(&[batch]).unwrap();
    // }
    //
    // println!("{}", String::from_utf8(buf).unwrap());

    Ok(writer.into_inner()?)
}

pub fn infer_span_schema(spans: &[Span], gen_id_column: bool) -> EntitySchema {
    let mut fields = vec![
        Field::new("start_time_unix_nano", DataType::UInt64, false),
        Field::new("end_time_unix_nano", DataType::UInt64, true),
        Field::new("trace_id", DataType::Binary, false),
        Field::new("span_id", DataType::Binary, false),
        Field::new("trace_state", DataType::Utf8, true),
        Field::new("parent_span_id", DataType::Binary, true),
        Field::new("name", DataType::Utf8, false),
        Field::new("kind", DataType::UInt32, true),
        Field::new("dropped_attributes_count", DataType::UInt32, true),
        Field::new("dropped_events_count", DataType::UInt32, true),
        Field::new("dropped_links_count", DataType::UInt32, true),
    ];

    if gen_id_column {
        fields.push(Field::new("id", DataType::UInt32, false));
    }

    let attribute_types = infer_span_attribute_schema(spans);

    add_attribute_fields(&attribute_types, &mut fields);

    EntitySchema {
        schema: Arc::new(Schema::new(fields)),
        attribute_fields: attribute_types,
    }
}

