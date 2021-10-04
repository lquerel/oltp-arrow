use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, PrimitiveArray, PrimitiveBuilder, StringBuilder, StringDictionaryBuilder, UInt64Builder,
};
use arrow::datatypes::{ArrowDictionaryKeyType, ArrowPrimitiveType, DataType, Field, UInt16Type, UInt32Type, UInt8Type};
use serde_json::{Number, Value};
use twox_hash::RandomXxHashBuilder64;

use common::{Attributes, Span};

use crate::arrow::schema::{FieldInfo, FieldType};
use crate::arrow::{DataColumn, EntitySchema};

pub fn infer_span_attribute_schema(spans: &[Span]) -> HashMap<String, FieldInfo, RandomXxHashBuilder64> {
    let mut schema: HashMap<String, FieldInfo, RandomXxHashBuilder64> = Default::default();

    for span in spans {
        if let Some(attributes) = &span.attributes {
            infer_attribute_types(attributes, &mut schema);
        }
    }

    schema
}

pub fn infer_event_attribute_schema(spans: &[Span]) -> (usize, HashMap<String, FieldInfo, RandomXxHashBuilder64>) {
    let mut attribute_types: HashMap<String, FieldInfo, RandomXxHashBuilder64> = Default::default();
    let mut event_count = 0;

    for span in spans {
        if let Some(events) = &span.events {
            for event in events {
                infer_attribute_types(&event.attributes, &mut attribute_types);
                event_count += 1;
            }
        }
    }

    (event_count, attribute_types)
}

pub fn infer_link_attribute_schema(spans: &[Span]) -> (usize, HashMap<String, FieldInfo, RandomXxHashBuilder64>) {
    let mut attribute_types: HashMap<String, FieldInfo, RandomXxHashBuilder64> = Default::default();
    let mut link_count = 0;

    for span in spans {
        if let Some(links) = &span.links {
            for link in links {
                infer_attribute_types(&link.attributes, &mut attribute_types);
                link_count += 1;
            }
        }
    }

    (link_count, attribute_types)
}

pub fn infer_attribute_types(attributes: &Attributes, attribute_types: &mut HashMap<String, FieldInfo, RandomXxHashBuilder64>) {
    for kv in attributes {
        match kv.1 {
            Value::Null | Value::Array(_) | Value::Object(_) => {}
            Value::Bool(_) => {
                attribute_types
                    .entry(kv.0.clone())
                    .and_modify(|field_info| {
                        if field_info.field_type != FieldType::Bool {
                            field_info.field_type = FieldType::String;
                        }
                        field_info.non_null_count += 1;
                    })
                    .or_insert_with(|| FieldInfo {
                        non_null_count: 1,
                        field_type: FieldType::Bool,
                        dictionary_values: Default::default(),
                    });
            }
            Value::Number(number) => {
                attribute_types
                    .entry(kv.0.clone())
                    .and_modify(|field_info| {
                        if field_info.field_type == FieldType::U64 {
                            if number.is_u64() {
                                // no type promotion
                            } else if number.is_i64() {
                                field_info.field_type = FieldType::I64;
                            } else if number.is_f64() {
                                field_info.field_type = FieldType::F64;
                            }
                        } else if field_info.field_type == FieldType::I64 && number.is_f64() {
                            field_info.field_type = FieldType::F64;
                        }
                        field_info.non_null_count += 1;
                    })
                    .or_insert_with(|| {
                        if number.is_u64() {
                            FieldInfo {
                                non_null_count: 1,
                                field_type: FieldType::U64,
                                dictionary_values: Default::default(),
                            }
                        } else if number.is_i64() {
                            FieldInfo {
                                non_null_count: 1,
                                field_type: FieldType::I64,
                                dictionary_values: Default::default(),
                            }
                        } else {
                            FieldInfo {
                                non_null_count: 1,
                                field_type: FieldType::F64,
                                dictionary_values: Default::default(),
                            }
                        }
                    });
            }
            Value::String(_) => {
                attribute_types
                    .entry(kv.0.clone())
                    .and_modify(|field_info| {
                        field_info.dictionary_values.insert(kv.1.as_str().unwrap_or("").to_string());
                        field_info.non_null_count += 1;
                    })
                    .or_insert_with(|| {
                        let mut cardinality = HashSet::new();
                        cardinality.insert(kv.1.as_str().unwrap_or("").to_string());
                        FieldInfo {
                            non_null_count: 1,
                            field_type: FieldType::String,
                            dictionary_values: cardinality,
                        }
                    });
            }
        }
    }
}

fn build_primitive_array<T, F>(
    attribute_name: &str,
    attributes: &[Option<&Attributes>],
    builder: &mut PrimitiveBuilder<T>,
    num_converter: F,
) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
    F: Fn(&Number) -> Option<T::Native>,
{
    attributes.iter().for_each(|attrs| match attrs {
        None => builder.append_null().unwrap(),
        Some(attributes) => match attributes.get(attribute_name) {
            None => builder.append_null().unwrap(),
            Some(value) => {
                if let Value::Number(number) = value {
                    builder.append_value(num_converter(number).expect("invalid attribute type inference")).unwrap();
                } else {
                    builder.append_null().unwrap();
                }
            }
        },
    });
    builder.finish()
}

pub fn add_attribute_columns(attributes: Vec<Option<&Attributes>>, schema: &EntitySchema, columns: &mut Vec<ArrayRef>) {
    let row_count = attributes.len();

    for attribute in &schema.attribute_fields {
        match attribute.1.field_type {
            FieldType::U64 => {
                let mut builder = UInt64Builder::new(row_count);
                columns.push(Arc::new(build_primitive_array(attribute.0, &attributes, &mut builder, |number| {
                    number.as_u64()
                })));
            }
            FieldType::I64 => {
                let mut builder = Int64Builder::new(row_count);
                columns.push(Arc::new(build_primitive_array(attribute.0, &attributes, &mut builder, |number| {
                    number.as_i64()
                })));
            }
            FieldType::F64 => {
                let mut builder = Float64Builder::new(row_count);
                columns.push(Arc::new(build_primitive_array(attribute.0, &attributes, &mut builder, |number| {
                    number.as_f64()
                })));
            }
            FieldType::String => {
                if attribute.1.is_dictionary() {
                    let min_num_bits = min_num_bits_to_represent(attribute.1.dictionary_values.len());
                    if min_num_bits <= 8 {
                        let mut builder = StringDictionaryBuilder::new(PrimitiveBuilder::<UInt8Type>::new(row_count), StringBuilder::new(row_count));
                        build_dictionary(&attributes, attribute, &mut builder);
                        columns.push(Arc::new(builder.finish()));
                    } else if min_num_bits <= 16 {
                        let mut builder = StringDictionaryBuilder::new(PrimitiveBuilder::<UInt16Type>::new(row_count), StringBuilder::new(row_count));
                        build_dictionary(&attributes, attribute, &mut builder);
                        columns.push(Arc::new(builder.finish()));
                    } else {
                        let mut builder = StringDictionaryBuilder::new(PrimitiveBuilder::<UInt32Type>::new(row_count), StringBuilder::new(row_count));
                        build_dictionary(&attributes, attribute, &mut builder);

                        let array = builder.finish();
                        columns.push(Arc::new(array));
                    };
                } else {
                    let mut builder = StringBuilder::new(row_count);
                    attributes.iter().for_each(|attrs| match attrs {
                        None => builder.append_null().unwrap(),
                        Some(attributes) => match attributes.get(attribute.0) {
                            None => builder.append_null().unwrap(),
                            Some(value) => {
                                if let Value::String(string) = value {
                                    builder.append_value(string.clone()).unwrap();
                                } else {
                                    builder.append_null().unwrap();
                                }
                            }
                        },
                    });
                    let array = builder.finish();
                    columns.push(Arc::new(array));
                }
            }
            FieldType::Bool => {
                let mut builder = BooleanBuilder::new(row_count);
                attributes.iter().for_each(|attrs| match attrs {
                    None => builder.append_null().unwrap(),
                    Some(attributes) => match attributes.get(attribute.0) {
                        None => builder.append_null().unwrap(),
                        Some(value) => {
                            if let Value::Bool(bool) = value {
                                builder.append_value(*bool).unwrap();
                            } else {
                                builder.append_null().unwrap();
                            }
                        }
                    },
                });
                let array = builder.finish();
                columns.push(Arc::new(array));
            }
        }
    }
}

pub fn attribute_fields(prefix: &str, attributes_column: &HashMap<String, DataColumn>, fields: &mut Vec<Field>, columns: &mut Vec<ArrayRef>) {
    attributes_column.iter().for_each(|(name, data_column)| match data_column {
        DataColumn::U64Column { missing, values } => {
            fields.push(Field::new(&format!("{}{}", prefix, name), DataType::UInt64, *missing > 0));
            let mut builder = UInt64Builder::new(values.len());
            values.iter().for_each(|value| {
                match value {
                    None => builder.append_null(),
                    Some(value) => builder.append_value(*value),
                }
                .expect("append data into builder failed")
            });
            columns.push(Arc::new(builder.finish()));
        }
        DataColumn::I64Column { missing, values } => {
            fields.push(Field::new(&format!("{}{}", prefix, name), DataType::Int64, *missing > 0));
            let mut builder = Int64Builder::new(values.len());
            values.iter().for_each(|value| {
                match value {
                    None => builder.append_null(),
                    Some(value) => builder.append_value(*value),
                }
                .expect("append data into builder failed")
            });
            columns.push(Arc::new(builder.finish()));
        }
        DataColumn::F64Column { missing, values } => {
            fields.push(Field::new(&format!("{}{}", prefix, name), DataType::Float64, *missing > 0));
            let mut builder = Float64Builder::new(values.len());
            values.iter().for_each(|value| {
                match value {
                    None => builder.append_null(),
                    Some(value) => builder.append_value(*value),
                }
                .expect("append data into builder failed")
            });
            columns.push(Arc::new(builder.finish()));
        }
        DataColumn::StringColumn { missing, values } => {
            let mut dictionary_values = HashSet::new();
            let mut non_null_count = 0;
            let row_count = values.len();
            values.iter().for_each(|v| {
                if let Some(v) = v {
                    dictionary_values.insert(v);
                    non_null_count += 1;
                }
            });
            let is_dictionary = (dictionary_values.len() as f64 / non_null_count as f64) < 0.2;
            if is_dictionary {
                let min_num_bits = min_num_bits_to_represent(dictionary_values.len());
                if min_num_bits <= 8 {
                    let mut builder = StringDictionaryBuilder::new(PrimitiveBuilder::<UInt8Type>::new(row_count), StringBuilder::new(row_count));
                    values.iter().for_each(|v| match v {
                        None => builder.append_null().unwrap(),
                        Some(v) => {
                            builder.append(v.clone()).unwrap();
                        }
                    });
                    fields.push(Field::new(
                        &format!("{}{}", prefix, name),
                        DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
                        *missing > 0,
                    ));
                    columns.push(Arc::new(builder.finish()));
                } else if min_num_bits <= 16 {
                    let mut builder = StringDictionaryBuilder::new(PrimitiveBuilder::<UInt16Type>::new(row_count), StringBuilder::new(row_count));
                    values.iter().for_each(|v| match v {
                        None => builder.append_null().unwrap(),
                        Some(v) => {
                            builder.append(v.clone()).unwrap();
                        }
                    });
                    fields.push(Field::new(
                        &format!("{}{}", prefix, name),
                        DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
                        *missing > 0,
                    ));
                    columns.push(Arc::new(builder.finish()));
                } else {
                    let mut builder = StringDictionaryBuilder::new(PrimitiveBuilder::<UInt32Type>::new(row_count), StringBuilder::new(row_count));
                    values.iter().for_each(|v| match v {
                        None => builder.append_null().unwrap(),
                        Some(v) => {
                            builder.append(v.clone()).unwrap();
                        }
                    });
                    let array = builder.finish();
                    fields.push(Field::new(
                        &format!("{}{}", prefix, name),
                        DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                        *missing > 0,
                    ));
                    columns.push(Arc::new(array));
                };
            } else {
                fields.push(Field::new(&format!("{}{}", prefix, name), DataType::Utf8, *missing > 0));
                let mut builder = StringBuilder::new(values.len());
                values.iter().for_each(|value| {
                    match value {
                        None => builder.append_null(),
                        Some(value) => builder.append_value(value.clone()),
                    }
                    .expect("append data into builder failed")
                });
                columns.push(Arc::new(builder.finish()));
            }
        }
        DataColumn::BoolColumn { missing, values } => {
            fields.push(Field::new(&format!("{}{}", prefix, name), DataType::Boolean, *missing > 0));
            let mut builder = BooleanBuilder::new(values.len());
            values.iter().for_each(|value| {
                match value {
                    None => builder.append_null(),
                    Some(value) => builder.append_value(*value),
                }
                .expect("append data into builder failed")
            });
            columns.push(Arc::new(builder.finish()));
        }
    });
}

fn build_dictionary<K>(attributes: &[Option<&Attributes>], attribute: (&String, &FieldInfo), builder: &mut StringDictionaryBuilder<K>)
where
    K: ArrowDictionaryKeyType,
{
    attributes.iter().for_each(|attrs| match attrs {
        None => builder.append_null().unwrap(),
        Some(attributes) => match attributes.get(attribute.0) {
            None => builder.append_null().unwrap(),
            Some(value) => {
                if let Value::String(string) = value {
                    builder.append(string.clone()).unwrap();
                } else {
                    builder.append_null().unwrap();
                }
            }
        },
    });
}

pub fn add_attribute_fields(attribute_types: &HashMap<String, FieldInfo, RandomXxHashBuilder64>, fields: &mut Vec<Field>) {
    for attribute_info in attribute_types.iter() {
        match attribute_info.1.field_type {
            FieldType::U64 => {
                fields.push(Field::new(&format!("attributes_{}", attribute_info.0), DataType::UInt64, true));
            }
            FieldType::I64 => {
                fields.push(Field::new(&format!("attributes_{}", attribute_info.0), DataType::Int64, true));
            }
            FieldType::F64 => {
                fields.push(Field::new(&format!("attributes_{}", attribute_info.0), DataType::Float64, true));
            }
            FieldType::String => {
                if attribute_info.1.is_dictionary() {
                    let min_num_bits = min_num_bits_to_represent(attribute_info.1.dictionary_values.len());
                    if min_num_bits <= 8 {
                        fields.push(Field::new(
                            &format!("attributes_{}", attribute_info.0),
                            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
                            true,
                        ));
                    } else if min_num_bits <= 16 {
                        fields.push(Field::new(
                            &format!("attributes_{}", attribute_info.0),
                            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
                            true,
                        ));
                    } else {
                        fields.push(Field::new(
                            &format!("attributes_{}", attribute_info.0),
                            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
                            true,
                        ));
                    };
                } else {
                    fields.push(Field::new(&format!("attributes_{}", attribute_info.0), DataType::Utf8, true));
                }
            }
            FieldType::Bool => {
                fields.push(Field::new(&format!("attributes_{}", attribute_info.0), DataType::Boolean, true));
            }
        }
    }
}

const fn num_bits<T>() -> usize {
    std::mem::size_of::<T>() * 8
}

fn min_num_bits_to_represent(x: usize) -> u32 {
    assert!(x > 0);
    num_bits::<usize>() as u32 - x.leading_zeros()
}
