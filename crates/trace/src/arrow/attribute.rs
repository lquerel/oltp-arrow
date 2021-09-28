use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, PrimitiveBuilder, StringBuilder,
    StringDictionaryBuilder, UInt64Builder,
};
use arrow::datatypes::{
    ArrowDictionaryKeyType, DataType, Field, UInt16Type, UInt32Type, UInt8Type,
};
use serde_json::Value;
use twox_hash::RandomXxHashBuilder64;

use common::Attributes;

use crate::arrow::{EntitySchema, FieldInfo, FieldType};

pub fn infer_attribute_types(
    attributes: &Attributes,
    attribute_types: &mut HashMap<String, FieldInfo, RandomXxHashBuilder64>,
) {
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
                        cardinality: Default::default(),
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
                                cardinality: Default::default(),
                            }
                        } else if number.is_i64() {
                            FieldInfo {
                                non_null_count: 1,
                                field_type: FieldType::I64,
                                cardinality: Default::default(),
                            }
                        } else {
                            FieldInfo {
                                non_null_count: 1,
                                field_type: FieldType::F64,
                                cardinality: Default::default(),
                            }
                        }
                    });
            }
            Value::String(_) => {
                attribute_types
                    .entry(kv.0.clone())
                    .and_modify(|field_info| {
                        field_info
                            .cardinality
                            .insert(kv.1.as_str().unwrap_or("").to_string());
                        field_info.non_null_count += 1;
                    })
                    .or_insert_with(|| {
                        let mut cardinality = HashSet::new();
                        cardinality.insert(kv.1.as_str().unwrap_or("").to_string());
                        FieldInfo {
                            non_null_count: 1,
                            field_type: FieldType::String,
                            cardinality,
                        }
                    });
            }
        }
    }
}

pub fn add_attribute_columns(
    attributes: Vec<Option<&Attributes>>,
    schema: &EntitySchema,
    columns: &mut Vec<ArrayRef>,
) {
    let row_count = attributes.len();

    for attribute in &schema.attribute_fields {
        match attribute.1.field_type {
            FieldType::U64 => {
                let mut builder = UInt64Builder::new(row_count);
                attributes.iter().for_each(|attrs| match attrs {
                    None => builder.append_null().unwrap(),
                    Some(attributes) => match attributes.get(attribute.0) {
                        None => builder.append_null().unwrap(),
                        Some(value) => {
                            if let Value::Number(number) = value {
                                builder
                                    .append_value(
                                        number.as_u64().expect("invalid attribute type inference"),
                                    )
                                    .unwrap();
                            } else {
                                builder.append_null().unwrap();
                            }
                        }
                    },
                });
                columns.push(Arc::new(builder.finish()));
            }
            FieldType::I64 => {
                let mut builder = Int64Builder::new(row_count);
                attributes.iter().for_each(|attrs| match attrs {
                    None => builder.append_null().unwrap(),
                    Some(attributes) => match attributes.get(attribute.0) {
                        None => builder.append_null().unwrap(),
                        Some(value) => {
                            if let Value::Number(number) = value {
                                builder
                                    .append_value(
                                        number.as_i64().expect("invalid attribute type inference"),
                                    )
                                    .unwrap();
                            } else {
                                builder.append_null().unwrap();
                            }
                        }
                    },
                });
                columns.push(Arc::new(builder.finish()));
            }
            FieldType::F64 => {
                let mut builder = Float64Builder::new(row_count);
                attributes.iter().for_each(|attrs| match attrs {
                    None => builder.append_null().unwrap(),
                    Some(attributes) => match attributes.get(attribute.0) {
                        None => builder.append_null().unwrap(),
                        Some(value) => {
                            if let Value::Number(number) = value {
                                builder
                                    .append_value(
                                        number.as_f64().expect("invalid attribute type inference"),
                                    )
                                    .unwrap();
                            } else {
                                builder.append_null().unwrap();
                            }
                        }
                    },
                });
                columns.push(Arc::new(builder.finish()));
            }
            FieldType::String => {
                if attribute.1.is_dictionary() {
                    let min_num_bits = min_num_bits_to_represent(attribute.1.cardinality.len());
                    if min_num_bits <= 8 {
                        let mut builder = StringDictionaryBuilder::new(
                            PrimitiveBuilder::<UInt8Type>::new(row_count),
                            StringBuilder::new(row_count),
                        );
                        build_dictionary(&attributes, attribute, &mut builder);
                        columns.push(Arc::new(builder.finish()));
                    } else if min_num_bits <= 16 {
                        let mut builder = StringDictionaryBuilder::new(
                            PrimitiveBuilder::<UInt16Type>::new(row_count),
                            StringBuilder::new(row_count),
                        );
                        build_dictionary(&attributes, attribute, &mut builder);
                        columns.push(Arc::new(builder.finish()));
                    } else {
                        let mut builder = StringDictionaryBuilder::new(
                            PrimitiveBuilder::<UInt32Type>::new(row_count),
                            StringBuilder::new(row_count),
                        );
                        build_dictionary(&attributes, attribute, &mut builder);
                        columns.push(Arc::new(builder.finish()));
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
                    columns.push(Arc::new(builder.finish()));
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
                columns.push(Arc::new(builder.finish()));
            }
        }
    }
}

fn build_dictionary<K>(
    attributes: &[Option<&Attributes>],
    attribute: (&String, &FieldInfo),
    builder: &mut StringDictionaryBuilder<K>,
) where
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

pub fn add_attribute_fields(
    attribute_types: &HashMap<String, FieldInfo, RandomXxHashBuilder64>,
    fields: &mut Vec<Field>,
) {
    for attribute_info in attribute_types.iter() {
        match attribute_info.1.field_type {
            FieldType::U64 => {
                fields.push(Field::new(
                    &format!("attributes_{}", attribute_info.0),
                    DataType::UInt64,
                    true,
                ));
            }
            FieldType::I64 => {
                fields.push(Field::new(
                    &format!("attributes_{}", attribute_info.0),
                    DataType::Int64,
                    true,
                ));
            }
            FieldType::F64 => {
                fields.push(Field::new(
                    &format!("attributes_{}", attribute_info.0),
                    DataType::Float64,
                    true,
                ));
            }
            FieldType::String => {
                if attribute_info.1.is_dictionary() {
                    let min_num_bits =
                        min_num_bits_to_represent(attribute_info.1.cardinality.len());
                    if min_num_bits <= 8 {
                        fields.push(Field::new(
                            &format!("attributes_{}", attribute_info.0),
                            DataType::Dictionary(
                                Box::new(DataType::UInt8),
                                Box::new(DataType::Utf8),
                            ),
                            true,
                        ));
                    } else if min_num_bits <= 16 {
                        fields.push(Field::new(
                            &format!("attributes_{}", attribute_info.0),
                            DataType::Dictionary(
                                Box::new(DataType::UInt16),
                                Box::new(DataType::Utf8),
                            ),
                            true,
                        ));
                    } else {
                        fields.push(Field::new(
                            &format!("attributes_{}", attribute_info.0),
                            DataType::Dictionary(
                                Box::new(DataType::UInt32),
                                Box::new(DataType::Utf8),
                            ),
                            true,
                        ));
                    };
                } else {
                    fields.push(Field::new(
                        &format!("attributes_{}", attribute_info.0),
                        DataType::Utf8,
                        true,
                    ));
                }
            }
            FieldType::Bool => {
                fields.push(Field::new(
                    &format!("attributes_{}", attribute_info.0),
                    DataType::Boolean,
                    true,
                ));
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
