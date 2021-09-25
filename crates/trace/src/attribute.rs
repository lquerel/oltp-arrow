use serde_json::Value;
use common::{Attributes};
use std::collections::HashMap;
use std::sync::Arc;
use arrow::datatypes::{Field, DataType};
use crate::arrow::{FieldType, EntitySchema};
use arrow::array::{ArrayRef, UInt64Builder, Int64Builder, Float64Builder, StringBuilder, BooleanBuilder};

pub fn infer_attribute_types(attributes: &Attributes, attribute_types: &mut HashMap<String, FieldType>) {
    for kv in attributes {
        match kv.1 {
            Value::Null | Value::Array(_) | Value::Object(_) => {}
            Value::Bool(_) => {
                attribute_types.entry(kv.0.clone())
                    .and_modify(|field_type| {
                        if *field_type != FieldType::Bool {
                            *field_type = FieldType::String;
                        }
                    })
                    .or_insert_with(|| FieldType::Bool);
            }
            Value::Number(number) => {
                attribute_types.entry(kv.0.clone())
                    .and_modify(|field_type| {
                        if *field_type == FieldType::U64 {
                            if number.is_u64() {
                                // no type promotion
                            } else if number.is_i64() {
                                *field_type = FieldType::I64;
                            } else if number.is_f64() {
                                *field_type = FieldType::F64;
                            }
                        } else if *field_type == FieldType::I64 {
                            if number.is_f64() {
                                *field_type = FieldType::F64;
                            }
                        }
                    })
                    .or_insert_with(|| {
                        if number.is_u64() {
                            FieldType::U64
                        } else if number.is_i64() {
                            FieldType::I64
                        } else {
                            FieldType::F64
                        }
                    });
            }
            Value::String(_) => {
                attribute_types.insert(kv.0.clone(), FieldType::String);
            }
        }
    }
}

pub fn add_attribute_columns(attributes: Vec<Option<&Attributes>>, schema: &EntitySchema, columns: &mut Vec<ArrayRef>) {
    let row_count = attributes.len();

    for attribute in &schema.attribute_fields {
        match attribute.1 {
            FieldType::U64 => {
                let mut builder = UInt64Builder::new(row_count);
                attributes.iter().for_each(|attrs| match attrs {
                    None => builder.append_null().unwrap(),
                    Some(attributes) => {
                        match attributes.get(attribute.0) {
                            None => builder.append_null().unwrap(),
                            Some(value) => {
                                if let Value::Number(number) = value {
                                    builder.append_value(number.as_u64().expect("invalid attribute type inference")).unwrap();
                                } else {
                                    builder.append_null().unwrap();
                                }
                            }
                        }
                    }
                });
                columns.push(Arc::new(builder.finish()));
            }
            FieldType::I64 => {
                let mut builder = Int64Builder::new(row_count);
                attributes.iter().for_each(|attrs| match attrs {
                    None => builder.append_null().unwrap(),
                    Some(attributes) => {
                        match attributes.get(attribute.0) {
                            None => builder.append_null().unwrap(),
                            Some(value) => {
                                if let Value::Number(number) = value {
                                    builder.append_value(number.as_i64().expect("invalid attribute type inference")).unwrap();
                                } else {
                                    builder.append_null().unwrap();
                                }
                            }
                        }
                    }
                });
                columns.push(Arc::new(builder.finish()));
            }
            FieldType::F64 => {
                let mut builder = Float64Builder::new(row_count);
                attributes.iter().for_each(|attrs| match attrs {
                    None => builder.append_null().unwrap(),
                    Some(attributes) => {
                        match attributes.get(attribute.0) {
                            None => builder.append_null().unwrap(),
                            Some(value) => {
                                if let Value::Number(number) = value {
                                    builder.append_value(number.as_f64().expect("invalid attribute type inference")).unwrap();
                                } else {
                                    builder.append_null().unwrap();
                                }
                            }
                        }
                    }
                });
                columns.push(Arc::new(builder.finish()));
            }
            FieldType::String => {
                let mut builder = StringBuilder::new(row_count);
                attributes.iter().for_each(|attrs| match attrs {
                    None => builder.append_null().unwrap(),
                    Some(attributes) => {
                        match attributes.get(attribute.0) {
                            None => builder.append_null().unwrap(),
                            Some(value) => {
                                if let Value::String(string) = value {
                                    builder.append_value(string.clone()).unwrap();
                                } else {
                                    builder.append_null().unwrap();
                                }
                            }
                        }
                    }
                });
                columns.push(Arc::new(builder.finish()));
            }
            FieldType::Bool => {
                let mut builder = BooleanBuilder::new(row_count);
                attributes.iter().for_each(|attrs| match attrs {
                    None => builder.append_null().unwrap(),
                    Some(attributes) => {
                        match attributes.get(attribute.0) {
                            None => builder.append_null().unwrap(),
                            Some(value) => {
                                if let Value::Bool(bool) = value {
                                    builder.append_value(*bool).unwrap();
                                } else {
                                    builder.append_null().unwrap();
                                }
                            }
                        }
                    }
                });
                columns.push(Arc::new(builder.finish()));
            }
        }
    }
}

pub fn add_attribute_fields(attribute_types: &HashMap<String, FieldType>, fields: &mut Vec<Field>) {
    for attribute_type in attribute_types.iter() {
        match attribute_type.1 {
            FieldType::U64 => {
                fields.push(Field::new(&format!("attributes_{}", attribute_type.0), DataType::UInt64, true));
            }
            FieldType::I64 => {
                fields.push(Field::new(&format!("attributes_{}", attribute_type.0), DataType::Int64, true));
            }
            FieldType::F64 => {
                fields.push(Field::new(&format!("attributes_{}", attribute_type.0), DataType::Float64, true));
            }
            FieldType::String => {
                fields.push(Field::new(&format!("attributes_{}", attribute_type.0), DataType::Utf8, true));
            }
            FieldType::Bool => {
                fields.push(Field::new(&format!("attributes_{}", attribute_type.0), DataType::Boolean, true));
            }
        }
    }
}