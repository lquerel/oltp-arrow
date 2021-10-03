use std::collections::{BTreeMap};
use serde::Serialize;
use std::sync::Arc;
use arrow::datatypes::{Schema, DataType, UInt8Type, UInt16Type, UInt32Type};
use arrow::array::{ArrayRef, BooleanArray, Array, Int8Array, Int32Array, Int64Array, UInt8Array, UInt32Array, UInt64Array, Float64Array, BinaryArray, StringArray, DictionaryArray};
use itertools::Itertools;

#[derive(Debug, Serialize)]
pub struct StatisticsReporter {
    file: String,
    batches: Vec<BatchStatistics>,
    stats_enabled: bool,
}

#[derive(Debug, Serialize)]
pub struct ColumnStatistics {
    // Column type
    column_type: ColumnType,
    // Total number of values (including missing values)
    total_values: usize,
    // Number of unique values
    cardinality: usize,
    // Number of missing values
    missing_values: usize,
    dictionary: bool,
}

#[derive(Debug, Serialize)]
pub struct BatchStatistics {
    stats_enabled: bool,
    span_columns: ColumnsStatistics,
    event_columns: ColumnsStatistics,
    link_columns: ColumnsStatistics,
}

#[derive(Debug, Serialize)]
pub enum ColumnType {
    U8,
    U32,
    U64,
    I8,
    I32,
    I64,
    F64,
    String,
    Boolean,
    Binary,
}

impl StatisticsReporter {
    pub fn new(file: &str) -> Self {
        Self { file: file.into(), batches: vec![], stats_enabled: true }
    }

    pub fn noop() -> Self {
        Self { file: "".into(), batches: vec![], stats_enabled: false}
    }

    pub fn next_batch(&mut self) -> &mut BatchStatistics {
        self.batches.push(BatchStatistics {
            stats_enabled: self.stats_enabled,
            span_columns: ColumnsStatistics::new(self.stats_enabled),
            event_columns: ColumnsStatistics::new(self.stats_enabled),
            link_columns: ColumnsStatistics::new(self.stats_enabled)
        });
        self.batches.last_mut().unwrap()
    }
}

#[derive(Debug, Serialize)]
pub struct ColumnsStatistics {
    stats_enabled: bool,
    columns: BTreeMap<String, ColumnStatistics>,
}

impl BatchStatistics {
    pub fn span_stats(&mut self) -> &mut ColumnsStatistics {
        &mut self.span_columns
    }

    pub fn event_stats(&mut self) -> &mut ColumnsStatistics {
        &mut self.event_columns
    }

    pub fn link_stats(&mut self) -> &mut ColumnsStatistics {
        &mut self.link_columns
    }
}

impl ColumnsStatistics {
    pub fn new(stats_enabled: bool) -> ColumnsStatistics {
        Self { stats_enabled, columns: Default::default() }
    }

    pub fn report(&mut self, schema: Arc<Schema>, array_data: &[ArrayRef]) {
        if self.stats_enabled {
            let fields = schema.fields();

            if fields.len() != array_data.len() {
                panic!("schema definition not aligned with array data");
            }

            fields.iter().enumerate().for_each(|(i, field)| {
                let column_stats = match field.data_type() {
                    DataType::Boolean => {
                        let column = array_data[i].as_any().downcast_ref::<BooleanArray>().unwrap();
                        ColumnStatistics {
                            column_type: ColumnType::Boolean,
                            total_values: column.len(),
                            cardinality: column.values().iter().unique().count(),
                            missing_values: column.null_count(),
                            dictionary: false,
                        }
                    }
                    DataType::Int8 => {
                        let column = array_data[i].as_any().downcast_ref::<Int8Array>().unwrap();
                        ColumnStatistics {
                            column_type: ColumnType::I8,
                            total_values: column.len(),
                            cardinality: column.values().iter().unique().count(),
                            missing_values: column.null_count(),
                            dictionary: false,
                        }
                    }
                    DataType::Int32 => {
                        let column = array_data[i].as_any().downcast_ref::<Int32Array>().unwrap();
                        ColumnStatistics {
                            column_type: ColumnType::I32,
                            total_values: column.len(),
                            cardinality: column.values().iter().unique().count(),
                            missing_values: column.null_count(),
                            dictionary: false,
                        }
                    }
                    DataType::Int64 => {
                        let column = array_data[i].as_any().downcast_ref::<Int64Array>().unwrap();
                        ColumnStatistics {
                            column_type: ColumnType::I64,
                            total_values: column.len(),
                            cardinality: column.values().iter().unique().count(),
                            missing_values: column.null_count(),
                            dictionary: false,
                        }
                    }
                    DataType::UInt8 => {
                        let column = array_data[i].as_any().downcast_ref::<UInt8Array>().unwrap();
                        ColumnStatistics {
                            column_type: ColumnType::U8,
                            total_values: column.len(),
                            cardinality: column.values().iter().unique().count(),
                            missing_values: column.null_count(),
                            dictionary: false,
                        }
                    }
                    DataType::UInt32 => {
                        let column = array_data[i].as_any().downcast_ref::<UInt32Array>().unwrap();
                        ColumnStatistics {
                            column_type: ColumnType::U32,
                            total_values: column.len(),
                            cardinality: column.values().iter().unique().count(),
                            missing_values: column.null_count(),
                            dictionary: false,
                        }
                    }
                    DataType::UInt64 => {
                        let column = array_data[i].as_any().downcast_ref::<UInt64Array>().unwrap();
                        ColumnStatistics {
                            column_type: ColumnType::U64,
                            total_values: column.len(),
                            cardinality: column.values().iter().unique().count(),
                            missing_values: column.null_count(),
                            dictionary: false,
                        }
                    }
                    DataType::Float64 => {
                        let column = array_data[i].as_any().downcast_ref::<Float64Array>().unwrap();
                        ColumnStatistics {
                            column_type: ColumnType::F64,
                            total_values: column.len(),
                            cardinality: column.values().iter().map(|v| v.to_be_bytes()).unique().count(),
                            missing_values: column.null_count(),
                            dictionary: false,
                        }
                    }
                    DataType::Binary => {
                        let column = array_data[i].as_any().downcast_ref::<BinaryArray>().unwrap();
                        ColumnStatistics {
                            column_type: ColumnType::Binary,
                            total_values: column.len(),
                            cardinality: column.iter().filter_map(|value| value).unique().count(),
                            missing_values: column.null_count(),
                            dictionary: false,
                        }
                    }
                    DataType::Utf8 => {
                        let column = array_data[i].as_any().downcast_ref::<StringArray>().unwrap();
                        ColumnStatistics {
                            column_type: ColumnType::String,
                            total_values: column.len(),
                            cardinality: column.iter().filter_map(|value| value).unique().count(),
                            missing_values: column.null_count(),
                            dictionary: false,
                        }
                    }
                    DataType::Dictionary(index_type, _data_type) => {
                        match index_type.as_ref() {
                            DataType::UInt8 => {
                                let column = array_data[i].as_any().downcast_ref::<DictionaryArray<UInt8Type>>().unwrap();
                                ColumnStatistics {
                                    column_type: ColumnType::String,
                                    total_values: column.keys().len(),
                                    cardinality: column.keys().iter().unique().count(),
                                    missing_values: column.null_count(),
                                    dictionary: true,
                                }
                            }
                            DataType::UInt16 => {
                                let column = array_data[i].as_any().downcast_ref::<DictionaryArray<UInt16Type>>().unwrap();
                                ColumnStatistics {
                                    column_type: ColumnType::String,
                                    total_values: column.keys().len(),
                                    cardinality: column.keys().iter().unique().count(),
                                    missing_values: column.null_count(),
                                    dictionary: true,
                                }
                            }
                            DataType::UInt32 => {
                                let column = array_data[i].as_any().downcast_ref::<DictionaryArray<UInt32Type>>().unwrap();
                                ColumnStatistics {
                                    column_type: ColumnType::String,
                                    total_values: column.keys().len(),
                                    cardinality: column.keys().iter().unique().count(),
                                    missing_values: column.null_count(),
                                    dictionary: true,
                                }
                            }
                            _ => panic!("unsupported index type '{}'", index_type.as_ref())
                        }
                    }
                    _ => panic!("unsupported column type '{}'", field.data_type())
                };

                self.columns.insert(field.name().clone(), column_stats);
            });
        }
    }
}