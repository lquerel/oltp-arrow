use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, DictionaryArray, Float64Array, Int32Array, Int64Array, Int8Array, StringArray, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{DataType, Schema, UInt16Type, UInt32Type, UInt8Type};
use itertools::Itertools;
use serde::{Serialize, Deserialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use bitvec::vec::BitVec;
use bitvec::order::Msb0;
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::fs;

#[derive(Debug, Serialize, Deserialize)]
pub struct StatisticsReporter {
    pub file: String,
    pub batches: Vec<BatchStatistics>,
    pub stats_enabled: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnStatistics {
    // Column type
    pub column_type: ColumnType,
    // Total number of values (including missing values)
    pub total_values: usize,
    // Number of unique values
    pub cardinality: usize,
    // Number of missing values
    pub missing_values: usize,
    pub dictionary: bool,
    pub validity_map: BitVec<Msb0, u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchStatistics {
    pub stats_enabled: bool,
    pub span_columns: ColumnsStatistics,
    pub event_columns: ColumnsStatistics,
    pub link_columns: ColumnsStatistics,
}

#[derive(Debug, Serialize, Deserialize)]
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
        Self {
            file: file.into(),
            batches: vec![],
            stats_enabled: true,
        }
    }

    pub fn noop() -> Self {
        Self {
            file: "".into(),
            batches: vec![],
            stats_enabled: false,
        }
    }

    pub fn next_batch(&mut self) -> &mut BatchStatistics {
        self.batches.push(BatchStatistics {
            stats_enabled: self.stats_enabled,
            span_columns: ColumnsStatistics::new(self.stats_enabled),
            event_columns: ColumnsStatistics::new(self.stats_enabled),
            link_columns: ColumnsStatistics::new(self.stats_enabled),
        });
        self.batches.last_mut().unwrap()
    }
}

#[derive(Debug, Serialize, Deserialize)]
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
        Self {
            stats_enabled,
            columns: Default::default(),
        }
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
                        let mut validity_map = BitVec::<Msb0, u8>::new();
                        column.values().iter().enumerate().for_each(|(i, _)| if column.is_valid(i) { validity_map.push(true); } else { validity_map.push(false); });
                        ColumnStatistics {
                            column_type: ColumnType::Boolean,
                            total_values: column.len(),
                            cardinality: column
                                .values()
                                .iter()
                                .enumerate()
                                .filter_map(|(i, v)| if column.is_valid(i) { Some(v) } else { None })
                                .unique()
                                .count(),
                            missing_values: column.null_count(),
                            dictionary: false,
                            validity_map,
                        }
                    }
                    DataType::Int8 => {
                        let column = array_data[i].as_any().downcast_ref::<Int8Array>().unwrap();
                        let mut validity_map = BitVec::<Msb0, u8>::new();
                        column.values().iter().enumerate().for_each(|(i, _)| if column.is_valid(i) { validity_map.push(true); } else { validity_map.push(false); });
                        ColumnStatistics {
                            column_type: ColumnType::I8,
                            total_values: column.len(),
                            cardinality: column
                                .values()
                                .iter()
                                .enumerate()
                                .filter_map(|(i, v)| if column.is_valid(i) { Some(v) } else { None })
                                .unique()
                                .count(),
                            missing_values: column.null_count(),
                            dictionary: false,
                            validity_map
                        }
                    }
                    DataType::Int32 => {
                        let column = array_data[i].as_any().downcast_ref::<Int32Array>().unwrap();
                        let mut validity_map = BitVec::<Msb0, u8>::new();
                        column.values().iter().enumerate().for_each(|(i, _)| if column.is_valid(i) { validity_map.push(true); } else { validity_map.push(false); });
                        ColumnStatistics {
                            column_type: ColumnType::I32,
                            total_values: column.len(),
                            cardinality: column
                                .values()
                                .iter()
                                .enumerate()
                                .filter_map(|(i, v)| if column.is_valid(i) { Some(v) } else { None })
                                .unique()
                                .count(),
                            missing_values: column.null_count(),
                            dictionary: false,
                            validity_map,
                        }
                    }
                    DataType::Int64 => {
                        let column = array_data[i].as_any().downcast_ref::<Int64Array>().unwrap();
                        let mut validity_map = BitVec::<Msb0, u8>::new();
                        column.values().iter().enumerate().for_each(|(i, _)| if column.is_valid(i) { validity_map.push(true); } else { validity_map.push(false); });
                        ColumnStatistics {
                            column_type: ColumnType::I64,
                            total_values: column.len(),
                            cardinality: column
                                .values()
                                .iter()
                                .enumerate()
                                .filter_map(|(i, v)| if column.is_valid(i) { Some(v) } else { None })
                                .unique()
                                .count(),
                            missing_values: column.null_count(),
                            dictionary: false,
                            validity_map
                        }
                    }
                    DataType::UInt8 => {
                        let column = array_data[i].as_any().downcast_ref::<UInt8Array>().unwrap();
                        let mut validity_map = BitVec::<Msb0, u8>::new();
                        column.values().iter().enumerate().for_each(|(i, _)| if column.is_valid(i) { validity_map.push(true); } else { validity_map.push(false); });
                        ColumnStatistics {
                            column_type: ColumnType::U8,
                            total_values: column.len(),
                            cardinality: column
                                .values()
                                .iter()
                                .enumerate()
                                .filter_map(|(i, v)| if column.is_valid(i) { Some(v) } else { None })
                                .unique()
                                .count(),
                            missing_values: column.null_count(),
                            dictionary: false,
                            validity_map
                        }
                    }
                    DataType::UInt32 => {
                        let column = array_data[i].as_any().downcast_ref::<UInt32Array>().unwrap();
                        let mut validity_map = BitVec::<Msb0, u8>::new();
                        column.values().iter().enumerate().for_each(|(i, _)| if column.is_valid(i) { validity_map.push(true); } else { validity_map.push(false); });
                        ColumnStatistics {
                            column_type: ColumnType::U32,
                            total_values: column.len(),
                            cardinality: column
                                .values()
                                .iter()
                                .enumerate()
                                .filter_map(|(i, v)| if column.is_valid(i) { Some(v) } else { None })
                                .unique()
                                .count(),
                            missing_values: column.null_count(),
                            dictionary: false,
                            validity_map
                        }
                    }
                    DataType::UInt64 => {
                        let column = array_data[i].as_any().downcast_ref::<UInt64Array>().unwrap();
                        let mut validity_map = BitVec::<Msb0, u8>::new();
                        column.values().iter().enumerate().for_each(|(i, _)| if column.is_valid(i) { validity_map.push(true); } else { validity_map.push(false); });
                        ColumnStatistics {
                            column_type: ColumnType::U64,
                            total_values: column.len(),
                            cardinality: column
                                .values()
                                .iter()
                                .enumerate()
                                .filter_map(|(i, v)| if column.is_valid(i) { Some(v) } else { None })
                                .unique()
                                .count(),
                            missing_values: column.null_count(),
                            dictionary: false,
                            validity_map
                        }
                    }
                    DataType::Float64 => {
                        let column = array_data[i].as_any().downcast_ref::<Float64Array>().unwrap();
                        let mut validity_map = BitVec::<Msb0, u8>::new();
                        column.values().iter().enumerate().for_each(|(i, _)| if column.is_valid(i) { validity_map.push(true); } else { validity_map.push(false); });
                        ColumnStatistics {
                            column_type: ColumnType::F64,
                            total_values: column.len(),
                            cardinality: column
                                .values()
                                .iter()
                                .enumerate()
                                .filter_map(|(i, v)| if column.is_valid(i) { Some(v.to_be_bytes()) } else { None })
                                .unique()
                                .count(),
                            missing_values: column.null_count(),
                            dictionary: false,
                            validity_map
                        }
                    }
                    DataType::Binary => {
                        let column = array_data[i].as_any().downcast_ref::<BinaryArray>().unwrap();
                        let mut validity_map = BitVec::<Msb0, u8>::new();
                        column.iter().enumerate().for_each(|(i, _)| if column.is_valid(i) { validity_map.push(true); } else { validity_map.push(false); });
                        ColumnStatistics {
                            column_type: ColumnType::Binary,
                            total_values: column.len(),
                            cardinality: column.iter().filter_map(|value| value).unique().count(),
                            missing_values: column.null_count(),
                            dictionary: false,
                            validity_map
                        }
                    }
                    DataType::Utf8 => {
                        let column = array_data[i].as_any().downcast_ref::<StringArray>().unwrap();
                        let mut validity_map = BitVec::<Msb0, u8>::new();
                        column.iter().enumerate().for_each(|(i, _)| if column.is_valid(i) { validity_map.push(true); } else { validity_map.push(false); });
                        ColumnStatistics {
                            column_type: ColumnType::String,
                            total_values: column.len(),
                            cardinality: column.iter().filter_map(|value| value).unique().count(),
                            missing_values: column.null_count(),
                            dictionary: false,
                            validity_map
                        }
                    }
                    DataType::Dictionary(index_type, _data_type) => match index_type.as_ref() {
                        DataType::UInt8 => {
                            let column = array_data[i].as_any().downcast_ref::<DictionaryArray<UInt8Type>>().unwrap();
                            let mut validity_map = BitVec::<Msb0, u8>::new();
                            column.keys().iter().for_each(|v| if v.is_some() { validity_map.push(true); } else { validity_map.push(false); });
                            ColumnStatistics {
                                column_type: ColumnType::String,
                                total_values: column.keys().len(),
                                cardinality: column.keys().iter().filter_map(|v| v).unique().count(),
                                missing_values: column.null_count(),
                                dictionary: true,
                                validity_map
                            }
                        }
                        DataType::UInt16 => {
                            let column = array_data[i].as_any().downcast_ref::<DictionaryArray<UInt16Type>>().unwrap();
                            let mut validity_map = BitVec::<Msb0, u8>::new();
                            column.keys().iter().for_each(|v| if v.is_some() { validity_map.push(true); } else { validity_map.push(false); });
                            ColumnStatistics {
                                column_type: ColumnType::String,
                                total_values: column.keys().len(),
                                cardinality: column.keys().iter().filter_map(|v| v).unique().count(),
                                missing_values: column.null_count(),
                                dictionary: true,
                                validity_map
                            }
                        }
                        DataType::UInt32 => {
                            let column = array_data[i].as_any().downcast_ref::<DictionaryArray<UInt32Type>>().unwrap();
                            let mut validity_map = BitVec::<Msb0, u8>::new();
                            column.keys().iter().for_each(|v| if v.is_some() { validity_map.push(true); } else { validity_map.push(false); });
                            ColumnStatistics {
                                column_type: ColumnType::String,
                                total_values: column.keys().len(),
                                cardinality: column.keys().iter().filter_map(|v| v).unique().count(),
                                missing_values: column.null_count(),
                                dictionary: true,
                                validity_map
                            }
                        }
                        _ => panic!("unsupported index type '{}'", index_type.as_ref()),
                    },
                    _ => panic!("unsupported column type '{}'", field.data_type()),
                };

                self.columns.insert(field.name().clone(), column_stats);
            });
        }
    }
}

// pub fn generate_row_validity_map(stats_file: &str) -> Result<(), Box<dyn Error>> {
//     let json = fs::read_to_string(stats_file)?;
//     let stats: StatisticsReporter = serde_json::from_str(&json)?;
//     let batch = stats.batches.first().unwrap();
//     let span_columns = &batch.span_columns.columns;
//
//     print!("[");
//     for i in 0..200 {
//         let mut row = String::new();
//         for (_, column) in span_columns {
//             if !row.is_empty() {
//                 row.push(',');
//             }
//             if column.validity_map[i] {
//                 row.push('1');
//             } else {
//                 row.push('0');
//             }
//         }
//         println!("[{}],", row);
//     }
//     print!("]");
//     Ok(())
// }
//
// pub fn generate_row_validity_map2(stats_file: &str) -> Result<(), Box<dyn Error>> {
//     let json = fs::read_to_string(stats_file)?;
//     let stats: StatisticsReporter = serde_json::from_str(&json)?;
//     let batch = stats.batches.first().unwrap();
//     let span_columns = &batch.span_columns.columns;
//
//     let mut rows = vec![];
//     for i in 0..200 {
//         let mut row = vec![];
//         for (name, column) in span_columns {
//             if column.validity_map[i] {
//                 row.push(name.clone());
//             } else {
//                 row.push("".into());
//             }
//         }
//         rows.push(row);
//     }
//
//     rows.sort();
//
//     let mut col_names = span_columns.iter().map(|(name, _)| name.len()).collect::<Vec<_>>();
//     for row in rows.iter() {
//         for (i, col) in row.iter().enumerate() {
//             print!("{:width$}, ", col, width = col_names[i]);
//         }
//         println!();
//     }
//
//     Ok(())
// }

// #[cfg(test)]
// mod test {
//     use crate::arrow::statistics::{generate_row_validity_map, generate_row_validity_map2};
//     use bitvec::vec::BitVec;
//     use bitvec::order::Msb0;
//     use serde::{Serialize, Deserialize};
//
//     #[test]
//     fn test() {
//         generate_row_validity_map2("/Users/querel/rust/oltp-arrow/trace_samples_1.json.arrow_col_oriented_stats.json").unwrap();
//     }
//
//     #[test]
//     fn test2() {
//         #[derive(Debug, Serialize, Deserialize)]
//         struct Test {
//             field: i32,
//             validity_map: BitVec<Msb0, u8>,
//         }
//
//         let mut test = Test { field: 1, validity_map: Default::default() };
//         test.validity_map.push(true);
//         test.validity_map.push(false);
//
//         let json = serde_json::to_string(&test).unwrap();
//         println!("{}", json);
//         let test: Test = serde_json::from_str(&json).unwrap();
//         dbg!(&test);
//     }
// }