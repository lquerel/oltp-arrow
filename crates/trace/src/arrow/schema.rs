use std::collections::HashSet;
use serde::Serialize;

#[derive(PartialEq, Debug)]
pub struct FieldInfo {
    pub non_null_count: usize,
    pub field_type: FieldType,
    pub dictionary_values: HashSet<String>,
}

#[derive(PartialEq, Debug, Serialize)]
pub enum FieldType {
    U64,
    I64,
    F64,
    String,
    Bool,
}

impl FieldInfo {
    pub fn is_dictionary(&self) -> bool {
        (self.dictionary_values.len() as f64 / self.non_null_count as f64) < 0.2
    }
}
