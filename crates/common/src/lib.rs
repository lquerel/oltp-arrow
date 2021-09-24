use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use serde_json::Value;

pub type Attributes = HashMap<String, Value>;

#[derive(Serialize, Deserialize, Debug)]
pub struct Span {
    pub trace_id: String,
    pub span_id: String,
    pub trace_state: Option<String>,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub kind: Option<i32>,
    pub start_time_unix_nano: u64,
    pub end_time_unix_nano: Option<u64>,
    pub attributes: Option<Attributes>,
    pub dropped_attributes_count: Option<u32>,
    pub events: Option<Vec<Event>>,
    pub dropped_events_count: Option<u32>,
    pub links: Option<Vec<Link>>,
    pub dropped_links_count: Option<u32>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Event {
    pub time_unix_nano: u64,
    pub name: String,
    pub attributes: Attributes,
    pub dropped_attributes_count: Option<u32>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Link {
    pub trace_id: String,
    pub span_id: String,
    pub trace_state: Option<String>,
    pub attributes: Attributes,
    pub dropped_attributes_count: Option<u32>,
}