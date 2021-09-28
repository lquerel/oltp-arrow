# OLTP Arrow protocol

This work-in-progress project has not been yet approved by the OpenTelemetry community.

## How to run a benchmark on spans
1) Install Rust
```shell
> curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# or check this [page](https://www.rust-lang.org/tools/install) if you are on Windows 
```
2) Clone this repo and go to the root of this project
3) Export a sample of the span data
```shell
> cargo run --release -p trace
```
3) Generate line delimited JSON  file containing spans with the format returned by the previous command
4) Run a benchmark on one or several json files 
```shell
> cargo run --release -p trace -- -b <batch_size:1000> file1.json file2.json ...

No argument file provided!

Please specify one or several line delimited JSON files containing span entities following the format below.

{"trace_id":"<id>","span_id":"<id>","trace_state":"<state>","parent_span_id":"<id>","name":"<name>","kind":0,"start_time_unix_nano":1626371667388918000,"end_time_unix_nano":1626371667388918010,"attributes":{"label_3":"<number>","label_2":"<bool>","label_1":"<text>"},"dropped_attributes_count":0,"events":[{"time_unix_nano":1626371667388918000,"name":"<event_name>","attributes":{"label_3":"<number>","label_2":"<bool>","label_1":"<text>"},"dropped_attributes_count":0}],"dropped_events_count":0,"links":[{"trace_id":"<id>","span_id":"<id>","trace_state":"<state>","attributes":{"label_3":"<number>","label_2":"<bool>","label_1":"<text>"},"dropped_attributes_count":0}],"dropped_links_count":0}
{"trace_id":"<id>","span_id":"<id>","trace_state":"<state>","parent_span_id":"<id>","name":"<name>","kind":0,"start_time_unix_nano":1626371667388918000,"end_time_unix_nano":1626371667388918010,"attributes":{"label_3":"<number>","label_2":"<bool>","label_1":"<text>"},"dropped_attributes_count":0,"events":[{"time_unix_nano":1626371667388918000,"name":"<event_name>","attributes":{"label_3":"<number>","label_2":"<bool>","label_1":"<text>"},"dropped_attributes_count":0}],"dropped_events_count":0,"links":[{"trace_id":"<id>","span_id":"<id>","trace_state":"<state>","attributes":{"label_3":"<number>","label_2":"<bool>","label_1":"<text>"},"dropped_attributes_count":0}],"dropped_links_count":0}
...

The following fields are optionals:
- trace_state
- parent_span_id
- kind
- end_time_unix_nano
- attributes
- dropped_attributes_count (any level)
- events
- dropped_events_count
- links
- dropped_links_count
```
