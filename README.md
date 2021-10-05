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
3) Generate line delimited JSON  file containing spans with the format returned by the previous command
4) Run a benchmark on one or several json files 
```shell
> cargo run --release -p trace -- -b <batch_size:1000> file1.json file2.json ...
```

## Interpretation of benchmark results

The results are presented in a table with 4 columns:
* Column 1: List of metric labels is displayed per json file.
* Column 2: Metrics for the reference implementation (Protobuf OLTP v1).
* Column 3: Metrics for the Arrow implementation consuming a row-oriented data source.
* Column 4: Metrics for the Arrow implementation consuming a columnar-oriented data source.

Column 3 is close to a OLTP to OLTP-Arrow converter. 
Column 4 is close to a client SDK compatible with OLTP-Arrow.

## How to get better results 

Apache Arrow is an auto-descriptive columnar encoding format optimized for batch. Unlike protobuf, Arrow buffers contain 
a description of the schema for each column. Additionally, string columns can use a dictionary encoding to save space.

Consequently, to improve your results it is recommended to:
* Group spans in batch (> 1000 entries)
* When possible, group spans sharing the same attributes to minimize  

The option -s generates a set of json files containing statistics information on the processed batches. These files can
be helpful to optimize the definition of your batch.