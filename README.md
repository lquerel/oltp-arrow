# OLTP Arrow protocol

This work-in-progress project has not been yet approved by the OpenTelemetry community.

## How to run a benchmark on spans
1) Install Rust
```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# or check this [page](https://www.rust-lang.org/tools/install) if you are on Windows 
```
2) Clone this repo and go to the root of this project
3) Export a sample of the span data
```shell
cargo run --release -p trace
```
3) Generate line delimited JSON  file containing spans with the format returned by the previous command
4) Run a benchmark on one or several json files 
```shell
cargo run --release -p trace -- -b <batch_size:1000> file1.json file2.json ...
```
