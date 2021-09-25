use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    prost_build::compile_protos(
        &[
            "proto/opentelemetry/proto/metrics/v1/metrics.proto",
            "proto/opentelemetry/proto/trace/v1/trace.proto",
            "proto/opentelemetry/proto/events/v1/events.proto",
        ],
        &["proto/"],
    )?;
    Ok(())
}
