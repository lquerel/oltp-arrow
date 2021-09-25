pub mod opentelemetry {
    pub mod proto {
        pub mod common {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/opentelemetry.proto.common.v1.rs"
                ));
            }
        }

        pub mod resource {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/opentelemetry.proto.resource.v1.rs"
                ));
            }
        }

        pub mod metrics {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/opentelemetry.proto.metrics.v1.rs"
                ));
            }
        }

        pub mod trace {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.trace.v1.rs"));
            }
        }

        pub mod events {
            pub mod v1 {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/opentelemetry.proto.events.v1.rs"
                ));
            }
        }
    }
}
