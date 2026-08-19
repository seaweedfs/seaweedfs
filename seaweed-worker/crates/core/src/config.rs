use std::time::Duration;

/// How one worker process connects and how much work it will take on.
#[derive(Clone, Debug)]
pub struct WorkerOptions {
    /// Admin gRPC address, e.g. "localhost:23646".
    pub admin_address: String,
    pub worker_id: String,
    pub worker_version: String,
    /// Advertised address; empty when the worker takes no inbound connections.
    pub worker_address: String,
    pub heartbeat_interval: Duration,
    pub reconnect_delay: Duration,
    pub max_detection_concurrency: i32,
    pub max_execution_concurrency: i32,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            admin_address: "localhost:23646".to_string(),
            worker_id: String::new(),
            worker_version: env!("CARGO_PKG_VERSION").to_string(),
            worker_address: String::new(),
            heartbeat_interval: Duration::from_secs(10),
            reconnect_delay: Duration::from_secs(5),
            max_detection_concurrency: 1,
            max_execution_concurrency: 1,
        }
    }
}
