use std::net::SocketAddr;

use anyhow::Result;
use axum::{Router, http::StatusCode, routing::get};
use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};
use tonic_health::{ServingStatus, server::HealthReporter};

use crate::read_cache::ReadCache;

#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,
    pub connected: IntGauge,
    pub requests: IntCounterVec,
    pub request_seconds: HistogramVec,
    pub in_flight: IntGaugeVec,
    pub query_results: IntCounter,
    pub cache_requests: IntCounterVec,
    pub storage_bytes: IntCounter,
    pub storage_read_seconds: Histogram,
    cache_bytes: IntGauge,
    cache_entries: IntGauge,
    health: Option<HealthReporter>,
}

impl Metrics {
    pub fn new() -> Result<Self> {
        let registry = Registry::new();
        let opts = |name: &str, help: &str| {
            Opts::new(name, help)
                .namespace("SeaweedFS")
                .subsystem("vector")
        };
        let buckets = prometheus::exponential_buckets(0.0001, 2.0, 24)?;
        let histogram =
            |name: &str, help: &str| HistogramOpts::from(opts(name, help)).buckets(buckets.clone());
        let connected = IntGauge::with_opts(opts(
            "connected",
            "Whether the engine is registered with the master.",
        ))?;
        let requests = IntCounterVec::new(
            opts(
                "request_total",
                "Vector engine requests by operation and gRPC status.",
            ),
            &["type", "code"],
        )?;
        let request_seconds = HistogramVec::new(
            histogram("request_seconds", "Vector engine request duration."),
            &["type"],
        )?;
        let in_flight = IntGaugeVec::new(
            opts("in_flight_requests", "Vector engine requests in progress."),
            &["type"],
        )?;
        let query_results =
            IntCounter::with_opts(opts("query_results_total", "Vectors returned by queries."))?;
        let cache_requests = IntCounterVec::new(
            opts("cache_requests_total", "Immutable reads by cache outcome."),
            &["result"],
        )?;
        let storage_bytes = IntCounter::with_opts(opts(
            "storage_read_bytes_total",
            "Bytes fetched for immutable cache reads.",
        ))?;
        let storage_read_seconds = Histogram::with_opts(histogram(
            "storage_read_seconds",
            "Immutable storage fetch duration.",
        ))?;
        let cache_bytes =
            IntGauge::with_opts(opts("cache_bytes", "Current cache weight in bytes."))?;
        let cache_entries = IntGauge::with_opts(opts("cache_entries", "Current cache entries."))?;
        for collector in [
            Box::new(connected.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(requests.clone()),
            Box::new(request_seconds.clone()),
            Box::new(in_flight.clone()),
            Box::new(query_results.clone()),
            Box::new(cache_requests.clone()),
            Box::new(storage_bytes.clone()),
            Box::new(storage_read_seconds.clone()),
            Box::new(cache_bytes.clone()),
            Box::new(cache_entries.clone()),
        ] {
            registry.register(collector)?;
        }
        Ok(Self {
            registry,
            connected,
            requests,
            request_seconds,
            in_flight,
            query_results,
            cache_requests,
            storage_bytes,
            storage_read_seconds,
            cache_bytes,
            cache_entries,
            health: None,
        })
    }

    pub fn with_health(mut self, health: HealthReporter) -> Self {
        self.health = Some(health);
        self
    }

    pub async fn set_connected(&self, connected: bool) {
        self.connected.set(i64::from(connected));
        if let Some(health) = self.health.clone() {
            let status = if connected {
                ServingStatus::Serving
            } else {
                ServingStatus::NotServing
            };
            health.set_service_status("", status).await;
            health
                .set_service_status("vector_pb.VectorService", status)
                .await;
        }
    }

    pub fn start(&self, operation: &str) -> InFlight {
        let gauge = self.in_flight.with_label_values(&[operation]);
        gauge.inc();
        InFlight {
            gauge,
            requests: self.requests.clone(),
            operation: operation.into(),
            completed: false,
            _timer: self
                .request_seconds
                .with_label_values(&[operation])
                .start_timer(),
        }
    }

    fn gather(&self, cache: Option<&ReadCache>) -> Result<String> {
        if let Some(cache) = cache {
            let (entries, bytes) = cache.size();
            self.cache_entries.set(entries as i64);
            self.cache_bytes.set(bytes as i64);
        }
        let mut buffer = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut buffer)?;
        Ok(String::from_utf8(buffer)?)
    }
}

pub struct InFlight {
    gauge: IntGauge,
    requests: IntCounterVec,
    operation: String,
    completed: bool,
    _timer: prometheus::HistogramTimer,
}

impl InFlight {
    pub fn complete(&mut self, error: Option<tonic::Code>) {
        let code = error
            .map(|c| format!("{c:?}"))
            .unwrap_or_else(|| "OK".into());
        self.requests
            .with_label_values(&[&self.operation, &code])
            .inc();
        self.completed = true;
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.gauge.dec();
        if !self.completed {
            self.requests
                .with_label_values(&[&self.operation, "Cancelled"])
                .inc();
        }
    }
}

pub async fn serve(metrics: Metrics, address: SocketAddr, cache: Option<ReadCache>) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(address).await?;
    serve_listener(metrics, listener, cache).await
}

pub async fn serve_listener(
    metrics: Metrics,
    listener: tokio::net::TcpListener,
    cache: Option<ReadCache>,
) -> Result<()> {
    let readiness = metrics.clone();
    let app = Router::new()
        .route("/health", get(|| async { StatusCode::OK }))
        .route(
            "/ready",
            get(move || {
                let metrics = readiness.clone();
                async move {
                    if metrics.connected.get() == 1 {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    }
                }
            }),
        )
        .route(
            "/metrics",
            get(move || {
                let metrics = metrics.clone();
                let cache = cache.clone();
                async move {
                    metrics
                        .gather(cache.as_ref())
                        .map(|body| ([("content-type", "text/plain; version=0.0.4")], body))
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
                }
            }),
        );
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn endpoints_follow_registration_and_publish_metrics() {
        let metrics = Metrics::new().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(serve_listener(metrics.clone(), listener, None));
        let client = reqwest::Client::new();
        assert_eq!(
            client
                .get(format!("{endpoint}/health"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            client
                .get(format!("{endpoint}/ready"))
                .send()
                .await
                .unwrap()
                .status(),
            503
        );
        metrics.set_connected(true).await;
        assert_eq!(
            client
                .get(format!("{endpoint}/ready"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        let in_flight = metrics.start("QueryVectors");
        let body = client
            .get(format!("{endpoint}/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(body.contains("SeaweedFS_vector_connected 1"));
        assert!(body.contains("SeaweedFS_vector_in_flight_requests{type=\"QueryVectors\"} 1"));
        drop(in_flight);
        assert_eq!(
            metrics.in_flight.with_label_values(&["QueryVectors"]).get(),
            0
        );
        metrics.set_connected(false).await;
        assert_eq!(
            client
                .get(format!("{endpoint}/ready"))
                .send()
                .await
                .unwrap()
                .status(),
            503
        );
        task.abort();
    }

    #[tokio::test]
    async fn grpc_health_follows_registration() {
        use tonic_health::pb::{HealthCheckRequest, health_client::HealthClient};
        let (reporter, service) = tonic_health::server::health_reporter();
        let metrics = Metrics::new().unwrap().with_health(reporter);
        metrics.set_connected(false).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let channel = tonic::transport::Endpoint::from_shared(endpoint)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = HealthClient::new(channel);
        let request = || HealthCheckRequest {
            service: "vector_pb.VectorService".into(),
        };
        assert_eq!(
            client.check(request()).await.unwrap().into_inner().status,
            2
        );
        metrics.set_connected(true).await;
        assert_eq!(
            client.check(request()).await.unwrap().into_inner().status,
            1
        );
        metrics.set_connected(false).await;
        assert_eq!(
            client.check(request()).await.unwrap().into_inner().status,
            2
        );
        task.abort();
    }
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics")
            .field("connected", &self.connected.get())
            .finish()
    }
}
