use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use hyper::http;
use rustls::pki_types::CertificateDer;
use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo};
use tower::{Layer, Service};

use super::tls::parse_common_name;

const GRPC_HEALTH_SERVICE_PREFIX: &str = "/grpc.health.v1.Health/";

/// Confines clients whose certificate common name is in
/// grpc.health_check_commonNames to the gRPC health service, like the Go
/// servers' healthCheckOnlyTap. A name on allowed_commonNames too stays
/// confined.
#[derive(Clone, Debug, Default)]
pub struct HealthCheckOnlyLayer {
    names: Arc<HashSet<String>>,
}

impl HealthCheckOnlyLayer {
    pub fn new(names: &[String]) -> Self {
        Self {
            names: Arc::new(names.iter().cloned().collect()),
        }
    }
}

impl<S> Layer<S> for HealthCheckOnlyLayer {
    type Service = HealthCheckOnlyService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        HealthCheckOnlyService {
            inner,
            names: self.names.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct HealthCheckOnlyService<S> {
    inner: S,
    names: Arc<HashSet<String>>,
}

impl<S, B> Service<http::Request<B>> for HealthCheckOnlyService<S>
where
    S: Service<http::Request<B>, Response = http::Response<tonic::body::Body>>,
    S::Future: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let peer_certs = request
            .extensions()
            .get::<TlsConnectInfo<TcpConnectInfo>>()
            .and_then(|info| info.peer_certs());
        if let Some(common_name) = health_check_confined(
            &self.names,
            request.uri().path(),
            peer_certs.as_deref().map(Vec::as_slice),
        ) {
            let status = tonic::Status::permission_denied(format!(
                "certificate {:?} may only call the gRPC health service",
                common_name
            ));
            return Box::pin(async move { Ok(status.into_http()) });
        }
        Box::pin(self.inner.call(request))
    }
}

fn health_check_confined(
    names: &HashSet<String>,
    path: &str,
    peer_certs: Option<&[CertificateDer<'static>]>,
) -> Option<String> {
    if names.is_empty() || path.starts_with(GRPC_HEALTH_SERVICE_PREFIX) {
        return None;
    }
    let common_name = parse_common_name(peer_certs?.first()?).ok()?;
    names.contains(&common_name).then_some(common_name)
}
