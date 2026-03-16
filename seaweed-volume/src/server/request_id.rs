use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use hyper::http::{self, HeaderValue};
use tonic::metadata::MetadataValue;
use tonic::{Request, Status};
use tower::{Layer, Service};

tokio::task_local! {
    static CURRENT_REQUEST_ID: String;
}

#[derive(Clone, Debug, Default)]
pub struct GrpcRequestIdLayer;

#[derive(Clone, Debug)]
pub struct GrpcRequestIdService<S> {
    inner: S,
}

impl<S> Layer<S> for GrpcRequestIdLayer {
    type Service = GrpcRequestIdService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcRequestIdService { inner }
    }
}

impl<S, B> Service<http::Request<B>> for GrpcRequestIdService<S>
where
    S: Service<http::Request<B>, Response = http::Response<tonic::body::BoxBody>> + Send + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<B>) -> Self::Future {
        let request_id = match request.headers().get("x-amz-request-id") {
            Some(value) => match value.to_str() {
                Ok(value) if !value.is_empty() => value.to_owned(),
                _ => generate_grpc_request_id(),
            },
            None => generate_grpc_request_id(),
        };

        if let Ok(value) = HeaderValue::from_str(&request_id) {
            request.headers_mut().insert("x-amz-request-id", value);
        }

        let future = self.inner.call(request);

        Box::pin(async move {
            let mut response: http::Response<tonic::body::BoxBody> =
                scope_request_id(request_id.clone(), future).await?;
            if let Ok(value) = HeaderValue::from_str(&request_id) {
                response.headers_mut().insert("x-amz-request-id", value);
            }
            Ok(response)
        })
    }
}

pub async fn scope_request_id<F, T>(request_id: String, future: F) -> T
where
    F: Future<Output = T>,
{
    CURRENT_REQUEST_ID.scope(request_id, future).await
}

pub fn current_request_id() -> Option<String> {
    CURRENT_REQUEST_ID.try_with(Clone::clone).ok()
}

pub fn outgoing_request_id_interceptor(
    mut request: Request<()>,
) -> Result<Request<()>, Status> {
    if let Some(request_id) = current_request_id() {
        let value = MetadataValue::try_from(request_id.as_str())
            .map_err(|_| Status::internal("invalid scoped request id"))?;
        request.metadata_mut().insert("x-amz-request-id", value);
    }
    Ok(request)
}

pub fn generate_http_request_id() -> String {
    use rand::Rng;

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let rand_val: u32 = rand::thread_rng().gen();
    format!("{:X}{:08X}", nanos, rand_val)
}

fn generate_grpc_request_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[cfg(test)]
mod tests {
    use super::{current_request_id, outgoing_request_id_interceptor, scope_request_id};
    use tonic::Request;

    #[tokio::test]
    async fn test_scope_request_id_exposes_current_value() {
        let request_id = "req-123".to_string();
        let current = scope_request_id(request_id.clone(), async move {
            current_request_id().unwrap()
        })
        .await;
        assert_eq!(current, request_id);
    }

    #[tokio::test]
    async fn test_outgoing_request_id_interceptor_propagates_scope() {
        let request = scope_request_id("req-456".to_string(), async move {
            outgoing_request_id_interceptor(Request::new(())).unwrap()
        })
        .await;
        assert_eq!(
            request
                .metadata()
                .get("x-amz-request-id")
                .unwrap()
                .to_str()
                .unwrap(),
            "req-456"
        );
    }
}
