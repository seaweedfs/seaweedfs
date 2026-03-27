use axum::body::Body;
use axum::extract::Query;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::Router;
use pprof::protos::Message;
use serde::Deserialize;

#[derive(Deserialize, Default)]
struct ProfileQuery {
    seconds: Option<u64>,
}

pub fn build_debug_router() -> Router {
    Router::new()
        .route("/debug/pprof/", get(pprof_index_handler))
        .route("/debug/pprof/cmdline", get(pprof_cmdline_handler))
        .route("/debug/pprof/profile", get(pprof_profile_handler))
        .route("/debug/pprof/symbol", any(pprof_symbol_handler))
        .route("/debug/pprof/trace", get(pprof_trace_handler))
}

async fn pprof_index_handler() -> Response {
    let body = concat!(
        "<html><head><title>/debug/pprof/</title></head><body>",
        "<a href=\"cmdline\">cmdline</a><br>",
        "<a href=\"profile\">profile</a><br>",
        "<a href=\"symbol\">symbol</a><br>",
        "<a href=\"trace\">trace</a><br>",
        "</body></html>",
    );
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        body,
    )
        .into_response()
}

async fn pprof_cmdline_handler() -> Response {
    let body = std::env::args().collect::<Vec<_>>().join("\0");
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        body,
    )
        .into_response()
}

async fn pprof_profile_handler(Query(query): Query<ProfileQuery>) -> Response {
    let seconds = query.seconds.unwrap_or(30).clamp(1, 300);
    let guard = match pprof::ProfilerGuard::new(100) {
        Ok(guard) => guard,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to start profiler: {}", e),
            )
                .into_response();
        }
    };

    tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;

    let report = match guard.report().build() {
        Ok(report) => report,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to build profile report: {}", e),
            )
                .into_response();
        }
    };

    let profile = match report.pprof() {
        Ok(profile) => profile,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to encode profile: {}", e),
            )
                .into_response();
        }
    };

    let mut bytes = Vec::new();
    if let Err(e) = profile.encode(&mut bytes) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to serialize profile: {}", e),
        )
            .into_response();
    }

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/octet-stream")],
        bytes,
    )
        .into_response()
}

async fn pprof_symbol_handler() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        "num_symbols: 0\n",
    )
        .into_response()
}

async fn pprof_trace_handler(Query(query): Query<ProfileQuery>) -> Response {
    let seconds = query.seconds.unwrap_or(1).clamp(1, 30);
    tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(Vec::<u8>::new()))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_debug_index_route() {
        let app = build_debug_router();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/debug/pprof/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_debug_cmdline_route() {
        let app = build_debug_router();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/debug/pprof/cmdline")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
