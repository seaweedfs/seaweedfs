use std::sync::Arc;

use bytes::Bytes;
use lance::io::WrappingObjectStore;
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use weed_vector_server::{metrics::Metrics, read_cache::ReadCache};

#[tokio::test]
async fn immutable_reads_report_hits_misses_bytes_and_weight() {
    let metrics = Metrics::new().unwrap();
    let cache = ReadCache::new(1024).with_metrics(metrics.clone());
    let store = cache.wrap("memory", Arc::new(InMemory::new()));
    let path = Path::from("data/example.lance");
    store
        .put(&path, Bytes::from_static(b"abc").into())
        .await
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap(),
            "abc"
        );
    }
    assert_eq!(metrics.cache_requests.with_label_values(&["miss"]).get(), 1);
    assert_eq!(metrics.cache_requests.with_label_values(&["hit"]).get(), 1);
    assert_eq!(metrics.storage_bytes.get(), 3);
    assert_eq!(metrics.storage_read_seconds.get_sample_count(), 1);
    let (entries, weight) = cache.size();
    assert_eq!(entries, 1);
    assert!(weight >= 3);
    let metrics_endpoint = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/metrics", metrics_endpoint.local_addr().unwrap());
    let task = tokio::spawn(weed_vector_server::metrics::serve_listener(
        metrics,
        metrics_endpoint,
        Some(cache),
    ));
    let body = reqwest::get(endpoint).await.unwrap().text().await.unwrap();
    assert!(body.contains("SeaweedFS_vector_cache_entries 1"));
    assert!(body.contains("SeaweedFS_vector_cache_bytes "));
    task.abort();
}
