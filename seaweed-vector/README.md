# SeaweedFS vector engine

`weed-vector` serves the S3 Vectors data plane through the S3 gateway. Indexes use Lance tables on the cluster's filers and volume servers. Go and Rust volume servers use the same engine and API.

With a local `weed mini` cluster running:

```sh
weed-vector --masters=localhost:9333 --metrics-port=9329
```

Point an AWS S3 Vectors client at the S3 endpoint, normally `http://localhost:8333`. The engine registers with the masters automatically. `--listen` defaults to `127.0.0.1:8090`; use an address reachable from the gateway when running on another host or in a container.

| Setting | Default | Purpose |
| --- | --- | --- |
| `--masters` | Required | Comma-separated master addresses |
| `--listen` | `127.0.0.1:8090` | gRPC listener and registered address |
| `--filer-group` | Empty | Filer group used for table storage |
| `--metrics-port` | `0` | HTTP metrics and health listener; zero disables it |
| `--metrics-ip` | `127.0.0.1` | HTTP listener address |
| `--security-toml` | Discovered | Cluster certificates, signing keys and encryption keys |

The engine caches a quarter of the memory it may use (its cgroup limit, else the machine's), split 2:4:1 between file reads, loaded indexes and table metadata, and logs the sizes at startup. Each query probes a tenth of the index's partitions, at least 8, and more under a filter.

The HTTP listener provides `/health`, `/ready`, and `/metrics`. Readiness requires an active registration with the master. The gRPC listener also implements the standard health service for `vector_pb.VectorService` and the whole server. These health statuses follow master registration.

Metrics use the `SeaweedFS_vector_` prefix:

- `connected`: active master registration.
- `request_total{type,code}`, `request_seconds{type}`, and `in_flight_requests{type}`: engine requests and their gRPC outcomes.
- `query_results_total`: vectors returned by queries.
- `cache_requests_total{result}`, `cache_bytes`, and `cache_entries`: immutable read cache activity and occupancy.
- `storage_read_bytes_total` and `storage_read_seconds`: storage fetches for cacheable immutable reads.

The S3 gateway separately publishes `SeaweedFS_s3_` request metrics with `type="s3vectors.<Operation>"`. Scrape both processes to distinguish gateway latency from engine work. Metrics and HTTP probes are unauthenticated; bind them to an internal address or restrict their network access.

For mutual TLS, configure `[grpc.vector]` for the engine's server certificate, `[grpc.client]` for its master/filer connections, and the cluster CA. HTTPS volume reads also use `[https.client]`. The engine follows the same signing and encryption configuration as the other cluster clients.

Run the Lance maintenance worker against the cluster's admin to build and update ANN indexes. Queries can scan data that has not been indexed yet. See the [maintenance worker documentation](../seaweed-worker/README.md) and [Helm chart](../k8s/charts/seaweedfs/README.md) for deployment settings.

The integration suite at `test/s3vectors` runs the same API and observability checks against both Go and Rust volume servers, including mutual TLS, signed volume reads, and encrypted volume data.

## Search indexed data only

The default search includes recent writes by scanning data that indexes do not
cover yet. To exclude that data, send `x-seaweedfs-vector-search-mode: indexed-only`
on `QueryVectors`. Omit the header, or set it to `all`, for the default behavior.
This is a SeaweedFS extension. With SigV4, the header must be signed.

For example, inject it before boto3 signs the request:

```python
def indexed_only(request, **kwargs):
    request.headers["x-seaweedfs-vector-search-mode"] = "indexed-only"

client.meta.events.register("before-sign.s3vectors.QueryVectors", indexed_only)
try:
    result = client.query_vectors(
        vectorBucketName="embeddings", indexName="articles",
        queryVector={"float32": embedding}, topK=10,
    )
finally:
    client.meta.events.unregister("before-sign.s3vectors.QueryVectors", indexed_only)
```

Indexed-only searches use the intersection of vector index coverage and the
coverage of each metadata index the filter requires, at one table version. Filters
must be conjunctions of string equality or `$in` predicates on ready indexed
metadata keys. An unavailable index or unsupported filter returns
`ValidationException`. A valid index with no eligible data returns an empty list.
A vector engine predating this extension returns `ServiceUnavailableException`;
the gateway never falls back to a full search.

The response includes `x-seaweedfs-vector-search-mode: indexed-only`,
`x-seaweedfs-vector-data-excluded: true|false`, and
`x-seaweedfs-vector-excluded-fragments`. The exclusion count describes coverage,
not the number of matching vectors. New writes, updates, and rewritten fragments
from compaction can be omitted until indexing catches up. Current deletions still
apply. Exact-search fallbacks stay within the eligible data; this option does not
guarantee a fixed latency.

Enterprise gateways send this mode to the shared single-engine backend using
`QueryIndexedVectors`; ordinary searches retain distributed routing. The shared
`Query` structure and existing query request/response messages are unchanged.
