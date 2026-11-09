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
| `--read-cache-mb` | `512` | Immutable data/index read cache; zero disables it |
| `--nprobes` | Automatic | ANN partitions searched per query |
| `--metrics-port` | `0` | HTTP metrics and health listener; zero disables it |
| `--metrics-ip` | `127.0.0.1` | HTTP listener address |
| `--security-toml` | Discovered | Cluster certificates, signing keys and encryption keys |

The HTTP listener provides `/health`, `/ready`, and `/metrics`. Readiness requires an active registration with the master. The gRPC listener also implements the standard health service for `vector_pb.VectorService` and the whole server. These health statuses follow master registration.

Metrics use the `SeaweedFS_vector_` prefix:

- `connected`: active master registration.
- `request_total{type,code}`, `request_seconds{type}`, and `in_flight_requests{type}`: engine requests and their gRPC outcomes.
- `query_results_total`: vectors returned by queries.
- `cache_requests_total{result}`, `cache_bytes`, and `cache_entries`: immutable read cache activity and occupancy.
- `storage_read_bytes_total` and `storage_read_seconds`: storage fetches for cacheable immutable reads.

The S3 gateway separately publishes `SeaweedFS_s3_` request metrics with `type="s3vectors.<Operation>"`. Scrape both processes to distinguish gateway latency from engine work. Metrics and HTTP probes are unauthenticated; bind them to an internal address or restrict their network access.

For mutual TLS, configure `[grpc.vector]` for the engine's server certificate, `[grpc.client]` for its master/filer connections, and the cluster CA. HTTPS volume reads also use `[https.client]`. The engine follows the same signing and encryption configuration as the other cluster clients.

Run the Lance maintenance worker against the same masters and Lance namespace to build and update ANN indexes. Queries can scan data that has not been indexed yet. See the [maintenance worker documentation](../seaweed-worker/README.md) and [Helm chart](../k8s/charts/seaweedfs/README.md) for deployment settings.

The integration suite at `test/s3vectors` runs the same API and observability checks against both Go and Rust volume servers, including mutual TLS, signed volume reads, and encrypted volume data.
