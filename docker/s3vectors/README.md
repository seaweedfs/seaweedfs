# Vector buckets locally

Run on Linux containers (amd64 or arm64):

```sh
docker compose up -d
export AWS_ACCESS_KEY_ID=vector-demo
export AWS_SECRET_ACCESS_KEY=vector-demo-secret
export AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://localhost:8333 s3vectors create-index \
  --vector-bucket-name embeddings --index-name images \
  --data-type float32 --dimension 3 --distance-metric cosine
aws --endpoint-url http://localhost:8333 s3vectors put-vectors \
  --vector-bucket-name embeddings --index-name images \
  --vectors '[{"key":"first","data":{"float32":[1,0,0]}}]'
aws --endpoint-url http://localhost:8333 s3vectors query-vectors \
  --vector-bucket-name embeddings --index-name images \
  --query-vector '{"float32":[1,0,0]}' --top-k 1
```

The engine registers with the master. Wait for that registration before creating
an index. The Lance worker builds and refreshes ANN indexes as data grows;
queries can scan data before an ANN index is built. The demo's admin UI is at
http://localhost:12535 (user `admin`, password `vector-demo`). Demo credentials
and plaintext cluster connections are for this local example. Production
settings are in the Helm/operator TLS and authentication documentation.

`docker compose down` preserves the named data volume. Add `-v` to discard it.
Override `SEAWEEDFS_IMAGE` to test a newer image. With metrics-enabled engines,
append `--metrics-port=9329 --metrics-ip=0.0.0.0` to the vector command and scrape
`vector:9329/metrics` inside the Compose network.

A native development setup can use `weed mini -vector -vectorBucket=embeddings`
with `weed-vector` beside `weed` or on PATH. `-vector.binary` selects an explicit
binary, and `-vector.metricsPort=9329` enables HTTP readiness and metrics on a
compatible engine. The child inherits the cluster's configuration directory and
stops with mini. Run `weed-worker` separately for ANN maintenance. `weed shell`
provides `s3vectors -operation <Operation> -file request.json` for the same API
operations using trusted cluster credentials.
