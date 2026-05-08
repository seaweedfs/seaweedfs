# S3 Lifecycle Integration Tests

End-to-end test of the event-driven S3 lifecycle worker, exercised through
the `s3.lifecycle.run-shard` shell command.

## Why backdate mtimes?

The S3 API rejects `Expiration.Days < 1`, so a literal "wait one day"
integration test isn't workable. Each test sets up a 1-day expiration rule,
puts the target object, then rewrites its filer entry's `Mtime` to ~30 days
ago via `filer_pb.UpdateEntry`. From the engine's perspective the object is
past its expiration window the moment the shell command starts.

## Running

```sh
# build the binary, start a local mini cluster, run tests, stop it
make test-with-server

# or, if a cluster is already running on the default ports
make test
```

The test runs the shell command once with `-shards 0-15` (one filer
subscription covering all 16 shards) rather than computing the target
object's shard up front. This keeps the test independent of the
`ShardID(bucket, key)` hash function — only that some shard reaches the
deletion within the polling window.

## Environment

| variable             | default                  | description                |
|----------------------|--------------------------|----------------------------|
| `WEED_BINARY`        | _required_               | path to `weed_binary`      |
| `S3_ENDPOINT`        | `http://localhost:8333`  | S3 API URL                 |
| `S3_GRPC_ENDPOINT`   | `localhost:18333`        | S3 gRPC for lifecycle dispatch |
| `MASTER_ENDPOINT`    | `http://localhost:9333`  | master HTTP                |
| `FILER_GRPC_ADDRESS` | `localhost:18888`        | filer gRPC for `UpdateEntry` |
