# S3 Vectors integration tests

Run `python3 -m pip install -r requirements.txt`, then `make test`.

The suite starts a master, volume, filer, authenticated S3/Lance gateway, and
vector engine on temporary ports. The same 12 API tests run with both Go and
Rust volume servers. Coverage includes bucket/index lifecycle, bucket policies,
empty results, ARN addressing, metadata, filters, cosine distance, atomic upserts,
pagination, segmented listing, authentication, and writer loss.

Server logs and data stay in pytest's temporary directory for diagnosis. Every
child process stops on exit. To select a volume server, set
`VOLUME_SERVER_IMPL=go` or `rust`.

In an enterprise checkout containing `rust/sw-vector-distributed`, the build and
default matrix also include distributed queries with two search workers. Those
queries must still succeed after the writer stops. `VECTOR_QUERY_MODE=single`
or `distributed` selects one query mode.

To use prebuilt binaries, set `WEED_BINARY`, `VECTOR_BINARY`,
`RUST_VOLUME_SERVER_BINARY`, and (for distributed queries) `VECTOR_WORKER_BINARY`,
then run `./run_test.sh`. `PYTHON` selects the Python interpreter. The Makefile
accepts `CARGO_PROFILE=dev` for faster local builds.
