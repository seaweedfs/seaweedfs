# Snowflake S3Compat API test suite

Integration tests that run the upstream
[Snowflake s3compat API test suite](https://github.com/snowflakedb/snowflake-s3compat-api-test-suite)
against SeaweedFS. The suite covers `getBucketLocation`, `getObject` (including
range reads), `getObjectMetadata`, `putObject` (including a 5 GB upload),
`listObjectsV2` (including paged listing of >1000 objects), `deleteObject`,
`deleteObjects`, `copyObject`, and `generatePresignedUrl`.

## Running locally

Requires `weed` (or `WEED_BIN`), the `aws` CLI, `mvn`, and JDK 11+ on `PATH`.

```sh
(cd weed && go install -buildvcs=false)   # build weed first
bash test/s3/snowflake/run.sh
```

`run.sh` starts a `weed server` with S3 enabled, calls `prepare.sh` to create
the fixtures, clones the suite into a scratch dir, and runs
`mvn -Dtest=S3CompatApiTest`. Set `WORK_DIR` to keep the server log and suite
clone around, `SKIP_SERVER_START=1` with `ENDPOINT_URL` to run against an
already-running server, and see the top of `run.sh` for the other overrides.

## Notes

- The server runs with `-s3.autoCreateBucket=false` so PUTs to a missing bucket
  return `NoSuchBucket` like AWS; the suite asserts this.
- The suite forces virtual-hosted-style bucket addressing, which requires
  wildcard DNS that does not exist for a local endpoint. `run.sh` switches it
  to path-style access with a `sed` patch.
- `NOT_ACCESSIBLE_BUCKET` is a real bucket carrying a deny-all bucket policy,
  which is how the suite's `AccessDenied` negative tests are satisfied.
