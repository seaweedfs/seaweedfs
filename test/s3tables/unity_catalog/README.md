# Unity Catalog OSS integration tests

These tests run Unity Catalog OSS in Docker against an embedded SeaweedFS
S3 endpoint. The `server.properties` mirrors the upstream playground at
[`mds-in-a-box/unitycatalog-playground`](https://github.com/data-engineering-helpers/mds-in-a-box/tree/main/unitycatalog-playground).

| Test | Variant | What it covers |
| --- | --- | --- |
| `TestUnityCatalogDeltaIntegration` | `aws.masterRoleArn=` empty, static keys | Catalog/schema/EXTERNAL Delta CRUD, list/get, `temporary-table-credentials`, S3 round-trip via vended creds. Mirrors the playground exactly. |
| `TestUnityCatalogMasterRoleIntegration` | `aws.masterRoleArn=arn:aws:iam::000000000000:role/UnityCatalogVendedRole` | Same flow, but UC is wired to call `sts:AssumeRole` against SeaweedFS (via `AWS_ENDPOINT_URL_STS`). The test asserts the vended creds carry a `session_token` and a non-static access key, proving the role-vended path actually works. |
| `TestUnityCatalogDeltaRsRoundTrip` | static keys + `delta-rs` Python client | Builds a small `python:3.11-slim + deltalake` image, fetches temporary creds from UC, and writes/reads a real Delta table at the registered `storage_location`. |

## Prerequisites

- Docker available locally (the tests call `docker run`/`docker build` directly).
- A `weed` binary at the repo root (`weed/weed`) or on `$PATH`.

## Run

```
go test -timeout 15m \
    -run 'TestUnityCatalog' \
    ./test/s3tables/unity_catalog/...
```

To pin a specific Unity Catalog image (defaults to
`unitycatalog/unitycatalog:main`):

```
UC_IMAGE=unitycatalog/unitycatalog:latest \
    go test -timeout 15m -run TestUnityCatalogDeltaIntegration \
    ./test/s3tables/unity_catalog/...
```

The tests self-skip when Docker is unavailable or no `weed` binary is on
the path; running under `-short` also skips them.

## Notes on the master-role variant

Unity Catalog OSS implements the `AwsIamRole` storage credential by
calling `sts:AssumeRole`. When `aws.masterRoleArn` is set, UC uses an
internal `StsClient`. Two env vars are passed to the container so that
client targets SeaweedFS instead of the real AWS STS endpoint:

- `AWS_ENDPOINT_URL_STS=http://host.docker.internal:<weed s3 port>`
- `AWS_ENDPOINT_URL=http://host.docker.internal:<weed s3 port>` (fallback for older AWS SDK builds)

The SeaweedFS side enables the STS handler with `-s3.iam.config` and a
`UnityCatalogVendedRole` role whose trust policy allows any caller to
assume it. The Go assertions check that the vended creds:

1. Include a `session_token` (only present when STS actually issued a
   session).
2. Do not echo back the static admin access key.

## MANAGED tables

Not exercised. UC OSS gates them behind `server.managed-table.enabled=true`
and a two-step staging flow; EXTERNAL Delta is the simpler path and
matches what most playground users actually run.
