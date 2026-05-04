# Unity Catalog OSS integration tests

These tests run Unity Catalog OSS in Docker against an embedded SeaweedFS
S3 endpoint. The `server.properties` mirrors the upstream playground at
[`mds-in-a-box/unitycatalog-playground`](https://github.com/data-engineering-helpers/mds-in-a-box/tree/main/unitycatalog-playground).

| Test | Variant | Status |
| --- | --- | --- |
| `TestUnityCatalogDeltaIntegration` | static keys, `aws.masterRoleArn=` empty | passes; covers catalog/schema/EXTERNAL Delta CRUD against SeaweedFS-backed warehouse and asserts that UC's `/temporary-table-credentials` *cannot* vend usable creds with this configuration -- exactly the gap the playground reports. |
| `TestUnityCatalogMasterRoleIntegration` | `aws.masterRoleArn=arn:aws:iam::000000000000:role/UnityCatalogVendedRole` | passes; proves SeaweedFS' STS endpoint accepts `sts:AssumeRole` for the role UC would use (Go SDK round-trip), and that UC starts and accepts CRUD when wired with the master-role config. The third hop -- UC's Java `StsClient` actually reaching SeaweedFS STS -- is currently a known gap, logged via `t.Logf` but not asserted. |
| `TestUnityCatalogDeltaRsRoundTrip` | static keys + `delta-rs` Python client | passes; resolves table metadata through UC and writes/reads a real Delta table at the registered `storage_location` using `python:3.11-slim + deltalake` with the SeaweedFS test credentials. |

## Prerequisites

- Docker available locally (the tests call `docker run` / `docker build` directly).
- A `weed` binary at the repo root (`weed/weed`) or on `$PATH`.

## Run

```bash
go test -timeout 15m \
    -run 'TestUnityCatalog' \
    ./test/s3tables/unity_catalog/...
```

Pin a specific Unity Catalog image (defaults to
`unitycatalog/unitycatalog:main`):

```bash
UC_IMAGE=unitycatalog/unitycatalog:latest \
    go test -timeout 15m -run TestUnityCatalogDeltaIntegration \
    ./test/s3tables/unity_catalog/...
```

The tests self-skip when Docker is unavailable or no `weed` binary is on
the path; running under `-short` also skips them.

## Why the static-key path can't vend usable creds

UC OSS' `AwsCredentialVendor.createPerBucketCredentialGenerator`:

```java
if (config.getSessionToken() != null && !config.getSessionToken().isEmpty()) {
    return new AwsCredentialGenerator.StaticAwsCredentialGenerator(config);
}
return createStsCredentialGenerator(config);
```

With the playground's `aws.masterRoleArn=` empty and no `s3.sessionToken.0`,
UC always falls back to an internal `StsClient.assumeRole(...)`. Against
SeaweedFS that call comes back as `AccessDenied` because the AWS Java SDK's
STS request lands on a SeaweedFS S3 path rather than the STS handler (the
Go SDK STS path from `assumeRoleViaSeaweedFS` works in the same SeaweedFS
instance). Setting a stub `s3.sessionToken.0` makes UC use
`StaticAwsCredentialGenerator` and *return* the static keys, but SeaweedFS
then rejects the vended `X-Amz-Security-Token` since it didn't issue it.

## What the tests actually validate today

- Unity Catalog accepts a SeaweedFS-backed `server.properties` and starts.
- Catalog / schema / EXTERNAL Delta table CRUD all work against the
  SeaweedFS warehouse via the UC REST API.
- SeaweedFS' STS endpoint correctly issues `sts:AssumeRole` credentials
  for the `UnityCatalogVendedRole` and those credentials are accepted on
  S3 round-trips (Go AWS SDK).
- Delta-RS resolves a UC table's `storage_location` and can write/read Delta
  data through the SeaweedFS S3 endpoint with the test credentials.

## What is still pending

- UC's Java `StsClient` reaching SeaweedFS' STS handler. Likely needs a
  closer look at how SeaweedFS' router treats POST `/` with `Action=...`
  in the form body when the embedded IAM router is not enabled in `weed
  mini`. Until that's resolved, the master-role test logs the failure but
  does not assert it.
- Spark client pointed at a UC catalog. The Delta-RS path runs as part of
  `TestUnityCatalogDeltaRsRoundTrip`.

## MANAGED tables

Not exercised. UC OSS gates them behind `server.managed-table.enabled=true`
and a two-step staging flow; EXTERNAL Delta is the simpler path and
matches what most playground users actually run.
