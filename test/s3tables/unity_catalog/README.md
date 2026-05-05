# Unity Catalog OSS integration tests

These tests run Unity Catalog OSS in Docker against an embedded SeaweedFS
S3 endpoint. The `server.properties` mirrors the upstream playground at
[`mds-in-a-box/unitycatalog-playground`](https://github.com/data-engineering-helpers/mds-in-a-box/tree/main/unitycatalog-playground).

| Test | Variant | Status |
| --- | --- | --- |
| `TestUnityCatalogDeltaIntegration` | static keys, `aws.masterRoleArn=` empty | passes; covers catalog/schema/EXTERNAL Delta CRUD against SeaweedFS-backed warehouse and asserts that UC's `/temporary-table-credentials` *cannot* vend usable creds with this configuration -- exactly the gap the playground reports. |
| `TestUnityCatalogMasterRoleIntegration` | `aws.masterRoleArn=arn:aws:iam::000000000000:role/UnityCatalogVendedRole` | passes; proves SeaweedFS' STS endpoint accepts `sts:AssumeRole` for the role UC would use (Go SDK round-trip), and that UC starts and accepts CRUD when wired with the master-role config. UC's own StsClient still talks to real AWS regardless of `aws.endpoint` / `AWS_ENDPOINT_URL_STS` (UC bug, see below); that hop is logged via `t.Logf` rather than asserted. |
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
`unitycatalog/unitycatalog:v0.4.0`):

```bash
UC_IMAGE=unitycatalog/unitycatalog:main \
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

With `aws.masterRoleArn=` empty and `s3.sessionToken.0=` empty (this
test's configuration), `/temporary-table-credentials` short-circuits with
`"S3 bucket configuration not found."` before UC fires any STS call.
Setting a stub `s3.sessionToken.0` switches UC to
`StaticAwsCredentialGenerator` and the endpoint returns the static keys,
but the response carries that stub session token -- SeaweedFS won't
recognize it on the next S3 call, so the vended creds aren't usable for
table I/O. Clients have to fall back to the static keys directly.

With `aws.masterRoleArn` set, UC's `AwsCredentialGenerator.StsAwsCredentialGenerator`
builds the StsClient with only `.region(...)` and `.credentialsProvider(...)` --
no `.endpointOverride()`. The SDK's generic env-var resolution doesn't kick in
for that builder shape, so even with `AWS_ENDPOINT_URL_STS=...` (or the
matching `aws.endpointUrlSts` Java property, or the catch-all
`AWS_ENDPOINT_URL=...`) the StsClient still targets real AWS and gets back
`InvalidClientTokenId`. Verified by pointing the env var at port 1: UC reports
the same AWS-issued 403 that it reports against SeaweedFS, and a sniffer in
front of SeaweedFS' STS port records zero traffic. SeaweedFS' STS handler
itself works -- the Go SDK round-trip in `assumeRoleViaSeaweedFS` proves that
against the same SeaweedFS instance.

UC's own AWS credential-vending tests don't catch this because they mock
`StsClient` away entirely -- `BaseCRUDTestWithMockCredentials` injects a
custom `stsClientBuilderSupplier` returning an `EchoAwsStsClient` that
synthesizes credentials in-process, and `CloudCredentialVendorTest` uses
`Mockito.mockStatic(StsClient.class)`. No test ever exercises the wire
path between UC's Java SDK and a real STS endpoint, so the missing
`endpointOverride` slipped through.

Fix is upstream in
[unitycatalog/unitycatalog#1532](https://github.com/unitycatalog/unitycatalog/pull/1532),
which adds an `aws.endpoint` property and applies it to both the StsClient
and the S3Client builders. Until that lands, the master-role test logs
the failure but does not assert it.

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

Nothing on the SeaweedFS side. The remaining gap (UC's StsClient ignoring
endpoint config) needs a UC OSS patch upstream.

## MANAGED tables

Not exercised. UC OSS gates them behind `server.managed-table.enabled=true`
and a two-step staging flow (`POST /staging-tables` then `POST /tables`);
EXTERNAL Delta is the simpler path and what these tests cover.
