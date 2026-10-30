# S3 Vectors integration tests — design

## Goal

Cover the vector-bucket API surface end-to-end against a real `weed` server —
the path no unit or handler-level test exercises: HTTP dispatch on
`X-Amz-Target` / per-path routing, authentication, catalog persistence,
policy enforcement, and (when a vector engine is reachable) the data plane.

## Layout

    test/s3vectors/
      DESIGN.md                        this file
      s3vectors_integration_test.go    environment + test cases

Conventions follow `test/s3tables/sts_integration`: a `TestEnvironment`
brings up `weed server` (master + filer + volume + s3) on testutil-allocated
ports against a temp data dir, the test drives the JSON API over real HTTP,
and `testing.Short()` skips the suite.

## What the test covers

### Tier 1 — no vector engine required (runs everywhere)

These ops resolve through the catalog and the vector policy layer without an
engine RPC, so they work against a plain `weed server`:

- `CreateVectorBucket` / `GetVectorBucket` / `ListVectorBuckets` /
  `DeleteVectorBucket` — happy path, `encryptionConfiguration` round-trip
  (`sseType` -> stored `sseAlgorithm` -> echoed back as `sseType`), format
  filtering (an explicit non-Lance bucket is not a vector bucket).
- `PutVectorBucketPolicy` / `GetVectorBucketPolicy` / `DeleteVectorBucketPolicy`
  — the caller's document round-trips verbatim; `s3vectors:` actions are
  accepted, foreign-service actions/ARNs rejected; `NotAction`/`NotResource`
  rejected.
- Policy **enforcement** through the gateway: a non-owner `PutVectors` /
  `GetVectors` is denied without a grant, an `Allow` on
  `arn:aws:s3vectors:...:bucket/B/*` grants, a `Deny` vetoes an `Allow` on the
  same action, deletion restores owner-only access — the same lifecycle the
  package-level conformance test covers, but through signed HTTP requests.
- Validation boundaries: `maxResults` caps, `topK` bounds, `dataType`
  rejection, `dimension` range, empty/oversize PutVectors batches (these fail
  before the engine is touched, so they work engine-less too).

### Tier 2 — vector engine required (skipped when absent)

Set `S3VECTORS_ENGINE_ADDR` (or drop in a `weed-vector` binary on PATH) to
enable the data plane: `CreateIndex`, `GetIndex`, `ListIndexes`,
`DeleteIndex`, `PutVectors`, `GetVectors`, `ListVectors`, `DeleteVectors`,
`QueryVectors`. Each Tier-2 test skips itself when the engine isn't reachable
so the suite stays green on hosts that can't build the Rust workspace.

## Harness

- `NewTestEnvironment` mirrors `sts_integration`: locate or build the `weed`
  binary, allocate ports with `testutil`, spawn `weed server -dir=<tmp>
  -master.port -volume.port -filer -s3.port` with `-s3.vectors` enabled
  (the flag name from `weed s3 -h`), wait for the S3 port to accept
  connections, then run requests.
- Requests are signed SigV4 with the `s3vectors` service name — the gateway
  routes vectors traffic on the service name in the credential scope, so an
  unsigned or `s3`-signed request never reaches the vectors router. A small
  `vectorsClient` helper wraps POST + `X-Amz-Target`/`/op` + signing +
  status-code assertions; identities vary per-request for policy tests.
- `Cleanup` kills the process group and removes the temp dir.
- `TestMain`-less design: each test builds its own environment so a hung
  server can't take down the suite; `t.Parallel()` is deliberately avoided —
  port allocation is cheap but vector-engine bring-up is not.

## Not covered here (by design)

- Auth failure modes (expired creds, bad signatures) — covered by the
  `sts_integration` suite and the s3api auth tests; the vectors router shares
  the same `iam.AuthenticateRequest` path.
- Lance/segment internals — the package's `listSegmentVectors` unit tests and
  the engine's own suite cover them; the integration layer only asserts the
  API contract.
