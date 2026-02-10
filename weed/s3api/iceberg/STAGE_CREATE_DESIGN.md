# Iceberg Stage-Create Support Design

## Problem
`stage-create=true` currently cannot be fulfilled safely with the existing create path because table registration and metadata-file persistence are coupled. The prior behavior risked partial state (catalog entry without expected metadata file).

## Goals
- Implement Iceberg-compatible staged table creation semantics.
- Avoid partial table state on failures.
- Keep conflict behavior consistent with current commit path (`409 CommitFailedException`).
- Preserve backward compatibility for non-staged creates.

## Non-Goals
- Supporting all speculative client workflows in one step.
- Adding cross-process transactional primitives in filer/S3Tables.

## Proposed Semantics

### 1) `POST /v1/.../namespaces/{ns}/tables` with `stage-create=true`
- Validate request as normal create request.
- Build initial metadata (`v1`) exactly like normal create.
- Persist `v1.metadata.json` to table metadata directory.
- Do **not** call `S3Tables.CreateTable`.
- Return `200` with `LoadTableResult` (`metadata-location` = `.../metadata/v1.metadata.json`).

This produces a staged metadata root that can be committed later, while avoiding catalog visibility before commit.

### 2) `POST /v1/.../namespaces/{ns}/tables/{table}` commit path
- Keep current logic for existing tables.
- Add create-commit flow for missing table:
  - If `GetTable` returns not found:
    - Require at least one requirement of type `assert-create`.
    - Build base metadata from staged `v1` (if present) or synthetic metadata using resolved table location.
    - Apply updates and requirements.
    - Write new metadata file (typically `v2` if staged `v1` exists, otherwise `v1`).
    - Finalize with `S3Tables.CreateTable` (not `UpdateTable`).
- Map `already exists` / conflict to `409 CommitFailedException`.
- Best-effort cleanup of newly written metadata file on any create-finalization failure.

## Data and State Model

### Metadata files
- Staged create writes `metadata/v1.metadata.json` at final table location.
- Commit writes next version and updates catalog pointer by creating table entry.

### Optional staged-intent marker (recommended)
- Store small JSON marker under a dedicated internal prefix, e.g.:
  - `/buckets/<bucket>/.iceberg_staged/<namespace>/<table>/<uuid>.json`
- Fields:
  - `table_uuid`
  - `location`
  - `created_at`
  - `expires_at`
- Purpose:
  - Better observability and cleanup of abandoned staged creates.
  - Diagnostics for duplicate staged attempts.

If marker is skipped in v1 implementation, staged metadata file alone is still sufficient for functional behavior.

## API and Error Contract

### Create with stage-create
- Success: `200` with metadata payload.
- Invalid request: `400 BadRequestException`.
- Metadata file persistence failure: `500 InternalServerError`.

### Commit finalize (table missing + assert-create)
- Requirement failure: `409 CommitFailedException`.
- Concurrent create detected (`CreateTable` conflict/already exists): `409 CommitFailedException`.
- Other backend failures: `500 InternalServerError` (after best-effort metadata cleanup).

## Concurrency and Failure Handling
- Finalization uses `CreateTable` as the single catalog-visibility gate.
- On conflict, clean up newly written metadata file best-effort.
- Add bounded retry only when appropriate for conflict races.
- Preserve existing deterministic UUID behavior for retries within one request.

## Security and Authorization
- Stage-create request uses same auth checks as current create path.
- Finalize commit uses same auth checks as create/update table operations.
- No staged state should bypass policy checks.

## Implementation Plan

### Phase 1: Functional staged create
1. Remove `NotImplemented` rejection for `stage-create=true`.
2. In `handleCreateTable`:
   - write metadata file,
   - skip `CreateTable`,
   - return `LoadTableResult`.
3. In `handleUpdateTable`:
   - support not-found + `assert-create` as create-finalization flow using `CreateTable`.
4. Add cleanup logic on finalize failures.

### Phase 2: Hardening
1. Add optional staged-intent marker + TTL janitor.
2. Add metrics/counters:
   - staged create count
   - staged finalize success/failure
   - staged cleanup failures

### Phase 3: Interop polish
1. Validate behavior against Spark/Flink/Trino create transaction flows.
2. Document client expectations around staged metadata retention.

## Test Plan

### Unit tests
- `handleCreateTable` with `stage-create=true`:
  - returns success payload,
  - does not invoke `CreateTable`,
  - writes `v1.metadata.json`.
- Commit with missing table + `assert-create`:
  - creates catalog entry via `CreateTable`.
- Commit without `assert-create` on missing table:
  - returns `404` or `409` (choose one and lock behavior).
- Conflict on finalize:
  - returns `409 CommitFailedException`,
  - cleanup attempted.

### Integration tests
- End-to-end staged create -> commit finalize -> load table.
- Concurrent finalize race from two clients: exactly one success.
- Abandoned staged create cleanup behavior (if marker/TTL implemented).

## Rollout
- Gate behind a feature flag initially, e.g. `ICEBERG_ENABLE_STAGE_CREATE=true`.
- Default OFF for one release cycle if risk-sensitive; otherwise ON with clear release note.

## Open Questions
- Should commit-on-missing-table without `assert-create` be `404` or `409`?
- Do we require staged-intent marker in phase 1, or defer to phase 2?
- Should staged metadata be deleted automatically if never finalized within TTL?
