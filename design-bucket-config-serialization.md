# Design: Serializing Bucket Configuration Mutations

Issue #9651 — concurrent `PutBucketVersioning` + `PutBucketEncryption` (as Terraform
issues them in parallel) intermittently lose the encryption write.

## Root cause

The bucket's entire config lives in one filer entry, `/buckets/<name>`. Every
config API does a read-modify-write of that single entry, and the writes are not
serialized:

- `updateBucketConfig(bucket, fn)` (`s3api_bucket_config.go:468`) — sources from a
  possibly-stale cached `BucketConfig`, mutates `Entry.Extended`, writes the
  **whole** entry. Used by: versioning, object-lock config, lifecycle, ACL/owner.
- `UpdateBucketMetadata` → `setBucketMetadata` (`:1042`) — reads a fresh entry,
  mutates `Entry.Content`, writes the **whole** entry. Used by: encryption, CORS,
  tagging, ownership, policy, notification.

Two ingredients produce the lost update:

1. **No serialization** of the read→modify→write (the cache mutexes only guard the
   in-memory map, not the RMW).
2. **Whole-entry rewrite from an independent snapshot** — `updateBucketConfig`
   rebuilds from a stale cached `BucketConfig` whose `Content` predates the
   concurrent encryption write, so writing the whole entry reverts `Content`.

Sequential calls always pass (each sees the previous write), so it only surfaces
under concurrency — and CI's slower IO widens the window (the "2 of ~12 runs").

## Goals

- No lost updates across concurrent bucket-config changes — for **all** config
  fields, not just versioning/encryption.
- Correct for a single S3 gateway (the reported case) and for multiple gateways.
- Reuse the filer primitives just merged (per-path lock, `WriteCondition`,
  `ObjectTransaction`); do not reintroduce a distributed lock.
- Minimal blast radius: the fix lands at the two chokepoint helpers.

## Non-goals

- Changing the one-entry-per-bucket storage model.
- Multi-filer-concurrent bucket writes (addressed only as an optional phase 3).

## The two ingredients map to two complementary fixes

### Fix A — serialize + read fresh (closes the window for whole-entry writers)

Both `updateBucketConfig` and `UpdateBucketMetadata` must run their RMW under one
per-bucket critical section, and **re-read the entry fresh from the filer inside
it** — not rebuild from the cached `BucketConfig`. The lock alone is insufficient:
without the fresh read, two serialized writers still each apply a stale snapshot.

### Fix B — field-level updates (removes the collision entirely)

The two writers touch disjoint fields (`Extended[versioning]` vs `Content`). If
each path updated only its own field instead of rewriting the whole entry, neither
could clobber the other regardless of ordering. This is the structural fix and
makes serialization a defense-in-depth concern rather than a correctness
requirement for cross-field cases.

## Where to serialize (layering)

The bucket entry is a single filer entry, so unlike object writes there is no
sharding — the question is purely the scope of the lock:

| Layer | Serializes across | Cost | Notes |
|---|---|---|---|
| 1. Gateway-local per-bucket lock | one gateway process | tiny | fixes the reported (single-gateway/CI) case |
| 2. Filer per-path lock via conditional write | all gateways on one filer | small | reuses #9640 `CreateEntry`+`WriteCondition` |
| 3. Route-by-key to bucket-key owner filer | all gateways and filers | medium | same mechanism as the object DLM-removal |

## Recommended plan (phased)

### Phase 1 — minimal fix for #9651 (gateway-local lock + fresh read)

Add a bounded per-bucket lock table to `S3ApiServer`, reusing the same
`util.LockTable` the filer uses for its per-path lock:

```go
// in S3ApiServer
bucketConfigLocks *util.LockTable[string] // serialize bucket-entry RMW

func (s3a *S3ApiServer) withBucketConfigLock(bucket string, fn func() s3err.ErrorCode) s3err.ErrorCode {
    lk := s3a.bucketConfigLocks.AcquireLock("bucketConfig", bucket, util.ExclusiveLock)
    defer s3a.bucketConfigLocks.ReleaseLock(bucket, lk)
    return fn()
}
```

Wrap the RMW in **both** chokepoints, and inside the lock read the entry fresh:

- `updateBucketConfig`: acquire the lock; re-read `/buckets/<name>` from the filer
  (not the cache); rebuild `BucketConfig` from that fresh entry; apply `fn`; write;
  invalidate cache; release.
- `UpdateBucketMetadata`/`setBucketMetadata`: same lock key; it already reads fresh,
  so it just needs to share the critical section.

Both must use the **same** lock keyed on `bucket`, so versioning and encryption
contend on one mutex. This closes the reported window. Limitation: only one
gateway; two gateways behind a load balancer still race.

Test: parallel `PutBucketVersioning` + `PutBucketEncryption`, assert both persist
(the exact Terraform scenario), plus an N-way parallel variant over distinct
fields.

### Phase 2 — robust across gateways (field-level + CAS via merged primitives)

Move the writers off whole-entry rewrites:

- **Extended-based config** (versioning, object-lock, ownership, tagging-in-Extended)
  → `ObjectTransaction` `PATCH_EXTENDED` on `/buckets/<name>`. The owner filer reads
  the entry fresh under its per-path lock and merges only the named keys, so the
  gateway never sends a whole-entry snapshot — this dissolves *both* ingredients for
  these fields.
- **`Content`-based config** (encryption, CORS, tags blob) — **chosen and
  implemented (b3): extend `PATCH_EXTENDED` with `set_content`.** Under the same
  per-path lock the filer reads the entry fresh, merges extended attributes, and
  replaces `Content`, preserving the rest. So a content write becomes a field-level
  patch too — `setBucketMetadata` patches `Content`, `updateBucketConfig` patches
  extended keys, and the two serialize on the lock instead of racing whole-entry
  rewrites. This is cleaner than the alternatives below: no client-side retry, no
  storage migration, and it reuses `ObjectTransaction`'s existing atomic lock.
  - (b1, rejected) Conditional `CreateEntry` overwrite with `IF_ETAG_MATCH` + retry
    (#9640): correct but needs client-side retry, and the bucket directory entry has
    no reliable ETag to compare on.
  - (b2, future) Migrate each per-feature config out of the single `Content` blob
    into its own `Extended` key. Then even *intra-blob* writes (tags vs encryption)
    stop racing. Larger migration; tracked separately.

Once all paths are field-level patches, the phase-1 gateway lock is unnecessary —
the filer enforces atomicity. (This is the path taken: phase 1 was skipped.)

### Phase 3 — multi-filer (only if needed)

If multiple filers can write `/buckets/<name>` concurrently, a filer-local per-path
lock no longer suffices. Route bucket-config writes to
`PrimaryForKey("/buckets/<name>")` (the lock-ring view) and serialize on that one
owner filer — the same route-by-key design used to take object writes off the DLM.
Overkill for rare config writes; include only if multi-filer bucket writes are real.

## Correctness summary

- Phase 1: all RMW for a bucket serialize within a gateway; the fresh read means the
  second writer observes the first's change. Closes #9651 for single-gateway.
- Phase 2: `PATCH_EXTENDED` is atomic field-level merge at the filer (no snapshot);
  CAS turns a concurrent `Content` write into a retry, enforced under the filer's
  per-path lock — correct for any number of gateways sharing a filer.
- Phase 3: one owner filer serializes all writers — correct across filers too.

## Scope checklist (every path that RMWs the bucket entry)

All of these funnel through the two chokepoints, so fixing the chokepoints covers
them — but the fix must not leave any of them on an unserialized path:

- via `updateBucketConfig`: versioning, object-lock config, lifecycle, ACL/owner.
- via `UpdateBucketMetadata`/`setBucketMetadata`: encryption, CORS, tagging,
  ownership controls, bucket policy, notification.
- bucket create/delete (`CreateEntry`/`DeleteEntry` of `/buckets/<name>`) already
  go through the filer's per-path lock on `CreateEntry`; ensure they take the same
  bucket lock if they also patch config.

## Cache rule (must document in code)

Under the lock, **read the entry from the filer, never rebuild from the cached
`BucketConfig`**. The cache is for reads; it must be invalidated on every write and
never be the source for an RMW. This is the single most important detail — the lock
without the fresh read does not fix the bug.
