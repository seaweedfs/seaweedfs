# S3 Lifecycle Redesign — Event-Driven Expiration

## Why

`PutBucketLifecycleConfiguration` today walks every entry under the bucket inside the request handler (`weed/s3api/s3api_bucket_handlers.go:900-1068` → `weed/s3api/filer_util.go:191`), so it times out on buckets with tens of thousands of objects. Worse, only one rule shape actually expires anything: simple `Expiration.Days` + prefix-only filter on a non-versioned bucket. Tag/size filters, `Expiration.Date`, versioned buckets, `NoncurrentVersionExpiration`, `AbortIncompleteMultipartUpload`, and `ExpiredObjectDeleteMarker` are silent no-ops. There is no scan-time evaluator anywhere — `weed/s3api/s3lifecycle/rule.go` defines `Rule`, `ObjectInfo`, `EvalResult`, `Action` but no `Evaluate` and no callers.

Three independent mechanisms exist today, only one of which is real enforcement:

1. Read-triggered TTL check on the filer (`weed/filer/filer.go:396-408`, `:435-449`). Lazy: cold objects never expire.
2. Volume-level TTL: `filer.conf` + TTL volumes drop chunks wholesale, but only for writes that arrive after the rule exists.
3. The synchronous "back-stamp" inside the PUT handler that walks existing entries to bridge #1 and #2. This is what's slow.

## Design

### Core idea

Drive lifecycle expiration off the persistent metadata change log at `/topics/.system/log/<YYYY-MM-DD>/<HH-MM>.<filerId>` (`weed/filer/topics.go:5`, `weed/filer/filer_notify.go:166`). The same log powers `filer.sync`, `filer.backup`, and `filer.meta.tail`. Each event carries `OldEntry` and `NewEntry` with full attributes and `Extended`. The day/hour-minute layout makes time-windowed reads cheap. Cost scales with **change rate**, not bucket size.

### Repo facts the design depends on

Verified in the codebase:

- Meta events are persisted under `SystemLogDir = TopicsDir + "/.system/log"` and accessed via `Filer.ReadPersistedLogBuffer` and `Filer.CollectLogFileRefs` (`weed/filer/filer_notify_read.go`).
- Each event's `EventNotification.OldEntry` and `NewEntry` are `*filer_pb.Entry` with full `Attributes` and `Extended` (`weed/pb/filer_pb/filer.pb.go:622-672`).
- Versioning constants (`weed/s3api/s3_constants/`):
  - `VersionsFolder = ".versions"` — appended as a suffix, not a child. A versioned object at key `foo/bar.txt` has its versions under directory `foo/bar.txt.versions/` (`weed/s3api/filer_multipart.go:578`).
  - `ExtDeleteMarkerKey = "Seaweed-X-Amz-Delete-Marker"` — a delete marker is a version file in the `.versions` directory whose `Extended[ExtDeleteMarkerKey] == "true"`. There is **no** `IsDeleteMarker` field on `filer_pb.Entry`.
  - `ExtLatestVersionIdKey`, `ExtLatestVersionFileNameKey`, `ExtLatestVersionMtimeKey`, `ExtLatestVersionIsDeleteMarker` — the *current-version pointer* lives on the `.versions` directory entry's `Extended`.
- Multipart uploads live under `<bucket>/.uploads/<uploadId>/` (`s3a.genUploadsFolder` in `weed/s3api/s3api_object_handlers_multipart.go:475`).
- Existing internal delete helpers we will reuse: `s3a.createDeleteMarker(bucket, object)` (`weed/s3api/s3api_object_versioning.go:162`), `s3a.deleteSpecificObjectVersion(bucket, object, versionId)` (`:968`), `s3a.deleteUnversionedObjectWithClient(client, bucket, object)` (`weed/s3api/s3api_object_handlers_delete.go:169`).

### Phase 0 — Verified assumptions

Each open assumption resolved against the codebase at `sulfuric-podium`:

**1. Persisted log payload includes `Extended`.** `Filer.logMetaEvent` (`weed/filer/filer_notify.go:110-119`) marshals `*filer_pb.SubscribeMetadataResponse{EventNotification:{OldEntry, NewEntry, …}}` into `LogEntry.data` via `entry.ToProtoEntry()`, and `Entry.extended` (`weed/pb/filer.proto:135`) is a `map<string,bytes>` serialized as part of the proto. `ReadPersistedLogBuffer` (`weed/filer/filer_notify.go:203`) hands back the same `*filer_pb.LogEntry` whose `data` deserializes back to the full event with `Extended` intact. Lifecycle's predicate evaluation against tags/`Extended` keys is therefore directly available from the meta log without any side fetch.

**2. Meta-log retention is unbounded by default.** Persisted log files at `<SystemLogDir>/YYYY-MM-DD/HH-MM.<filerId>` are written via `Filer.appendToFile` (`weed/filer/filer_notify_append.go:14-49`). The created `Entry` has no `TtlSec` field set, only `Crtime`/`Mtime`/`Mode`/`Uid`/`Gid`. There is no built-in cleaner: nothing under `weed/filer/` or `weed/server/` deletes `topics/.system/log/` files. The volume-level TTL only kicks in if an operator configures a `filer.conf` rule whose `LocationPrefix` matches `/topics/.system/log/` and whose `Ttl` is set, in which case `Filer.appendToFile` → `assignAndUpload` (`weed/filer/filer_notify_append.go:51-94`) routes through `MatchStorageRule` and the assigned volume carries the rule's `Ttl`. Without that explicit configuration, retention is operator-bounded only (manual deletion of old day directories).

This means:
- For default deployments, `metaLogRetention = ∞` and the retention mode gate (`metaLogRetention < eventLogHorizon(rule) + bootstrapLookbackMin`) never trips — every reader-driven kind runs `event_driven`.
- For deployments that opt into volume-TTL pruning of the meta log, operators must configure `metaLogRetention` to match the configured TTL; the gate then promotes long-horizon rules to `scan_only` automatically.
- Phase 2 will read `metaLogRetention` from a cluster-config knob (default: a sentinel meaning "unbounded"). When the operator sets a TTL on `topics/.system/log/`, they must also set `metaLogRetention` to the same value; the design doc surfaces this as a required-coupled configuration in Phase 8 docs.

**3. `.versions/` filename scheme.** Inside `<object>.versions/`, version files are named `v_<32-hex>` where the 32-hex string is `<16-hex-timestamp><16-hex-random>` (`weed/s3api/s3api_version_id.go:30-55`, `:148-150`). Two formats coexist, distinguished by the timestamp portion's value relative to threshold `0x4000000000000000` (`weed/s3api/s3api_version_id.go:24`):
- **New (inverted) format**: timestamp portion = `MaxInt64 - now_ns`, value > threshold (~0x68… in 2025). Newer versions sort **earlier** lexicographically. Used for new `.versions/` directories.
- **Old (raw) format**: timestamp portion = `now_ns`, value < threshold. Older versions sort earlier lexicographically. Detected by reading `Extended[ExtLatestVersionIdKey]` on the `.versions/` directory entry and applying `isNewFormatVersionId` (`weed/s3api/s3api_version_id.go:58-70`).

For the design's "successor" version computation (the version that replaced this one to make it non-current):
- New format: the lex-immediate-predecessor in `.versions/` (sorts earlier = newer).
- Old format: the lex-immediate-successor (sorts later = newer).
- Universal fallback: `getVersionTimestamp(versionId)` (`weed/s3api/s3api_version_id.go:74-92`) returns the actual ns timestamp regardless of format; `compareVersionIds(a, b)` (`:97-140`) gives a format-agnostic newest-first comparator. Phase 5 will use these helpers when discovering successor non-current time.

The `.versions/` directory entry's `Extended` carries the current-version pointer (`ExtLatestVersionIdKey`, `ExtLatestVersionFileNameKey`, `ExtLatestVersionMtimeKey`, `ExtLatestVersionIsDeleteMarker`) — already documented above as repo facts.

### Storage

**Three storage areas, separated by ownership and write-concurrency boundaries.**

The bucket directory's xattrs continue to hold the policy itself:

- `Extended["s3-bucket-lifecycle-configuration-xml"]` — original XML (existing key, see `weed/s3api/s3api_bucket_lifecycle_config.go:11`).

**Multi-action rules.** A single AWS lifecycle XML `<Rule>` may declare multiple actions in parallel — for example `Expiration.Days=90` together with `AbortIncompleteMultipartUpload.DaysAfterInitiation=7` and `NoncurrentVersionExpiration.NoncurrentDays=30`. Each action has its **own** delay/horizon/mode and must drive its own pending stream and cursor independently. Modeling a rule as one compiled entry with one `kind` and one delay collapses these — e.g. picking the smallest delay (7d MPU) means the 90d expiration cursor "advances past" objects that aren't yet due, and the 90d action never re-fires for them.

The engine therefore **expands every XML rule into N compiled actions** at compile time, where N is the count of action sub-elements actually populated. Each compiled action has its own state, its own pending file, its own delay group, and its own mode. The shared filter (Prefix, Tags, Sizes, Status) is copied to each action — actions of the same rule are evaluated in parallel against the same filter.

**Per-action state** lives outside the bucket under `/etc/s3/lifecycle/<bucket>/<rule_hash_hex>/<action_kind>/`. One subdirectory per action; one writer per directory (the per-action cluster lock). The intermediate `<rule_hash_hex>` keeps a rule's actions grouped so operators can list "rule R's status" by enumerating its action subdirectories:

- `state` — protobuf, one record:
  - `rule_hash` (8 bytes; matches the parent rule directory)
  - `action_kind`: `EXPIRATION_DAYS` | `EXPIRATION_DATE` | `NONCURRENT_DAYS` | `NEWER_NONCURRENT` | `ABORT_MPU` | `EXPIRED_DELETE_MARKER` (matches the leaf directory name)
  - `rule_id` (display only; can be empty/duplicate; identical across sibling actions of the same XML rule)
  - `mode`: `event_driven` | `scan_at_date` | `scan_only` | `disabled` (scheduling intent — per action, not per rule)
  - `degraded_reason`: `NONE` | `LAG_HIGH` | `PENDING_FULL` | `DELETE_FAILURES` | `OPERATOR_PAUSED` | `RETENTION_BELOW_HORIZON` | `LOST_LOG` (orthogonal health signal; a rule's individual action can be `event_driven AND LAG_HIGH`, with auto-promotion to `scan_only` deferred until the next threshold; `RETENTION_BELOW_HORIZON` and `LOST_LOG` are set together with `mode = scan_only` by the retention gate and lost-log GC respectively)
  - `degraded_since_ns`: when the current `degraded_reason` was set; cleared together with reason
  - `bootstrap_complete`
  - `bootstrap_started_at_ns`
  - `bootstrap_completed_at_ns`
  - `last_safety_scan_ts_ns`
  - `next_safety_scan_ts_ns`
  - counters: `evaluated_total`, `expired_total`, `metadata_only_total`, `error_total`
- `pending` — protobuf, append-with-tombstones: `repeated PendingItem { path, version_id, due_at_ns, expected_identity }`. **One use only**: late predicate changes that create not-yet-due eligibility for *this* action (tag added at age 30d on a 60d rule's `EXPIRATION_DAYS` action). Dedupe key `(path, version_id)`. Sibling actions on the same rule have their own pending files.

No per-rule `blocked` file (compliance ledger). Object-lock and retain-until are the operator's concern — see "Object lock and compliance" below. (Cluster-level `_reader/blockers` is unrelated — it tracks paused cursor positions for un-processable events, not retained objects.)

Per-rule state does **not** include reader watermarks. Watermarks belong to the shared reader, not to individual rules — see below.

**Cluster reader state** lives at `/etc/s3/lifecycle/_reader/`. **A single reader task** (`s3.lifecycle.read`, locked by one cluster-wide lock) is subscribed to **one** filer endpoint at a time. That endpoint is just a routing convenience: `Filer.CollectLogFileRefs(start_position, stop_ts)` returns chunk refs from per-filer log files in directory order, and the worker heap-merges them client-side. The single subscription is for connection management; cross-filer event ordering happens in the worker.

Files:

- `reader_state` — protobuf, single record (no reader groups):
  - `primary_filer_endpoint` — current subscription target. Workers fail over to another filer if this one is unreachable.
  - `last_processed_original: map<delay_group_seconds, map<filer_id, MessagePosition>>` — per delay group, per per-filer log shard.
  - `last_processed_predicate: map<filer_id, MessagePosition>` — per per-filer shard.
  - `tail_drained_streams: set<TailDrainedStreamKey>` — stream-specific markers for departed shards already observed at their retained tail. Keys are `(ORIGINAL, delay_seconds, filer_id)` or `(PREDICATE, filer_id)`, never just `filer_id`; one delay group being drained does not prove another delay group is safe. The marker is cleared when that stream is lazily seeded again after the shard reappears.
  - `last_engine_compiled_at_ns: int64`.
- `blockers` — durable per-shard blocker records. When a deterministic per-event failure (or a sustained transient one) is observed, the worker pauses that shard at its current `MessagePosition` and records a `BlockerRecord` here; the cursor does **not** advance. Operators inspect via `weed shell s3.lifecycle.blockers list` and resolve manually (`retry`, `resume`, or `quarantine`). See "Blocked cursor handling" below. There is no automatic dead-letter — for lifecycle, silently routing a failed event aside would mean losing a delete decision; the design pauses instead.
- `retry_budget` — durable per-stream retry counter file. Tracks `consecutive_retries` for `(stream_kind, key)` tuples that produce repeated `RETRY_LATER`. Compacts on success; promotes to a `BlockerRecord` once the configured budget trips. See "Sustained-RETRY_LATER promotion" below.

**Why per-filer cursors are unavoidable.** The persisted log is partitioned by filer (`<HH-MM>.<filerId>` files). The client-side heap merge produces a single ordered stream from N per-filer streams, but resuming after a partial-batch failure requires knowing each per-filer stream's position — `LogEntry.Offset` is per-buffer (per filerId), not globally unique. A single `(ts, offset)` cursor across the merged stream would conflate offsets from different filers. Therefore:

- The cursor type per delay group is `map<filer_id, MessagePosition>`.
- `MessagePosition = {ts_ns, offset}` (the existing log-buffer type, `weed/util/log_buffer/log_read.go:38`) is the per-shard tie-breaker.
- The worker maintains the heap merge over per-shard `MessagePosition`s; on resume, each filer's stream restarts at its own cursor position.
- Watermark exposed to operators / metrics is the low-water-mark: `min over filer_ids of last_processed.ts_ns` per delay group. Lag = `now - watermark`.

This is more state than the previous "single subscription, single cursor" claim implied, but it's what correctness requires across a client-side multi-filer merge. The single subscription still gives connection-level simplicity (one filer endpoint, easy failover); the per-filer cursor map is internal bookkeeping.

**Cursor-skip semantics.** `last_processed[filer_id]` points at the *last resolved event* for that shard. Resume must skip events with position `<= cursor[filer_id]` (using the `(ts, offset)` order per shard) — strict `<=`, not `<`, otherwise the last processed event is replayed on every resume. Equivalently: deliver events with `(event.ts, event.offset) > cursor[filer_id]`. Phase 3 introduces a new API variant that takes per-shard `MessagePosition`s and applies this comparator — see "API change" below. The existing `pb.ReadLogFileRefs` filter `logEntry.TsNs <= startTsNs` (`weed/pb/filer_pb_direct_read.go:333`) is the wrong shape because it can't disambiguate equal-ts events.

**Required API addition** (Phase 3 dependency): `pb.ReadLogFileRefsWithPosition` extending the current `ReadLogFileRefs`:

- Take a per-shard `start_positions: map<filer_id, MessagePosition>` instead of a single `startTsNs`.
- Per-event callback signature `eachLogEntry(event, filer_id, position) → CallbackResult`.
- Skip predicate uses `(ts, offset)` ordering per shard, not `TsNs <= startTsNs`.
- Return per-shard `last_positions: map<filer_id, MessagePosition>`.

`CallbackResult` (explicit enum):

```
enum CallbackResult {
    Continue                      // proceed to next event
    HaltAll                       // stop merge entirely; return last_positions for ALL shards
    PauseShard(filer_id)          // stop delivering events from THIS shard for the rest of this call
                                  // (other shards continue); subsequent merge ticks within this call
                                  // never invoke the callback again for filer_id
}
```

`last_positions` semantics on return:

- For shards that returned `Continue` until exhaustion: `last_positions[filer_id]` = the last delivered event's position.
- For shards paused via `PauseShard(filer_id)`: `last_positions[filer_id]` = the position of the event whose callback returned `PauseShard` (the *paused-on* event, not the next un-delivered one). The lifecycle reader does **not** advance the cursor in this case — the BlockerRecord already records the failing position, and the cursor stays at the last *resolved* position recorded by the previous successful callback for that shard.
- For shards untouched (no events delivered before the call ended): `last_positions[filer_id]` = the value passed in via `start_positions[filer_id]` (unchanged).

`HaltAll` exits the function immediately; `last_positions` reflects the state up to the point of halt, with the halting event's shard's `last_positions` entry equal to the position that returned `HaltAll`.

No `afterChunk` hook. Under client-side heap merge, a chunk's events are interleaved with events from other shards, so "chunk read complete" does not mean "all events from this chunk delivered and resolved." A hook firing on chunk-read-completion could checkpoint past unresolved events. Checkpointing happens only inside the per-event callback (see "Periodic checkpointing" in the reader pseudocode below).

The existing `pb.ReadLogFileRefs` stays unchanged for other consumers; the new variant is additive.

Why one reader, one subscription:

- **Watermark correctness is well-defined.** Each per-filer cursor is its own monotonic stream; the heap merge produces a single ordered delivery; cursors persist per shard so resume after partial failure is correct on each shard. Watermark surfaced externally is the low-water-mark across the per-filer cursors per delay group.
- **Filer load is metadata-only.** `CollectLogFileRefs` returns chunk file IDs and their time ranges — no event bodies are deserialized at the filer. Worker reads bodies from volume servers, which parallelizes naturally across the chunks of the merged stream.
- **No partitioning, no epochs, no migration.** Adding parallelism is not a primary scaling story; if a single worker can't keep up after raising in-worker chunk-read concurrency and `lifecycle.delete.concurrency`, the next-stage answer is segment-level intra-stream parallelism (see "Future scaling" below). Explicitly **non-v1**.

**Flush-safety lag.** The filer's persisted log files appear on disk after `LogFlushInterval` (current constant: `time.Minute` — `weed/filer/filer.go:33`). The worker reads only events at `event.ts <= now - flushSafetyLag` (default `2 × LogFlushInterval`, so **≈2 minutes** with current defaults; reduces if `LogFlushInterval` is shortened) so a late-flushing filer cannot produce events older than the cursor after the cursor has advanced. All cutoffs are clamped: `effectiveCutoff = min(kindCutoff, now - flushSafetyLag)`. Phase 0 confirms the active value of `LogFlushInterval` and pins `flushSafetyLag` to it; the design treats `flushSafetyLag` as derived, not as a hardcoded number.

**Primary filer failover.** Workers reach the primary filer via a standard filer client; on connection failure, fall back to the next healthy filer endpoint (discovered via the master). The cursor is portable — every filer reads the same persisted logs from the shared namespace, so the worker can resume against a different primary without state loss.

Per-bucket bootstrap progress (used by bucket-level bootstrap tasks):

- `/etc/s3/lifecycle/<bucket>/_bootstrap` — protobuf: `last_scanned_path`, `bootstrap_started_at_ns`, the engine snapshot ID it was launched against. One writer (the bucket's bootstrap task).

Why `/etc/...` rather than inside the bucket: keeps system state out of bucket listings without needing list-time filters; matches existing convention (`/etc/seaweedfs/`, `/etc/...`).

`rule_hash` is `sha256(canonicalize(rule))[:8]` over a length-prefixed canonical form (sorted tag map, prefix verbatim, every action's parameters — days/date/count/flags, filter fields). Length prefixing prevents delimiter forgery between adjacent fields. Stable across reorder and resilient to empty or duplicate `Rule.ID`. Prefix `"logs"` and `"logs/"` hash differently because they match different objects under literal `strings.HasPrefix` semantics.

**Policy CAS is per-rule via the `rule_hash` directory layout.** Pending items live under `<rule_hash>/pending`; the drain pass and the `LifecycleDelete` server check whether `rule_hash` is still in the current policy's hash set. Edits to other rules don't affect this rule's pending. See "Policy-version CAS" section below for full mechanics.

### Tick (default 24h, configurable, jittered, per bucket)

For each bucket with `lifecycle.xml`:

1. Parse XML; compute `rule_hash` per rule. Expand each rule into its compiled actions (one per populated action element); compute `(rule_hash, action_kind)` keys.
2. Reconcile per-action `state` files under `/etc/s3/lifecycle/<bucket>/<rule_hash_hex>/<action_kind>/`: drop directories for `(rule_hash, action_kind)` pairs no longer present (after a grace period to absorb policy edit-and-revert); create directories for new pairs (`bootstrap_complete=false`).
3. Per **compiled action** (not per rule), decide mode in this order:
   - `action_kind == EXPIRATION_DATE` → `mode = SCAN_AT_DATE`. Detector schedules a single bucket-level bootstrap at the action's `rule.date`. No per-object pending. No reader involvement.
   - Else compute `eventLogHorizon(rule, action_kind)` — the age of an event the reader needs to be able to observe for *this specific action*:
     - `EXPIRATION_DAYS`     → `rule.Days`              (age-origin events at `now - days`)
     - `NONCURRENT_DAYS`     → `rule.NoncurrentDays`    (version-flip events at `now - days`)
     - `ABORT_MPU`           → `rule.DaysAfterInitiation` (MPU init events at `now - days`)
     - `NEWER_NONCURRENT`    → `smallDelay`             (count-based; reader observes version-flip events near `now - smallDelay`)
     - `EXPIRED_DELETE_MARKER` → `smallDelay`           (immediate; reader observes the marker creation event near `now - smallDelay`)
     If `metaLogRetention < eventLogHorizon(rule, action_kind) + bootstrapLookbackMin` → this action's `mode = scan_only` (with `degraded_reason = RETENTION_BELOW_HORIZON`). Event-driven would silently miss events that aged out of the log. Sibling actions on the same XML rule are evaluated independently — a 90d `EXPIRATION_DAYS` action may degrade to `scan_only` while its 7d `ABORT_MPU` sibling stays `event_driven`.
   - Else if `bootstrap_complete == false` → run bootstrap (the same walker; on completion this action transitions to its target mode).
   - Otherwise → `event_driven`.
4. Run `event_driven` actions: trigger pass, then drain `pending`.
5. Run `scan_only` actions: re-run bootstrap on the per-kind safety-scan cadence (see "Safety-scan cadence per rule kind" table). Date and count kinds don't have a TTL-style `MinTriggerAge`; their cadences are defined explicitly in that table. Bootstrap is idempotent; this is the same code path.

`metaLogRetention` is read from cluster config (Phase 0 confirms the knob). If unconfigured, default to a conservative value (e.g. 30d) and require operators to extend it explicitly for longer-TTL rules.

### Rule kinds and `dueAt`

Each rule kind defines `dueAt(rule, live)` — the earliest wall-clock time the entry becomes eligible. The trigger pass uses a kind-specific cutoff to bound work; eligibility is always checked against `dueAt`.

| Rule kind | Mode | `dueAt(rule, live)` | Original-write trigger cutoff | Predicate-change trigger cutoff |
|---|---|---|---|---|
| `EXPIRATION_DAYS` | `event_driven` | `live.mtime + days` | `now - days` | `now - smallDelay` |
| `EXPIRATION_DATE` | `scan_at_date` | n/a (single date-triggered sweep) | n/a | n/a |
| `NONCURRENT_DAYS` | `event_driven` | `successorMtime + days` | `now - days` (version-flip events) | `now - smallDelay` |
| `NEWER_NONCURRENT` | `event_driven` | `now` (count-based, immediate at flip) | `now - smallDelay` | `now - smallDelay` |
| `ABORT_MPU` | `event_driven` | `mpuInitMtime + days` | `now - days` (MPU init events) | n/a |
| `EXPIRED_DELETE_MARKER` | `event_driven` | `marker.mtime` (immediate when sole survivor) | `now - smallDelay` | n/a |

`smallDelay` is a single global, default 1 minute, just to avoid racing in-flight writes.

**Safety-scan cadence per rule kind.** A periodic re-bootstrap catches drift the reader cannot recover. Cadence depends on rule kind because `MinTriggerAge` is undefined for some:

| Rule kind | Safety-scan cadence (default) |
|---|---|
| `EXPIRATION_DAYS` | `max(MinTriggerAge, 7d)` capped at 30d |
| `NONCURRENT_DAYS` | `max(MinTriggerAge, 7d)` capped at 30d |
| `ABORT_MPU` | `max(MinTriggerAge, 1d)` |
| `EXPIRATION_DATE` | `24h` (no `MinTriggerAge`; daily scan picks up new objects matching the prefix) |
| `NEWER_NONCURRENT` | `7d` (count-based; periodic catches missed version-flip events) |
| `EXPIRED_DELETE_MARKER` | `7d` |

Operators can override per rule via shell command. `scan_only` mode (when retention < trigger window) uses the same cadence as the safety scan but it's the *only* enforcement path.

The trigger pass uses two cutoffs **per rule** (not one):

- **Age-origin events** (the "original-write" sweep covers all of these — the term "original-write" is a slight misnomer kept for continuity): cutoff = kind's "age-origin" column. These represent any event that establishes a new "object age" for AWS-semantic purposes (`LastModified` reset). Concretely:
  - `OldEntry == nil && NewEntry != nil` — fresh PUT.
  - `OldEntry != nil && NewEntry != nil` AND any of `Mtime`, `FileSize`, or chunk fileIds differ — overwrite or content rewrite.
  - MPU init events (initialize a new upload, not yet completed).
  An overwrite resets the lifecycle clock; the engine must re-evaluate. If we excluded overwrites from this stream, a 60d rule on a 30d-old object that was just overwritten would still see the original PUT event (already past the watermark) and never re-trigger; the rewritten content would be deleted at the original PUT's age + 60d, not the rewrite's age + 60d.
- **Predicate-change-only events** (`OldEntry != nil && NewEntry != nil` AND `Mtime/FileSize/chunks unchanged` AND `Extended` differs): cutoff = `now - smallDelay`. These represent tag/metadata edits that can change rule applicability *without* resetting the lifecycle clock. We must look at them quickly so a tag added at age 30d on a 60d rule doesn't wait until age 90d. `dueAt` is computed from `live.mtime + TTL`, so the object's eligibility instant is unchanged — but it gets on the radar at the right time. Events where both content and Extended changed are classified as age-origin, not predicate-change.

Events whose `OldEntry.Extended` and `NewEntry.Extended` are byte-identical (and no other rule-relevant fields changed) are filtered out at ingestion — most internal `UpdateEntry` traffic is irrelevant.

### Bootstrap — bucket-level, inline-delete

One walk per bucket evaluates every applicable rule per object via the engine's bucket index. Currently-due objects are deleted **inline** (no per-object pending writes); not-yet-due entries are skipped (the reader picks them up via the original-write watermark sweep). The walker checkpoints `last_scanned_path` per page in `/etc/s3/lifecycle/<bucket>/_bootstrap`; on `TRANSPORT_ERROR` it stops and the next bootstrap task resumes from the cursor.

```
T_start := now()
snapshot := engine.Snapshot()

// Refuse to start if a BOOTSTRAP blocker is active for this bucket — operator
// must clear it via blockers retry|resume|quarantine first. Without this guard
// the walker would re-attempt the same failing entry every tick and the cursor
// could advance past the blocked path if the underlying issue self-heals,
// without the blocker ever being explicitly resolved.
active_blockers := load("/etc/s3/lifecycle/_reader/blockers")
if any b in active_blockers where b.stream_kind == BOOTSTRAP and b.bucket == bucket:
    return ContinuationHint{ next_run_after_ns: now + 60s, reason: "BOOTSTRAP blocker active" }

for each entry under /buckets/<bucket>/ starting from last_scanned_path:
    if entry is internal (.uploads/, .versions/ for non-versioned, ...): continue
    candidates := snapshot.MatchPath(bucket, entry.path)
    if len(candidates) == 0: continue
    info := buildObjectInfo(entry)
    for each rule in candidates:
        if rule.kind == EXPIRATION_DATE && rule.mode == SCAN_AT_DATE && now() < rule.date:
            continue   // wait for the scheduled date-triggered bootstrap
        if !ruleAppliesToEntryShape(rule, entry): continue
        if now() < computeDueAt(rule, info):
            continue   // not-yet-due: reader's original-write sweep will pick this up later
        action := s3lifecycle.EvaluateAction(rule, action_kind, info, now())
        // Use the same deleteAndResolve contract as the reader/drain paths.
        // Outcomes: DONE/NOOP_RESOLVED → walker advances; RETRY_LATER → walker
        // halts at this entry, last_scanned_path stays at the previous successful
        // entry, next bootstrap task resumes there; BLOCKED → blocker record was
        // written and the walker halts (operator unblocks).
        outcome := deleteAndResolve(DeleteContext{
            bucket:            bucket,
            object_path:       entry.path,
            version_id:        entry.version_id,
            rule_hash:         rule.hash,
            rule:              rule,
            stream_kind:       BOOTSTRAP,
            shard:             "",                  // bootstrap is not shard-bound
            delay_seconds:     0,                   // unused for BOOTSTRAP
            position:          MessagePosition{},   // unused for BOOTSTRAP
            action:            action,
            expected_identity: identityFromLive(entry),
        })
        stream_key := StreamKey{ stream_kind: BOOTSTRAP, bootstrap: BootstrapKey{
            bucket: bucket, object_path: entry.path,
            version_id: entry.version_id, rule_hash: rule.hash } }
        retry_target := RetryTarget{
            key:        stream_key,
            bucket:     bucket,
            object_path: entry.path,
            version_id: entry.version_id,
            rule_hash:  rule.hash,
        }
        switch outcome {
        case DONE, NOOP_RESOLVED:
            clearRetryBudget(stream_key)
            continue
        case RETRY_LATER:
            promoted := recordRetryLater(retry_target)
            if promoted == BLOCKED:
                return ErrBootstrapBlocked        // promotion wrote BlockerRecord; halt and surface
            return ErrBootstrapRetryLater         // halt; resume from last_scanned_path next task
        case BLOCKED:
            clearRetryBudget(stream_key)
            return ErrBootstrapBlocked            // halt; operator must clear blocker
        }
    checkpoint last_scanned_path = entry.path     // only after DONE/NOOP_RESOLVED for every rule

// Walk completed for this snapshot. Commit cursor seed/rewind FIRST, then per-action
// completion. Order matters: engine.compile(action) keys activation off
// state[ActionKey].bootstrap_complete && state[ActionKey].mode == EVENT_DRIVEN;
// if either flips before the cursor seed commits, the engine could mark an action
// active while its cursor is still uninitialized.

// Compute target modes UPFRONT, per ActionKey, so the seeder can filter on them.
// The seeder looks at target_mode to decide whether to rewind a delay group's
// cursor for this action (only EVENT_DRIVEN actions participate in reader sweeps
// and need the cursor floor). Computing modes after seeding would leave the
// seeder reading whatever default value the state field has — wrong.
target_modes := {}
for each ActionKey k in snapshot.action_keys_for_bucket(bucket):       // expanded per-action
    target_modes[k] = decideMode(snapshot.actions[k])

newly_completed := { k for k in snapshot.action_keys_for_bucket(bucket)
                       where state[k].bootstrap_complete == false }

seedReaderCursorsForNewDelayGroups(snapshot, newly_completed, target_modes, T_start)
                                                                          // (1) durable cluster write

// (2) durable per-action writes — bootstrap_complete=true AND mode are committed
// atomically per ActionKey (single proto write per action), using the SAME
// target_modes computed upfront. Activation needs both: engine.compile activates
// an action iff bootstrap_complete && mode == EVENT_DRIVEN.
for each ActionKey k in snapshot.action_keys_for_bucket(bucket):
    state[k].mode                       = target_modes[k]
    state[k].bootstrap_started_at_ns    = T_start
    state[k].bootstrap_completed_at_ns  = now()
    state[k].bootstrap_complete         = true                            // committed AFTER (1)

engine.markActive(snapshot, action_keys_for_bucket)                       // (3) in-memory hint
remove _bootstrap (last_scanned_path) for this bucket
```

`decideMode(action)` is the same predicate used by the tick-time mode decision (date kind → `SCAN_AT_DATE`; reader-driven kind with `metaLogRetention < eventLogHorizon(rule, kind) + bootstrapLookbackMin` → `SCAN_ONLY`; rule explicitly disabled by operator → `DISABLED`; otherwise `EVENT_DRIVEN`). Computing it once at bootstrap completion and persisting it durably means subsequent engine refreshes don't have to recompute or guess. Computing it **before** seeding (rather than inside the per-action loop) lets the seeder see consistent target modes for every `ActionKey` it must consider.

Note the `newly_completed` filter passed to `seedReaderCursorsForNewDelayGroups`: rewind only applies to actions transitioning `bootstrap_complete=false → true`. Routine safety-scan re-bootstraps for already-active actions do **not** rewind shared cursors. See "Cursor seeding/rewind" below for the rationale.

**State transitions, explicit.** Bootstrap is bucket-level execution; completion is per-action:
- `_bootstrap` (per bucket): only the run cursor (`last_scanned_path`, `bootstrap_started_at_ns`, `snapshot_id`). Removed on completion.
- `state[ActionKey].bootstrap_complete` (per action): set to true for every `ActionKey` in the snapshot when the bucket walk finishes. Actions added after the walk started keep `bootstrap_complete=false` and trigger a new bucket walk on the next detector tick.
- `engine.markActive(ActionKey)` (in-memory): the action becomes a candidate in reader sweeps only after both (1) and (2) durable writes have committed for that key.

Bootstrap **never writes to per-action pending** in the normal path. Pending exists only for late-predicate-change exceptions, which are observed by the reader, not by bootstrap.

**Policy change during bootstrap.** Bootstrap binds to a `snapshot_id`. If lifecycle XML changes mid-walk:
- The current walk finishes (it's evaluating against the old snapshot, which is consistent with what the reader's cursor floors expect).
- The engine compiles a new snapshot. New `ActionKey`s start `pending_bootstrap`; unchanged `ActionKey`s carry their `bootstrap_complete=true`. (An XML edit that adds a new action sub-element to an existing rule introduces a new `ActionKey` for that kind only; the rule's other actions stay `active`.)
- Detector emits a new bucket bootstrap task on the next tick for the new snapshot. It walks again from the beginning (idempotent — inline deletes are CAS-protected).
- Removed `ActionKey`s' directories enter grace cleanup. Removing an action sub-element from an XML rule removes only that `ActionKey`; siblings under the same `rule_hash` survive.

**Cursor seeding/rewind on bootstrap completion.** Bootstrap must touch the shared cursors for the **actions** that just transitioned `bootstrap_complete=false → true` — not every action in the snapshot, and not on every safety-scan re-bootstrap. Scoping by `newly_completed` `ActionKey`s is critical: an action joining an existing shared delay group needs the shared cursor pulled back to its safe floor (the cursor reflects work done for sibling actions and has advanced past the new action's floor), but routine safety scans for already-active actions must not rewind that same cursor — doing so would force every safety-scan tick to replay big shared windows.

Rewind rule (Option B):

- For each newly-completing `ActionKey` whose `targetOriginalDelayGroup(action)` is non-nil (computed from `action_kind`/delay, *not* from `engine.originalDelayGroups` membership — that collection only contains actions already activated, which excludes the action we're now completing): set `reader_state.last_processed_original[D.seconds][filer_id] = min(existing_cursor, MessagePosition{T_start - max(D, bootstrapLookbackMin), BEFORE_FIRST_OFFSET})` for every shard. The `targetOriginalDelayGroup` filter covers `EXPIRATION_DAYS`, `NONCURRENT_DAYS`, `ABORT_MPU`, `NEWER_NONCURRENT`, `EXPIRED_DELETE_MARKER`, and any future event-driven kind. `EXPIRATION_DATE` returns nil and is skipped. Comparison is the lex `(ts, offset)` order — same shape as the strict-`<=` skip predicate — not bare ts; otherwise an existing cursor at `(T, k)` with `k > BEFORE_FIRST_OFFSET` and `T == new_floor.ts_ns` would be left in place even though the new action's floor sits at `(T, BEFORE_FIRST_OFFSET)` and an event at `(T, 0)` should be delivered.
- For event-driven actions with effectively-zero delay (`NEWER_NONCURRENT`, `EXPIRED_DELETE_MARKER`): the delay group is `0` (or `smallDelay`); the cursor floor is `T_start - bootstrapLookbackMin`. Same min-comparison and rewind logic apply.
- For predicate-sensitive actions (those whose rule has tag/size filters): also `min(existing_cursor, MessagePosition{T_start - bootstrapLookbackMin, BEFORE_FIRST_OFFSET})` on `last_processed_predicate[filer_id]`.

Sibling actions already in the same delay group will replay the rewind window. That replay is idempotent — pending upserts collapse on `(path, version_id)`, CAS on identity rejects redundant deletes — but operators should know it costs them work proportional to events in the rewind window. The reseed shell command and the bootstrap completion path both surface this in the status output.

When a bucket bootstrap completes, it calls `seedReaderCursorsForNewDelayGroups(snapshot, newly_completed, target_modes, T_start, force_reseed_delays={}, force_reseed_predicate=false)`. The `newly_completed` argument is a set of `ActionKey`s, restricting rewind scope to actions transitioning `bootstrap_complete=false → true`. Routine safety-scan re-bootstraps for already-active actions pass empty `newly_completed`, empty `force_reseed_delays`, and `force_reseed_predicate=false`, so they don't rewind cursors.

The optional **`force_reseed_delays`** and **`force_reseed_predicate`** parameters together form the **operator-driven scan_only re-enable path**. When `s3.lifecycle.reseed` flips an action from `scan_only` back to `event_driven`, the action already has `bootstrap_complete=true`, so it does not enter `newly_completed`. Without explicit force arguments, the seeder would skip it and the previously-deleted/stale cursors would never get re-seeded. The reseed shell command:

1. Computes `force_reseed_delays = { D : D = targetOriginalDelayGroup(action), for action in affected_actions where target_mode == EVENT_DRIVEN }`.
2. Sets `force_reseed_predicate = any action in affected_actions where action.predicateSensitive`. The predicate cursor is **shared across the whole engine** (one `map<filer_id, MessagePosition>`), so the seeder operates on it whenever any predicate-sensitive action is being re-enabled — independent of which delay group the action belongs to. While the action was in `scan_only`, predicate-change events for matching tags/sizes were not fed to the engine; without rewind, the action would silently miss any tag/metadata change that happened during the `scan_only` window.
3. Deletes `reader_state.last_processed_original[D.seconds]` for each `D` in `force_reseed_delays`. If `force_reseed_predicate` is true and the operator wants a hard reseed of the predicate cursor, also deletes `reader_state.last_processed_predicate` (otherwise the seeder will pull it back via `min(existing, new_floor)`, which is sufficient).
4. Runs a fresh bucket bootstrap that calls the seeder with the populated force arguments. The seeder treats them as needing seeding/rewind even when `newly_completed` is empty.
5. After the seeder commits, flips affected rules' `mode` from `scan_only` back to `event_driven` durably.

Without this scoping, every safety scan would silently rewind every shared cursor and force unrelated rules to repeatedly replay big windows; without the force-delay path, scan_only re-enable would be silently broken because the deleted delay-cursor never gets re-seeded; without the force-predicate path, predicate-sensitive rules re-enabling from scan_only would silently miss tag/metadata changes that occurred during the scan_only window.

```
seedReaderCursorsForNewDelayGroups(snapshot, newly_completed, target_modes, T_start,
                                   force_reseed_delays={}, force_reseed_predicate=false):
    if newly_completed is empty AND force_reseed_delays is empty AND !force_reseed_predicate:
        return     // safety-scan re-bootstrap; no rewind, no force-seed

    acquire cluster lock /etc/s3/lifecycle/_reader/seeding.lock
    rs := load("/etc/s3/lifecycle/_reader/reader_state")

    // Derive affected delay groups from each newly-completing rule's TARGET
    // delay group (computed from rule.kind/delay), NOT from engine.originalDelayGroups
    // membership: that collection only includes rules with bootstrap_complete &&
    // mode == EVENT_DRIVEN, so a rule that is *just now completing* is not yet a
    // member when this seeding runs.
    //
    // target_modes is the precomputed map<rule_hash, RuleMode> from the caller.
    //
    // targetOriginalDelayGroup(rule) returns the delay-group key the rule would
    // join if event-driven:
    //   EXPIRATION_DAYS / NONCURRENT_DAYS / ABORT_MPU      → rule.delay
    //   NEWER_NONCURRENT / EXPIRED_DELETE_MARKER           → smallDelay (delay = 0)
    //   EXPIRATION_DATE                                    → nil (not reader-driven)
    affected_delays := {}
    affects_predicate := false

    // Path 1: rules transitioning bootstrap_complete=false → true.
    for rule in snapshot.rules where rule.hash in newly_completed:
        if target_modes[rule.hash] != EVENT_DRIVEN:         continue   // SCAN_*/DISABLED skip rewind
        if D := targetOriginalDelayGroup(rule); D != nil:
            affected_delays.add(D)
        if rule.predicateSensitive:
            affects_predicate = true

    // Path 2: operator-driven scan_only re-enable. Force-seed each delay in
    // force_reseed_delays even if no rule is in newly_completed. This is the
    // only path that re-seeds an already-deleted cursor for an already-active rule.
    for D in force_reseed_delays:
        affected_delays.add(D)

    // Predicate cursor: rewound if any newly_completed predicate-sensitive rule
    // OR if force_reseed_predicate is set. The latter handles scan_only re-enable
    // for predicate-sensitive rules: while the rule was scan_only, predicate-change
    // events weren't applied; without rewind we'd silently miss any tag/metadata
    // change from that window.
    if force_reseed_predicate:
        affects_predicate = true

    known_filer_ids    := list_filer_ids_in("/topics/.system/log/")
    floor_ts_for(D)    := T_start - max(D, bootstrapLookbackMin)
    predicate_floor_ts := T_start - bootstrapLookbackMin

    for each delay D in affected_delays:
        if rs.last_processed_original[D.seconds] is unset:
            rs.last_processed_original[D.seconds] = {}
        for filer_id in known_filer_ids:
            new_floor := MessagePosition{ ts_ns: floor_ts_for(D), offset: BEFORE_FIRST_OFFSET }
            existing  := rs.last_processed_original[D.seconds][filer_id]
            if existing is unset:
                rs.last_processed_original[D.seconds][filer_id] = new_floor
            else if compareMessagePosition(new_floor, existing) < 0:
                // REWIND. compareMessagePosition is the lex (ts, offset) comparator —
                // matching the strict <= skip semantics. Bare-ts comparison would
                // miss boundary events when timestamps are equal but offsets differ.
                rs.last_processed_original[D.seconds][filer_id] = new_floor

    if affects_predicate:
        if rs.last_processed_predicate is unset:
            rs.last_processed_predicate = {}
        for filer_id in known_filer_ids:
            new_floor := MessagePosition{ ts_ns: predicate_floor_ts, offset: BEFORE_FIRST_OFFSET }
            existing  := rs.last_processed_predicate[filer_id]
            if existing is unset or compareMessagePosition(new_floor, existing) < 0:
                rs.last_processed_predicate[filer_id] = new_floor

save rs
release seeding.lock
```

Anchored to `T_start` (when the walk began), not `T_completed`. Replaying the past `D` window catches objects bootstrap missed under walk-cursor drift. Pre-existing delay-group / filer-id entries are pulled back if the new rule's safe floor is earlier than the current cursor; never advanced past their existing position. Sibling rules in the same delay group replay the rewind window — that's a known cost of cursor sharing; surfaced in the bootstrap-completion status.

**New filer shards** discovered during steady-state operation (a filer joins the cluster, or a chunk ref names a `filer_id` not yet in `last_processed_*`) are seeded by the reader at task start via `seedNewlyDiscoveredShardsLazily` (see reader pseudocode). For each newly-discovered `filer_id`, the cursor floor is set to `min(earliest_retained_log_position[filer_id].ts_ns, safe_floor_ts)` with `offset = BEFORE_FIRST_OFFSET`, and any matching `tail_drained_streams` marker is cleared because this is a new retained-log epoch for that stream, where:

- `earliest_retained_log_position[filer_id]` is read once via a filer probe (single RPC: `EarliestRetainedPositionPerShard`).
- `safe_floor_ts` is the kind-specific floor: `T_now - max(D, bootstrapLookbackMin)` for age-origin sweeps, `T_now - bootstrapLookbackMin` for predicate.

Picking the *earlier* of the two ensures no still-retained event can be skipped: if the earliest retained log for that shard predates the safe floor, the cursor sits at that earlier position so the events are delivered. The `BEFORE_FIRST_OFFSET` sentinel ensures the earliest event itself isn't filtered out by the strict `<=` skip predicate.

**Safety scan.** `SeaweedList` is not snapshot-isolated; an entry whose original-write event is older than `D` may be missed by the walk and the reader's replay alike. Defense: a periodic re-bootstrap on event-driven rules at the per-kind cadence (default `max(MinTriggerAge, 7d)` capped at 30d for age rules; 24h for date rules; 7d for count rules). Re-running is idempotent.

**Date-based rules** (`EXPIRATION_DATE`): no per-object pending. `mode = SCAN_AT_DATE`. The detector schedules a single bucket-level bootstrap at `rule.date`; that bootstrap deletes all matches inline. No reader involvement for date rules.

Before `rule.date`, no work happens. New objects PUT during the wait period don't need handling — the date-triggered bootstrap evaluates current state. Tag changes are likewise reflected. After `rule.date`, the bootstrap sweeps everything in one pass via inline `LifecycleDelete` calls (same as any other bootstrap); no per-object pending is written.

Bootstrap is checkpointable: persist `last_scanned_path` after each page. On worker death, the next bootstrap task resumes from there.

### Compiled policy engine

A naive design — per-rule reader tasks, each scanning the meta log filtered to its bucket prefix — multiplies log I/O by `N_rules` and event-evaluation by `N_events × N_rules`. Wrong shape.

The right shape: **one shared cluster-level reader, one compiled policy engine, one event read = one router lookup**. All rules across all buckets are evaluated by routing each event through the engine; rules with the same trigger delay share their cutoff sweep.

**ActionKey is the engine's primary identity.** Every per-action data structure — engine indexes, target modes, newly-completed sets, bootstrap completion, drain/locks/metrics, status — is keyed by `ActionKey{rule_hash, action_kind}`, not by `rule_hash` alone. A single XML rule with two action sub-elements appears as **two** `ActionKey`s through the entire pipeline: separate delay group memberships, separate cursors, separate pending files, separate locks, separate completion bits. Sibling actions can complete bootstrap on different schedules and degrade independently.

```go
type ActionKey struct {
    RuleHash   [8]byte
    ActionKind ActionKind
}
```

**Engine shape** (rebuilt periodically and on lifecycle PUT/DELETE):

```
type Engine struct {
    snapshot_id   uint64                                 // monotonic; bumps on every rebuild

    // Per-bucket prefix trie -> candidate ActionKeys.
    buckets       map[bucket]*BucketIndex

    // Trigger-delay groups. Original-write events for these (rule, action)
    // pairs become eligible at the same cutoff; the reader sweeps each group
    // in one pass. Only ActionKeys with mode = EVENT_DRIVEN AND
    // bootstrap_complete are included here — engine.compile filters before
    // adding to these collections so the reader cannot route events to
    // scan_only / scan_at_date / disabled / pending actions.
    originalDelayGroups map[time.Duration][]ActionKey   // e.g. 7d -> [{rA, ABORT_MPU}], 90d -> [{rA, EXPIRATION_DAYS}, {rB, NONCURRENT_DAYS}]

    // Predicate-change actions — single near-now sweep, no group needed.
    // Same EVENT_DRIVEN + bootstrap_complete filter.
    predicateActions    []ActionKey

    // Date-driven actions — handled by SCAN_AT_DATE bootstrap, not the reader.
    dateActions         map[ActionKey]time.Time

    // Definitions (for evaluation).
    actions       map[ActionKey]*CompiledAction
}

type BucketIndex struct {
    prefixTrie  *PrefixTrie                              // path prefix -> []ActionKey
    tagIndex    map[tagKey]map[tagValue][]ActionKey      // optional: speeds up tag-filter actions
    versioned   bool
}

type CompiledAction struct {
    raw         *s3lifecycle.Rule    // shared with sibling actions of the same XML rule
    key         ActionKey            // (rule_hash, action_kind)
    bucket      string
    delay       time.Duration        // MinTriggerAge(rule, kind) for age kinds; zero for date/count
    predicateSensitive bool           // true if the rule's filter has tag/size predicates
    mode        RuleMode              // EVENT_DRIVEN | SCAN_AT_DATE | SCAN_ONLY | DISABLED
                                       // Mirrored from durable state[ActionKey].mode at compile time;
                                       // engine.compile uses it to decide whether to register the
                                       // action in originalDelayGroups / predicateActions.
}
```

The engine is built once per worker process from all bucket lifecycle xml; rebuilt on policy change events observed in the meta log itself. The `snapshot_id` stamps each evaluation; pending items recorded under one snapshot are still valid as long as their `ActionKey` survives in the next snapshot.

**Two-phase action activation.** When the engine compiles a new `ActionKey` from a freshly-PUT lifecycle XML, the action starts in state `pending_bootstrap` and is **excluded from reader sweeps**. Only after the bucket's bootstrap task runs and seeds the cluster cursors for that action's delay group does the action transition to `active`. Activation is per `ActionKey`, not per rule — a rule's 7d `ABORT_MPU` action can become active before its 90d `EXPIRATION_DAYS` sibling finishes bootstrap. Concretely:

```
engine.compile(bucket, rule, kind):                    // called once per (rule, kind) pair
    key := ActionKey{rule.hash, kind}
    // Activation requires BOTH:
    //   (a) bootstrap_complete (durable; cursor seed already committed before this).
    //   (b) mode allows event-driven processing.
    // The reader only routes events to actions with mode = EVENT_DRIVEN. SCAN_AT_DATE
    // actions are handled by their date-triggered bootstrap; SCAN_ONLY actions are
    // handled by periodic safety-scan bootstraps; DISABLED actions do nothing.
    // Each CompiledAction carries its mode so reader matching can filter.
    if state[key].bootstrap_complete && state[key].mode == EVENT_DRIVEN:
        action.engine_state = active
    else:
        action.engine_state = inactive   // pending_bootstrap, scan_only, scan_at_date, or disabled

reader.MatchOriginalWrite/MatchPredicateChange/MatchPath:
    return only ActionKeys with engine_state == active

bootstrap.complete (REQUIRED ORDER — same as the main bootstrap pseudocode above):
    target_modes := { key: decideMode(snapshot.actions[key])
                      for key in snapshot.action_keys_for_bucket }
    seedReaderCursorsForNewDelayGroups(snapshot, newly_completed, target_modes, T_start)
                                                                                // (1) durable cluster
    // (2) per-action durable writes — mode AND bootstrap_complete committed atomically per ActionKey.
    for key in newly_completed:
        state[key].mode               = target_modes[key]
        state[key].bootstrap_complete = true                                    // AFTER (1)
        engine.markActive(key)                                                  // (3) in-memory hint
```

The transition order matters: cursor seed must commit **before** the per-action durable write that sets both `mode` and `bootstrap_complete=true`. If the engine refreshes between (1) and (2) for some key, it sees `bootstrap_complete=false` and keeps that action `pending_bootstrap` — correct. If the engine refreshes between (2) and (3), it sees `bootstrap_complete=true && mode == EVENT_DRIVEN` and activates the action — also correct, because (1) already committed. The in-memory `markActive` step is only an optimization to avoid waiting for the next engine refresh; it does not gate correctness. Writing `mode` and `bootstrap_complete` in a single proto write per action (atomic at the file level) avoids any window where one is set and the other isn't.

Policy changes during a bucket bootstrap: the bootstrap is bound to a specific `snapshot_id` (recorded in `_bootstrap`). If the policy changes mid-walk, the in-progress bootstrap finishes for its snapshot; the new policy compiles a fresh engine snapshot; new/changed `ActionKey`s start in `pending_bootstrap`; the next detector tick emits a fresh bucket bootstrap for the new snapshot. `ActionKey`s whose `(rule_hash, action_kind)` is unchanged carry over their `bootstrap_complete=true` and stay `active`. An XML edit that adds a new action sub-element to an existing rule produces a new `ActionKey` (same rule_hash, new action_kind) that goes through `pending_bootstrap` while the rule's other actions stay active.

### Reader — single subscription, client-side merge

The reader is one cluster-singleton task subscribed to one filer at a time. `Filer.CollectLogFileRefs(start_position, stop_ts)` returns chunk refs from log files in directory order. The worker passes those refs to `pb.ReadLogFileRefsWithPosition` (the new API — see Task #19), which heap-merges per-filer chunks by event ts client-side and applies the per-shard `(ts, offset)` skip filter. Chunk *bodies* are read in parallel from volume servers via the returned fileIds.

Cursors are per-filer-shard. `last_processed_original[D.seconds]` is `map<filer_id, MessagePosition>`; `MessagePosition = {ts_ns, offset}` disambiguates equal-ts events within a per-filer chunk. Resume skips events with `(event.ts, event.offset) <= cursor[filer_id]` — strict `<=` so the last resolved event isn't replayed.

```
reader_state  := load("/etc/s3/lifecycle/_reader/reader_state")
primary_filer := pickHealthyFiler(reader_state.primary_filer_endpoint)
flush_lag     := 2 * LogFlushInterval     // currently ~2 minutes; derived from filer config

// One probe per task entry: which per-filer log shards exist, what's the earliest
// retained position on each, and what's the latest? `earliest` is used by
// lazy-seeding to safely floor new shards' cursors; `latest` is used for tail-drain
// GC immediately below.
retained_range  := primary_filer.RetainedLogRangePerShard()   // map<filer_id, {earliest, latest}>
earliest_retained := { fid: r.earliest for fid, r in retained_range }

// TAIL-DRAIN / LOST-LOG GC. Run BEFORE blocker load and seeding so the rest of
// the pass operates on the post-GC cursor map (no stale departed-filer entries
// pinning low_water_ts to ancient timestamps; correctly-degraded streams when
// logs were pruned ahead of the cursor).
//
// For each cursor entry (delay group D, filer_id F) — and analogously for the
// predicate cursor map:
//
//   range := retained_range[F]
//
//   case A — F has a retained range AND cursor[F] >= range.latest:
//     The shard is tail-drained. Cursor caught up to (or past) the last
//     retained event for F; no further events from F can arrive without
//     reseeding via lazy-seeding when F reappears. Safe to delete cursor[F].
//
//   case B — F has a retained range AND cursor[F] < range.latest:
//     Not tail-drained. Keep cursor[F]; the regular pass will continue draining.
//
//   case C — F has NO retained range AND we have a durable
//   `tail_drained_streams[stream_key]` marker (recorded the last time we observed
//   case A and pruned this exact stream cursor): safe to skip — F's logs were pruned
//   after this stream cursor caught up. No action; cursor was already removed in
//   case A's pass.
//
//   case D — F has NO retained range AND no `tail_drained_streams[stream_key]`
//   marker AND cursor[F] is present: lost-log scenario. The shard's logs
//   were pruned before the cursor reached `range.latest` — events were
//   silently dropped. We must not GC the cursor as if it were safe; doing so
//   would erase the only signal that those events ever existed.
//     1. Mark every reader-driven rule as `degraded_reason = LOST_LOG` and
//        downgrade `mode = scan_only` until the next safety scan re-establishes
//        correctness. The log shard is global metadata history; once it is pruned,
//        we cannot prove which buckets were unaffected.
//     2. Surface `lifecycle_lost_log_total{filer_id}` and
//        `lifecycle_degraded_streams{reason="LOST_LOG"}` metrics; emit a
//        warning log including F and the cursor position.
//     3. Leave cursor[F] in place (don't GC) until an operator runs
//        `s3.lifecycle.reseed -ack-lost-log --reason <text>`, which clears
//        lost-log cursor entries and the degraded flag together. Deleting silently is
//        forbidden — the operator must explicitly acknowledge the data loss.
//
// `tail_drained_streams` is a small durable set in `reader_state` updated
// each time a stream cursor is removed under case A, so case C can distinguish
// "we already GC'd this exact stream safely" from "logs vanished without this
// stream catching up." The key is `(ORIGINAL, D, F)` or `(PREDICATE, F)`.
gcDepartedShards(reader_state, retained_range)
save reader_state                                    // persist GC results before reads

// LOAD ACTIVE BLOCKERS once at task entry. The reader uses this set to skip
// blocked (shard, stream, [delay]) tuples without re-reading their events
// each batch. Without this, a stuck shard would pull events repeatedly across
// every tick — wasted I/O and noisy "blocked again" log spam.
active_blockers := load("/etc/s3/lifecycle/_reader/blockers")
blocked_original := { (b.shard, b.delay_seconds) for b in active_blockers
                                                  where b.stream_kind == ORIGINAL }
blocked_predicate := { b.shard for b in active_blockers
                                where b.stream_kind == PREDICATE }

// SEED FIRST, PERSIST, THEN READ.
for each delay group D in engine.originalDelayGroups:
    seedNewlyDiscoveredShardsLazily(
        reader_state.last_processed_original[D.seconds],
        /*kind=*/ ageOrigin(D), earliest_retained,
        reader_state.tail_drained_streams, now)
seedNewlyDiscoveredShardsLazily(
    reader_state.last_processed_predicate,
    /*kind=*/ predicateChange, earliest_retained,
    reader_state.tail_drained_streams, now)
save reader_state                                    // durable seed before any reads

// Periodic checkpoint: persist reader_state every checkpointEveryNEvents (default 1000)
// or checkpointEveryT (default 5s), whichever first.

// Pass A: age-origin events, one sweep per delay group.
for each delay group D in engine.originalDelayGroups:
    effective_cutoff := min(now - D, now - flush_lag)
    starts := reader_state.last_processed_original[D.seconds]    // already seeded, persisted

    // Build the per-shard start positions to pass into the API. For shards with
    // an active blocker, we pass MaxMessagePosition so ReadLogFileRefsWithPosition
    // delivers nothing for them — the shard is paused at its previous cursor.
    pass_starts := {}
    for filer_id, pos in starts:
        if (filer_id, D.seconds) in blocked_original:
            pass_starts[filer_id] = MaxMessagePosition       // skips all events for this shard
        else:
            pass_starts[filer_id] = pos

    // Listing covers shards we will actually process. We compute the low-water
    // ONLY across non-blocked shards.
    active_shards := { fid for fid in starts if (fid, D.seconds) not in blocked_original }
    if active_shards is empty:                              // every shard for this delay is blocked
        continue                                            // skip to next delay group
    low_water_ts := min over fid in active_shards: starts[fid].ts_ns
    chunkRefs    := primary_filer.CollectLogFileRefs(start_position: ts_only(low_water_ts),
                                                     stop_ts: effective_cutoff)

    halt_batch := false
    pb.ReadLogFileRefsWithPosition(
        chunkRefs,
        readerFn,
        start_positions: pass_starts,
        stop_ts:         effective_cutoff,
        eachLogEntry(event, filer_id, position):
            // First-statement guard: if this (shard, delay) was blocked earlier
            // in THIS batch by a sibling event, the merge engine may still
            // deliver later events from the same shard before our pauseShard()
            // call propagates. Drop them without mutating the cursor — the
            // cursor must stay at the position recorded in the BlockerRecord.
            if (filer_id, D.seconds) in blocked_original:
                return continueLoop                              // no cursor update
            if !event.isOriginalWrite:
                reader_state.last_processed_original[D.seconds][filer_id] = position
                periodicCheckpoint()
                return continueLoop
            candidates := engine.MatchOriginalWrite(event, delayGroup=D)
            if len(candidates) == 0:
                reader_state.last_processed_original[D.seconds][filer_id] = position
                periodicCheckpoint()
                return continueLoop
            // handleEvent owns the retry-budget interaction (recordRetryLater /
            // clearRetryBudget) for the exact failing branch, so the reader
            // callback only sees the final outcome. handleEvent already returned
            // BLOCKED if its own recordRetryLater call promoted internally.
            outcome := handleEvent(event, candidates,
                                   /*stream_kind=*/ ORIGINAL, /*shard=*/ filer_id,
                                   /*delay_seconds=*/ D.seconds, /*position=*/ position)
            switch outcome:
                case DONE, NOOP_RESOLVED:
                    reader_state.last_processed_original[D.seconds][filer_id] = position
                    periodicCheckpoint()
                    return continueLoop
                case RETRY_LATER:
                    save reader_state
                    halt_batch = true
                    return halt
                case BLOCKED:
                    blocked_original.add((filer_id, D.seconds))
                    save reader_state
                    return pauseShard(filer_id)
    )
    save reader_state
    if halt_batch: return ContinuationHint{ next_run_after_ns: now + 60s, reason: "RETRY_LATER" }

// Pass B: predicate-change events.
predicate_cutoff := now - max(smallDelay, flush_lag)
starts := reader_state.last_processed_predicate                    // already seeded

pass_starts := {}
for filer_id, pos in starts:
    if filer_id in blocked_predicate:
        pass_starts[filer_id] = MaxMessagePosition        // skip all events for this shard
    else:
        pass_starts[filer_id] = pos

active_shards := { fid for fid in starts if fid not in blocked_predicate }
if active_shards is not empty:
    low_water_ts := min over fid in active_shards: starts[fid].ts_ns
    chunkRefs    := primary_filer.CollectLogFileRefs(start_position: ts_only(low_water_ts),
                                                     stop_ts: predicate_cutoff)

    halt_batch := false
    pb.ReadLogFileRefsWithPosition(
        chunkRefs, readerFn,
        start_positions: pass_starts, stop_ts: predicate_cutoff,
        eachLogEntry(event, filer_id, position):
            // First-statement guard: same-batch BLOCKED protection.
            if filer_id in blocked_predicate:
                return continueLoop                              // no cursor update
            if !event.isPredicateChange:
                reader_state.last_processed_predicate[filer_id] = position
                periodicCheckpoint()
                return continueLoop
            candidates := engine.MatchPredicateChange(event)
            if len(candidates) == 0:
                reader_state.last_processed_predicate[filer_id] = position
                periodicCheckpoint()
                return continueLoop
            // handleEvent owns retry-budget calls; reader callback only acts on outcome.
            outcome := handleEvent(event, candidates,
                                   /*stream_kind=*/ PREDICATE, /*shard=*/ filer_id,
                                   /*delay_seconds=*/ 0, /*position=*/ position)
            switch outcome:
                case DONE, NOOP_RESOLVED:
                    reader_state.last_processed_predicate[filer_id] = position
                    periodicCheckpoint()
                    return continueLoop
                case RETRY_LATER:
                    save reader_state
                    halt_batch = true
                    return halt
                case BLOCKED:
                    blocked_predicate.add(filer_id)
                    save reader_state
                    return pauseShard(filer_id)
    )
    save reader_state
    if halt_batch: return ContinuationHint{ next_run_after_ns: now + 60s, reason: "RETRY_LATER" }
```

`periodicCheckpoint` is `if eventCount % checkpointEveryNEvents == 0 || timeSinceLastSave > checkpointEveryT { save reader_state }`. It batches per-event mutations into chunked durable writes; on crash, redo is bounded by the configured budget. Defaults: 1000 events or 5s.

Notes on the listing call: `CollectLogFileRefs` accepts a single `MessagePosition` start. We pass the **low-water-mark ts across all per-filer cursors** so the listing returns chunks covering every shard's still-unread tail. `ReadLogFileRefsWithPosition` then applies the per-shard `start_positions` filter so each shard only delivers events strictly past its own cursor. This means we may stream a few already-processed events from shards that are ahead, but the per-shard filter drops them before the callback.

`seedNewlyDiscoveredShardsLazily` handles the case where a chunk ref names a `filer_id` we've never seen before. The caller passes in the precomputed `earliest_per_shard` (probed once at task entry) so the helper does not re-issue the RPC:

```
seedNewlyDiscoveredShardsLazily(cursors, kind, earliest_per_shard, tail_drained_streams, now):
    // earliest_per_shard: map<filer_id, MessagePosition>, from
    //   primary_filer.EarliestRetainedPositionPerShard() probed once per task.
    // kind:               ageOrigin(D) or predicateChange.
    // now:                wall clock at task start (for the safe floor).

    for filer_id in earliest_per_shard:
        if filer_id not in cursors:
            // Floor at the earlier of (a) the earliest retained log for this shard,
            // and (b) the kind-specific safe floor (now - max(kind.delay, bootstrapLookbackMin)
            // for age-origin; now - bootstrapLookbackMin for predicate). The EARLIER
            // of the two ensures retained events that predate the safe floor are
            // delivered; BEFORE_FIRST_OFFSET ensures the earliest event itself is
            // not filtered out by the strict <= skip predicate.
            safe_floor_ts := safeFloorTsFor(kind, now)
            shard_floor   := min(earliest_per_shard[filer_id].ts_ns, safe_floor_ts)
            cursors[filer_id] = MessagePosition{ ts_ns: shard_floor, offset: BEFORE_FIRST_OFFSET }
            delete tail_drained_streams[tailDrainedStreamKey(kind, filer_id)]
```

The probe is a single filer RPC at task start, cheap. Passing the result in (rather than re-probing inside the helper) means a single task only issues one `EarliestRetainedPositionPerShard` call no matter how many delay groups call the seeder.

Properties:

- **Cursors are per-filer-shard**; cursor state per delay group is `map<filer_id, MessagePosition>`. Resume after partial failure is correct on each shard independently.
- **Listing uses the low-water-mark ts**; per-shard filtering inside the new API. Single subscription endpoint; per-shard correctness.
- **Watermark exposed externally** is the low-water-mark across per-filer cursors per delay group. Lag = `now - watermark`.
- **Cost ≈ events × distinct_delay_groups** for age-origin passes; the per-delay sweeps each pull a time window from the merged stream. Real configs have a handful of distinct day values.
- For configs with many distinct delays (rare), the engine should optionally **merge nearby delay groups** into bins to bound the number of passes.
- **Filer is not the bottleneck** — it serves chunk metadata only. Volume reads parallelize per chunk.
- **Failover** moves the worker to a different `primary_filer_endpoint`; per-filer cursor map is portable because every filer reads the same persisted logs from the shared namespace.

If a single worker cannot keep up after raising in-worker chunk-read concurrency and `lifecycle.delete.concurrency`, the next-stage answer is segment-level intra-stream parallelism (see "Future scaling" — explicitly non-v1).

`handleEvent(event, candidates, stream_kind, shard, delay_seconds, position) → outcome`:

Single contract. Fetch the live entry once, evaluate every candidate rule against it, return the aggregate outcome. The caller passes the stream context (`stream_kind`, `shard`, `delay_seconds`, `position`) that `handleEvent` forwards to `DeleteContext` and any `BlockerRecord` writes. Reader passes `ORIGINAL` or `PREDICATE` plus `(filer_id, D.seconds, position)` or `(filer_id, 0, position)` respectively; bootstrap and pending drain construct `DeleteContext` directly without going through `handleEvent`.

The cursor (the per-shard `MessagePosition` for the event's `filer_id`) only advances past this event when **every** candidate returns `DONE`/`NOOP_RESOLVED`. If any candidate returns `RETRY_LATER` or `BLOCKED`, that outcome propagates up and the caller halts (or pauses the shard) per the reader pseudocode.

```
handleEvent(event, candidates, stream_kind, shard, delay_seconds, position):
    // handleEvent owns its retry-budget interactions. Building a RetryTarget
    // outside (in the reader callback) would have to guess "which candidate
    // failed" — wrong if the failure is fetchLive ERR (no candidates evaluated
    // yet) or an early candidate (not the last one). Doing it here means the
    // RetryTarget reflects the actual failing branch.
    stream_key := buildStreamKey(stream_kind, shard, delay_seconds, position)
    base_target := RetryTarget{
        key:         stream_key,
        bucket:      event.bucket,
        object_path: event.path,
        version_id:  extractVersionId(event.path),    // best effort; refined when live is available
        // rule_hash filled in per branch below (nil before first candidate evaluation)
    }

    // fetchLiveEntry is tri-state. Conflating not-found with errors would either
    // advance the cursor over un-evaluated events (transport blip) or stall on a
    // legitimately-deleted entry. Distinguish:
    //   FOUND     → proceed.
    //   NOT_FOUND → object is gone (live state = absent). Resolved.
    //   ERR       → transient transport/RPC failure → RETRY_LATER + retry budget.
    //   FATAL     → deterministic decode/schema failure → BLOCKED. writeBlocker.
    live, status := fetchLiveEntry(event.path)
    switch status:
        case NOT_FOUND:
            clearRetryBudget(stream_key)
            return NOOP_RESOLVED
        case ERR:
            return recordRetryLater(base_target)         // returns RETRY_LATER or promoted BLOCKED
        case FATAL:
            // Persistent failure. Persist the blocker durably BEFORE returning
            // BLOCKED — otherwise the cursor would pause without a record for
            // the operator to inspect or clear, and the lag-fallback would
            // eventually demote silently. If the durable write itself fails,
            // return RETRY_LATER and let the next batch retry; the cursor stays.
            ok := writeBlocker(BlockerRecord{
                stream_kind:      stream_kind,    // ORIGINAL or PREDICATE for reader callers
                shard:            shard,
                delay_seconds:    delay_seconds,  // 0 for PREDICATE
                position:         position,
                rule_hash:        nil,            // unset — failure is pre-evaluation, not bound to a rule
                bucket:           event.bucket,
                object_path:      event.path,
                version_id:       extractVersionId(event.path), // may be ""
                reason:           "fetch_live_FATAL",
                last_error:       reason,
                first_seen_at_ns: now(),
                last_retry_at_ns: now(),
                retry_count:      1,
            })
            if !ok: return RETRY_LATER            // blocker write failed; treat like outage
            // Clear any retry_budget entry that may have accumulated from prior
            // transient ERRs at this same position. Without this, a stream that
            // bounced through ERR → ERR → FATAL would leave a stale budget entry
            // that confuses subsequent budget accounting.
            clearRetryBudget(stream_key)
            return BLOCKED
        case FOUND:                               // continue
    info := buildObjectInfo(live)

    aggregate := NOOP_RESOLVED                    // start from "nothing required"
    for each rule in candidates:
        if !ruleAppliesToEntryShape(rule, live):  continue
        action := s3lifecycle.EvaluateAction(rule, action_kind, info, now)
        if action == ActionNone:                  continue
        dueAt := computeDueAt(rule, info)

        var per_rule_outcome eventOutcome
        ctx := DeleteContext{
            bucket:            event.bucket,
            object_path:       event.path,
            version_id:        live.version_id,
            rule_hash:         rule.hash,
            rule:              rule,
            stream_kind:       stream_kind,        // ORIGINAL or PREDICATE — passed in by the reader pass
            shard:             shard,               // == event.filer_id when called from reader
            delay_seconds:     delay_seconds,       // populated only when stream_kind == ORIGINAL
            position:          event.position,
            action:            action,
            expected_identity: identityFromLive(live),
        }
        if event.isOriginalWrite():
            // event.ts crossed the original cutoff, but live.mtime may be newer
            // (overwrite resets the clock). The overwrite produces its own
            // age-origin event; this older event is stale.
            if now < dueAt:
                per_rule_outcome = NOOP_RESOLVED
            else:
                per_rule_outcome = deleteAndResolve(ctx)
        else if event.isPredicateChange():
            if now >= dueAt:
                per_rule_outcome = deleteAndResolve(ctx)
            else:
                // Late predicate change made the object newly match, but mtime+TTL
                // is in the future. The original-write event is already past the
                // watermark, so we'd miss this without an exception record.
                err := upsertPending(rule, PendingItem{
                    path: event.path, version_id: extractVersionId(...),
                    due_at_ns: dueAt.UnixNano(),
                    expected_identity: identityFromLive(live),
                })
                if err == ErrPendingFull or err == ErrPendingWriteFailed:
                    // Demote the rule to scan_only durably. If the demote also
                    // fails (filer IO unhealthy), feed the retry budget for THIS
                    // rule's slot and return — sustained demote-failures will
                    // promote to BLOCKED.
                    if !durably_demote_rule_to_scan_only(rule.hash, reason: err):
                        rule_target := base_target
                        rule_target.rule_hash = rule.hash
                        return recordRetryLater(rule_target)
                    counter.pending_full_total{rule_hash}++; log_warn(...)
                    per_rule_outcome = NOOP_RESOLVED
                else:
                    per_rule_outcome = NOOP_RESOLVED   // exception recorded in pending
        else:
            per_rule_outcome = NOOP_RESOLVED       // unknown event class

        if per_rule_outcome == RETRY_LATER:
            // deleteAndResolve produced RETRY_LATER for THIS specific candidate
            // rule. Feed the retry budget with a target that reflects the
            // actual failing branch (this rule's hash + this rule's action).
            rule_target := base_target
            rule_target.rule_hash = rule.hash
            return recordRetryLater(rule_target)
        if per_rule_outcome == BLOCKED:
            // deleteAndResolve already wrote a BlockerRecord with this rule's
            // hash. Clear any retry budget that was tracking earlier transient
            // failures for this stream key.
            clearRetryBudget(stream_key)
            return BLOCKED
        aggregate = max(aggregate, per_rule_outcome)   // DONE > NOOP_RESOLVED for telemetry

    // All candidates returned DONE/NOOP_RESOLVED. Clear any in-flight retry
    // budget for this stream key — success at this position resets the budget.
    clearRetryBudget(stream_key)
    return aggregate                              // safe to advance cursor past event.ts
```

**Processing outcomes (single contract used by reader, bootstrap walker, and pending drain):**

| Outcome | Cursor advance? | Meaning |
|---|---|---|
| `DONE` | yes | Action resolved (delete succeeded, marker created, abort completed). |
| `NOOP_RESOLVED` | yes | Object already deleted / changed / no longer applicable / object-lock skipped. No further work needed. |
| `RETRY_LATER` | **no** | Transient failure (S3 unreachable, network blip, filer IO error). The batch halts at this position; the next task retries from the same `MessagePosition`. |
| `BLOCKED` | **no** | Deterministic or persistent failure (`FATAL_EVENT_ERROR`, malformed entry, `TRANSPORT_ERROR` exceeded its retry budget). The shard is paused and a `BlockerRecord` is written. The cursor stays at the failing position until an operator intervenes. |

**No automatic dead-lettering.** A dead letter would mean "this record is poison, skip it so the worker can keep moving." For lifecycle that's wrong: skipping a record means losing a delete decision (correctness). The worker pauses instead and surfaces the blocker. Operators decide whether to `retry`, `resume`, or explicitly `quarantine`.

`deleteAndResolve`:

```
type StreamKind enum { ORIGINAL = 0; PREDICATE = 1; BOOTSTRAP = 2; PENDING = 3; }

type DeleteContext struct {
    // Identifying:
    bucket            string
    object_path       string
    version_id        string          // optional; "" for non-versioned
    rule_hash         bytes
    rule              *CompiledRule

    // Stream / shard / position context (used by BlockerRecord scoping):
    stream_kind       StreamKind      // ORIGINAL | PREDICATE for reader; BOOTSTRAP / PENDING otherwise
    shard             string          // filer_id; "" for BOOTSTRAP / PENDING (not shard-bound)
    delay_seconds     int64           // populated when stream_kind == ORIGINAL; 0 otherwise
    position          MessagePosition // populated when stream_kind == ORIGINAL or PREDICATE

    // What to do:
    action            Action          // CURRENT | VERSION | DELETE_MARKER | ABORT_MPU
    expected_identity EntryIdentity   // CAS
}

deleteAndResolve(ctx DeleteContext):
    // K in-memory retries against the same event for FATAL_EVENT_ERROR / worker-local
    // deterministic errors. TRANSPORT_ERROR is NEVER counted here — it returns
    // RETRY_LATER immediately. The caller feeds the durable retry_budget for this
    // stream key; sustained TRANSPORT_ERROR promotes to BLOCKED, while watermark-lag
    // fallback remains the retention-safety backstop.
    for attempt in 1..K (default 5):
        outcome := lifecycleDeleteRPC(ctx.bucket, ctx.object_path, ctx.version_id,
                                      ctx.action, ctx.expected_identity)
        switch outcome {
        case DELETED:
            return DONE
        case NOT_FOUND, STALE_IDENTITY, STALE_POLICY, NO_LONGER_ELIGIBLE:
            return NOOP_RESOLVED
        case SKIPPED_OBJECT_LOCK:
            counter.skipped_object_lock_total++; log_warn(...); return NOOP_RESOLVED
        case TRANSPORT_ERROR:
            return RETRY_LATER                 // batch halts; cursor stays
        case FATAL_EVENT_ERROR:
            // retry deterministically; if still FATAL after K attempts, fall through to BLOCKED
            continue
        }

    // K in-memory retries exhausted. The event is deterministically un-processable.
    // Write a BlockerRecord scoped by stream_kind so the right cursor pauses; only
    // return BLOCKED after the durable write commits. If the write itself fails
    // (filer IO unhealthy), return RETRY_LATER instead — without a durable
    // blocker, BLOCKED would silently halt the stream with nothing for the
    // operator to inspect.
    ok := writeBlocker(BlockerRecord{
        stream_kind:      ctx.stream_kind,
        shard:            ctx.shard,
        delay_seconds:    ctx.delay_seconds,    // populated only for ORIGINAL
        position:         ctx.position,         // populated for ORIGINAL/PREDICATE
        rule_hash:        ctx.rule_hash,
        bucket:           ctx.bucket,
        object_path:      ctx.object_path,
        version_id:       ctx.version_id,
        reason:           "FATAL_EVENT_ERROR after K retries",
        last_error:       last_outcome.reason,
        first_seen_at_ns: now(),
        last_retry_at_ns: now(),
        retry_count:      K,
    })
    if !ok: return RETRY_LATER                  // blocker write failed; treat like outage
    // Clear any retry_budget entry that earlier RETRY_LATER attempts at this
    // same key may have left; promotion to BLOCKED via FATAL is a different
    // path from sustained-RETRY_LATER promotion, but they share the stream key.
    clearRetryBudget(buildStreamKeyFromCtx(ctx))
    return BLOCKED
```

The function takes a single `DeleteContext` because four callers (reader-original, reader-predicate, bootstrap walker, pending drain) all populate the same shape. The `stream_kind` field tells `writeBlocker` which cursor to pause: ORIGINAL pauses `(shard, delay_seconds)`; PREDICATE pauses `shard` predicate stream; BOOTSTRAP pauses the bucket walker; PENDING pauses the rule's drain.

**Watermark advance, per the outcome enum above.** The per-shard cursor `last_processed_*[filer_id]` advances past an event whose `handleEvent` returns `DONE` or `NOOP_RESOLVED`. It does **not** advance on `RETRY_LATER` (transient — next batch retries) or `BLOCKED` (persistent — operator intervention required). The meta log itself is the durable queue; the cursor's persisted position **is** the retry pointer.

`drainPending` runs per `(bucket, rule_hash)`. At task entry it loads `_reader/blockers` and refuses to drain if a `PENDING` blocker is active for this rule:

```
// Task-start guard: if a PENDING blocker is active for this (bucket, rule_hash),
// don't drain. Operator must clear via blockers retry|resume|quarantine.
active_blockers := load("/etc/s3/lifecycle/_reader/blockers")
if any b in active_blockers where b.stream_kind == PENDING
                                  and b.bucket == bucket
                                  and b.rule_hash == rule.hash:
    return ContinuationHint{ next_run_after_ns: now + 60s, reason: "PENDING blocker active" }

// Per-item processing.
// Build the stream key + retry target BEFORE fetchLive, so that an ERR can
// participate in the retry budget. Without this, repeated transient fetchLive
// failures for the same pending item would never promote to BLOCKED.
stream_key := StreamKey{ stream_kind: PENDING, pending: PendingKey{
    bucket: bucket, rule_hash: rule.hash,
    object_path: item.path, version_id: item.version_id } }
retry_target := RetryTarget{
    key:        stream_key,
    bucket:     bucket,
    object_path: item.path,
    version_id: item.version_id,
    rule_hash:  rule.hash,
}
// RetryTarget intentionally carries no action / expected_identity — operator-driven
// retry replays drainPending against current live state, which re-fetches and
// re-evaluates from scratch.

live, status := fetchLiveEntry(item.path)
switch status:
    case NOT_FOUND:
        clearRetryBudget(stream_key)                   // resolved (object gone)
        pending.delete(item); return
    case ERR:
        // Transient failure — feed the retry budget. Sustained ERR promotes to BLOCKED.
        promoted := recordRetryLater(retry_target)
        if promoted == BLOCKED:
            return                                     // BlockerRecord written; drain pauses
        return                                         // leave pending intact; next drain retries
    case FATAL:
        // Deterministic per-event failure. Block this rule's drain at this item;
        // skip the retry budget — FATAL is not a transient outage.
        if !writeBlocker(BlockerRecord{
            stream_kind:      PENDING,
            shard:            "",                    // PENDING is not shard-bound
            delay_seconds:    0,
            position:         MessagePosition{},     // unused for PENDING
            rule_hash:        rule.hash,
            bucket:            bucket,
            object_path:       item.path,
            version_id:        item.version_id,
            reason:            "fetch_live_FATAL",
            last_error:        reason,
            first_seen_at_ns:  now(),
            last_retry_at_ns:  now(),
            retry_count:       1,
        }): return                                   // blocker write failed; leave pending intact
        clearRetryBudget(stream_key)                  // any prior ERR-driven budget is superseded
        return                                       // pending stays; drain pauses for this rule
    case FOUND:            // continue
        clearRetryBudget(stream_key)                  // fetchLive succeeded; reset any pre-existing budget

if !identityMatch(live, item.expected_identity):
    // object moved since we deferred. The newer event already created its own pending
    // or already deleted; drop this stale entry.
    pending.delete(item); return

info  := buildObjectInfo(live)
dueAt := computeDueAt(rule, info)
if now < dueAt:
    item.due_at_ns = dueAt.UnixNano()      // mtime advanced (rare but possible via metadata update)
    pending.upsert(item); return

action := s3lifecycle.EvaluateAction(rule, action_kind, info, now)
if action == ActionNone:                   pending.delete(item); return

// Same deleteAndResolve contract as reader and bootstrap.
outcome := deleteAndResolve(DeleteContext{
    bucket:            bucket,
    object_path:       item.path,
    version_id:        item.version_id,
    rule_hash:         rule.hash,
    rule:              rule,
    stream_kind:       PENDING,
    shard:             "",                  // PENDING is not shard-bound
    delay_seconds:     0,                   // unused for PENDING
    position:          MessagePosition{},   // unused for PENDING
    action:            action,
    expected_identity: item.expected_identity,
})
switch outcome {
case DONE, NOOP_RESOLVED:
    clearRetryBudget(stream_key)
    pending.delete(item)
case RETRY_LATER:
    promoted := recordRetryLater(retry_target)
    if promoted == BLOCKED:
        // BlockerRecord already written by recordRetryLater on threshold trip.
        // Leave pending intact; drain pauses on this rule until operator clears.
        return
    // Plain transient: leave pending intact; next drain retries.
    return
case BLOCKED:
    // BlockerRecord already written by deleteAndResolve. Leave pending intact;
    // drain pauses on this rule until the operator clears the blocker.
    clearRetryBudget(stream_key)        // any in-flight retry budget is moot
    return
}
```

### Why current state, not event payload

Tags and other predicates can change after the original PUT. Binding the predicate to the event payload means a tag added at age 30d for a 60d rule isn't picked up until 60d after the tag-add — up to one TTL late. Live evaluation handles this in O(1) extra filer lookups.

### CAS identity

```
type entryIdentity struct {
    Mtime        time.Time   // entry.Attributes.Mtime
    Size         int64       // entry.Attributes.FileSize
    HeadFid      string      // entry.Chunks[0].FileId, "" if zero-byte
    ExtendedHash [8]byte     // sha256(canonicalize(entry.Extended))[:8]
}
```

`ExtendedHash` covers tags and any other Extended-stored predicate input. Without it, a tag flipped between worker evaluation and server-side delete would let the server proceed against stale eligibility.

Mtime alone is fooled by snapshot-restore. Head fid resolves it: a re-uploaded object lands on a fresh fid.

### Policy-version CAS — per ActionKey, not per bucket

A single bucket-wide etag would invalidate this action's pending whenever any *other* rule (or sibling action) on the same bucket is edited. Wrong: an unrelated edit shouldn't dump pending work that's still valid under this action. Worse, because pending wouldn't be re-enqueued unless the object's events fire again (and many objects' relevant events are already in the past), the work would simply be lost.

Use the **`ActionKey{rule_hash, action_kind}`** as the CAS token. Pending items live under `/etc/s3/lifecycle/<bucket>/<rule_hash_hex>/<action_kind>/pending`, which already binds them to a specific (rule version, action kind) pair (the rule_hash changes when the rule's filter or any action's parameters change; the action_kind selects the sibling under that rule). On drain and on `LifecycleDelete`:

- Server reads current `lifecycle.xml`, computes the `ActionKey` set (one per populated action sub-element), and checks whether `request.ActionKey` is a member.
- Member → proceed.
- Not a member → `STALE_POLICY`. Either the rule was removed / had its definition changed (so a new `rule_hash` now exists), or the specific action sub-element was removed from an otherwise-unchanged rule. The pending item is genuinely orphaned; drop it. The new `ActionKey` (if any) will go through bootstrap and pick up still-relevant work.

Per-`ActionKey` CAS:
- An edit to rule A leaves rule B's actions untouched.
- An edit to rule A's filter or any action parameter creates a new `rule_hash`; the old `<old_hash_A>/` directory tree (with all its action sub-directories) is orphaned. The orphan is cleaned up after a grace period (so an edit-and-revert preserves progress).
- Removing one action sub-element from rule A while keeping others: only the corresponding `<rule_hash_A>/<removed_kind>/` directory becomes orphaned; siblings stay live. (`rule_hash` is computed over the full action set, so removing or adding an action sub-element produces a new `rule_hash` for the rule. Both the new and old rule_hash directory trees coexist during the grace period; the new one is bootstrapped from scratch.)
- A removal of rule A entirely: the `<hash_A>/` tree becomes orphaned, cleaned up after grace.

### Versioned buckets — corrected model

Versioning facts (verified in repo):
- A versioned object at key `foo/bar.txt` lives under directory `foo/bar.txt.versions/` (suffix `.versions`, not a `.versions` subdirectory).
- Each version is its own file inside that directory; filename encodes timestamp + version id.
- A delete marker is a version file with `Extended[ExtDeleteMarkerKey] == "true"`.
- The current-version pointer is stored as `Extended[ExtLatestVersionFileNameKey]` (and friends) on the `.versions` *directory entry* itself.

Worker handling:

| Event path shape | Meaning | Worker action |
|---|---|---|
| `<bucket>/.../<key>.versions/<versionFile>` (NewEntry, no OldEntry) | New version added (PUT or DeleteMarker). | Trigger evaluation for the previously-latest version (now non-current). Use the parent dir's `ExtLatestVersionMtimeKey` *before* this event as the successor's becoming-current time → no, simpler: when handling, list the `.versions` dir and determine the just-became-non-current version; its `successorMtime = event.TsNs`. |
| `<bucket>/.../<key>.versions` (Old/NewEntry, attrs change on the directory) | Latest pointer flipped. | Same trigger as above, redundantly; idempotent. |
| `<bucket>/.../<key>.versions/<versionFile>` where Extended has `ExtDeleteMarkerKey=true` and that file is now the only entry in the dir | A delete marker is the sole survivor. | `ExpiredObjectDeleteMarker` evaluation. |

For each versioned event, `ruleAppliesToEntryShape` filters out entries that don't match the rule's target (e.g. `Expiration.Days` rules target the current version on a non-versioned bucket; on a versioned bucket they create a new delete marker via `createDeleteMarker`). `NoncurrentVersionExpiration` targets non-current versions only.

Action dispatch on versioned buckets:
- `ActionDeleteObject` (current version of versioned bucket) → `s3a.createDeleteMarker(bucket, key)`.
- `ActionDeleteVersion` (a specific non-current version) → `s3a.deleteSpecificObjectVersion(bucket, key, versionId)`.
- `ActionExpireDeleteMarker` → `s3a.deleteSpecificObjectVersion(bucket, key, versionId)` for the marker.

### AbortIncompleteMultipartUpload

Multipart uploads live under `<bucket>/.uploads/<uploadId>/`. Treat creation events for `<uploadId>` directories under `.uploads/` as triggers; eligibility is `now - mtime > rule.AbortMPUDays`. On expiry, call the existing abort-MPU path (which removes the upload directory).

### Worker-to-S3 discovery and failover

The worker process (`weed/worker/`) connects to the admin server today (`weed/worker/worker.go:81 SendHeartbeat`). The admin server is the natural discovery point for S3 endpoints.

- **Registry.** S3 servers register their gRPC listen address with admin on startup, the same way they register HTTP today. New admin RPC `ListS3Endpoints() repeated S3Endpoint`.
- **Worker cache.** Each worker fetches the S3 endpoint list at startup and refreshes every 30s in the heartbeat path. Cache survives admin reachability blips.
- **Selection.** For each `LifecycleDelete`, pick a healthy endpoint round-robin. On RPC failure (DEADLINE_EXCEEDED, UNAVAILABLE), mark that endpoint unhealthy for 60s and retry on the next.
- **All endpoints unhealthy.** Reader and bootstrap halt at the next `LifecycleDelete` they need to make. The reader's per-shard cursor map stays at the last resolved `(filer_id → MessagePosition)` entries; the bootstrap walker's `last_scanned_path` stays at the last successful entry. The task returns `ContinuationHint{ next_run_after_ns: now + 60s }` and exits. No pending is appended for the halted batch — the meta log is the durable retry queue. No data loss; lifecycle pauses cleanly. When S3 returns, the next read/bootstrap task resumes from exactly the same per-shard cursors.
- **Co-located deployments.** When workers and S3 servers run on the same nodes, the worker can prefer its local S3 endpoint by IP affinity to keep traffic on-host.
- **S3 is not a hard dependency for the worker process to start.** The worker boots and registers; the reader and bootstrap tasks halt on first `LifecycleDelete` if no endpoint is healthy. Engine compilation and policy reconciliation continue (no S3 needed). Once S3 returns, the next scheduled read/bootstrap task picks up where the previous halted, replaying events from the stalled cursor.

### Internal service-principal delete API — concrete shape

The worker is in a separate process (`weed/worker/`) and must not depend on user IAM signatures. We add an internal gRPC method on the S3 server, callable only from authorized workers:

```
service S3InternalService {
  rpc LifecycleDelete(LifecycleDeleteRequest) returns (LifecycleDeleteResponse);
}

message EntryIdentity {
  int64 mtime_ns = 1;
  int64 size = 2;
  string head_fid = 3;
  bytes extended_hash = 4;        // sha256(canonical Extended)[:8]
}

message LifecycleDeleteRequest {
  string bucket = 1;
  string object_path = 2;          // path under bucket dir
  string version_id = 3;           // optional, for ActionDeleteVersion
  enum Action { CURRENT = 0; VERSION = 1; DELETE_MARKER = 2; ABORT_MPU = 3; }
  Action action = 4;
  EntryIdentity expected_identity = 5;
  string trigger_rule_id = 6;
  bytes  trigger_rule_hash = 7;    // server checks membership in current policy's rule-hash set
  bool   skip_chunk_delete = 8;
}

message LifecycleDeleteResponse {
  enum Outcome {
    DELETED              = 0;
    NOT_FOUND            = 1;
    STALE_IDENTITY       = 2;     // CAS failed
    STALE_POLICY         = 3;     // rule_hash no longer in current policy
    NO_LONGER_ELIGIBLE   = 4;     // server-side re-evaluation says rule no longer applies
    SKIPPED_OBJECT_LOCK  = 5;     // object under legal hold or retain-until — skip silently
    TRANSPORT_ERROR      = 6;     // transient (network, RPC); retry by stalling watermark
    FATAL_EVENT_ERROR    = 7;     // deterministic per-event failure (corrupted entry, internal panic);
                                  // server's best attempt at distinguishing transient from permanent
  }
  Outcome outcome = 1;
  string  reason = 2;             // human-readable detail; populated for FATAL_EVENT_ERROR and TRANSPORT_ERROR
}
```

**Worker-local error classifications.** Some failures occur entirely on the worker side and never reach the server: malformed event payload that fails to deserialize, identity construction failure, evaluator panic recovered via `defer recover()`. These are not RPC outcomes — they're worker errors. The worker treats them under the same K-retry-then-`BLOCKED` rule as `FATAL_EVENT_ERROR` (5 in-memory retries, then write a `BlockerRecord` and pause the shard). Server-side `FATAL_EVENT_ERROR` is for cases the server detects (e.g., dispatch helper returns an error not classifiable as transport or object-lock).

Server handler (`weed/s3api/s3api_internal_lifecycle.go`, new file):

1. Authn: shared HMAC token from cluster config or mTLS — same scheme used by other intra-cluster RPCs in `weed/security/`.
2. Re-fetch live entry and verify `expected_identity` matches in full (mtime, size, head fid, extended hash). Mismatch → `STALE_IDENTITY`.
3. Re-fetch bucket lifecycle xml; compute the rule-hash set; if `request.trigger_rule_hash` is not a member → `STALE_POLICY`. (Per-rule CAS, not per-bucket. Other rules' edits don't poison this rule's deletes.)
4. Re-evaluate the rule against the live entry. If `Evaluate(rule, info, now) == ActionNone` → `NO_LONGER_ELIGIBLE`. (Defence in depth: tags or filters could have just changed.)
5. **Object lock**: explicitly call `s3a.enforceObjectLockProtections(bucket, object, versionId, governanceBypassAllowed=false)` (defined in `weed/s3api/s3api_object_retention.go:570`). The existing low-level helpers (`deleteUnversionedObjectWithClient`, `deleteSpecificObjectVersion`, `createDeleteMarker`) do **not** check object lock — the check lives in the user-facing HTTP handler (`weed/s3api/s3api_object_handlers_delete.go:225,124,147,368`) and must be reproduced here. If `enforceObjectLockProtections` returns an error, return `SKIPPED_OBJECT_LOCK`. Lifecycle never bypasses governance retention.

`enforceObjectLockProtections` currently takes `*http.Request` to extract the `x-amz-bypass-governance-retention` header; for lifecycle we hard-code `governanceBypassAllowed=false`. Phase 2 introduces a thin wrapper `enforceObjectLockForLifecycle(bucket, object, versionId)` that calls the same logic without needing a request, by either passing a synthetic request or refactoring the request-extraction step out of the core check.
6. Dispatch via the same internal helpers user deletes use:
   - `CURRENT` on non-versioned → `deleteUnversionedObjectWithClient`.
   - `CURRENT` on versioned → `createDeleteMarker`.
   - `VERSION` → `deleteSpecificObjectVersion`.
   - `DELETE_MARKER` → `deleteSpecificObjectVersion` (for the marker).
   - `ABORT_MPU` → existing abort-upload code.
7. Emit access log + Prometheus counter with principal `lifecycle:<rule_hash>:<rule_id>`.

**Worker handling per outcome:**

| Outcome | Worker action |
|---|---|
| `DELETED` | Advance cursor / drop pending item. Increment `expired_total`. |
| `NOT_FOUND` | Advance cursor / drop pending item. |
| `STALE_IDENTITY` | Advance cursor / drop pending. The newer event (if any) re-triggers via the reader. |
| `STALE_POLICY` | Advance cursor / drop pending. New rule's directory bootstraps. |
| `NO_LONGER_ELIGIBLE` | Advance cursor / drop pending. |
| `SKIPPED_OBJECT_LOCK` | **Advance cursor / drop pending. Increment `skipped_object_lock_total` counter; log at warn.** No retry, no blocked file. The next safety-scan bootstrap evaluates again; if the lock has been released, the object deletes then. |
| `TRANSPORT_ERROR` | **Halt the batch.** Cursor stays at the last resolved event. Retry on next read task. The meta log is the durable retry queue. |

This keeps replication, audit, object-lock, and metrics consistent without bypassing them and without piping the worker through the public IAM stack.

### Metadata-only deletion when chunks are on TTL volumes

When the entry's chunks were placed on TTL volumes, the volume server reclaims them wholesale on volume drop. Per-chunk `DeleteFile` RPCs are wasted work. The filer wire is already there: `Filer.DeleteChunks` honours `rule.DisableChunkDeletion` (`weed/filer/filer_deletion.go:588-594`).

The worker computes `skip_chunk_delete` per object before calling `LifecycleDelete`:

```go
ruleConf := f.FilerConf.MatchStorageRule(path)
skipChunkDelete :=
    live.Attributes.TtlSec > 0 &&    // chunks were placed on TTL volumes
    ruleConf.Ttl != ""                // path is still TTL-routed
```

The S3 server passes the flag to the filer delete path; per-chunk RPCs are skipped; the entry is removed; chunks are reclaimed on the volume's natural TTL drop. Storage reclamation lags by up to the volume's natural TTL boundary (bounded by the latest needle in that volume) — minutes-to-hours in practice, fine for lifecycle.

Versioned buckets: `createDeleteMarker` writes a marker (no chunks deleted in either path; flag is moot). `deleteSpecificObjectVersion` honours `skip_chunk_delete` for non-current versions or expired markers.

This composes with the operator-driven volume-TTL routing that replaces today's PUT back-stamp: an operator runs `weed shell s3.bucket.ttl.set -bucket X -ttl 60d` to add the `filer.conf` route. New writes flow to TTL volumes immediately. Existing entries can be back-stamped via an async maintenance task on `LaneLifecycle` (same code as today's `updateEntriesTTL`, parallelized and restartable). Once back-stamped, lifecycle deletes go through the metadata-only path.

### Scheduling

Existing maintenance scheduler in `weed/admin/maintenance/` and `weed/worker/`. The repo already anticipates this work: `weed/admin/plugin/scheduler_lane.go` defines `LaneLifecycle` (line 24), maps job type `s3_lifecycle` to it (line 83), exempts the lane from the default-lane admin lock (`laneRequiresLock[LaneLifecycle] = false`), and gives it a 5-minute idle sleep. Per-lane execution slots (`execRes` in `schedulerLaneState`) mean lanes don't starve each other for worker capacity.

Concrete plumbing:

- **Lane: `LaneLifecycle`, not `LaneDefault`.** Job-type string `s3_lifecycle` with **three subtypes** carried in params: `READ` (cluster singleton), `BOOTSTRAP` (per bucket), `DRAIN` (per `(bucket, ActionKey)`). The default lane is serialised under one admin lock and is reserved for vacuum/balance/EC/admin scripts; lifecycle must not enter it.
- Task type constants in `weed/worker/types/task_types.go`: `S3LifecycleReadTaskType`, `S3LifecycleBootstrapTaskType`, `S3LifecycleDrainTaskType`. All bound to lane `LaneLifecycle` via `jobTypeLaneMap`.
- Cluster-lock keys per subtype:
  - `READ` → `s3.lifecycle.read` (cluster singleton).
  - `BOOTSTRAP` → `s3.lifecycle.bootstrap:<bucket>` (per bucket; bootstrap walks all `ActionKey`s for the bucket in one pass).
  - `DRAIN` → `s3.lifecycle.drain:<bucket>:<rule_hash_hex>:<action_kind>` (per `ActionKey` — sibling actions of one rule have separate drain locks so their pending streams advance independently).
- Task params proto (extend `weed/pb/worker_pb/worker.proto`):
  ```
  message S3LifecycleParams {
    enum Subtype { SUBTYPE_UNSPECIFIED = 0; READ = 1; BOOTSTRAP = 2; DRAIN = 3; }
    Subtype subtype = 1;

    // bucket is required for BOOTSTRAP and DRAIN. rule_hash is optional for
    // BOOTSTRAP (omitting it means walk-all-actions-for-this-bucket) and
    // required for DRAIN. action_kind is required for DRAIN; ignored by
    // BOOTSTRAP (the walk evaluates every ActionKey per object). READ
    // ignores all three.
    string     bucket      = 2;
    bytes      rule_hash   = 3;       // 8 bytes when present
    ActionKind action_kind = 7;       // DRAIN only

    bool   force = 4;
    int64  batch_time_budget_ns = 5;
    int32  batch_event_budget = 6;
  }
  ```
- Continuation hint on task result (extend the result message returned to admin):
  ```
  message ContinuationHint {
    int64  next_run_after_ns = 1;     // 0 = ASAP, >0 = earliest re-emit
    int64  expected_work_size = 2;    // pending+events_remaining
    string prefer_worker_id = 3;      // cache-locality hint
  }
  ```
  Lane scheduler honours `next_run_after_ns` as a per-`(bucket, ActionKey)` re-arm timer for DRAIN, per-`bucket` for BOOTSTRAP; biases routing by `prefer_worker_id`. Other lanes ignore the field.
- Handler registration in `weed/worker/tasks/s3lifecycle/`. `Register()` wired from `weed/worker/worker.go`.
- Detector in `weed/admin/maintenance/`: per `LaneLifecycle` tick, list buckets, walk each per-action `state` file at `/etc/s3/lifecycle/<bucket>/<rule_hash_hex>/<action_kind>/state`, emit `read` / `bootstrap` / `drain` tasks, respecting `next_run_after_ns` and the optional off-peak window (config knob `lifecycle.run_hours`, e.g. `"01-06"`). DRAIN scheduling is per `ActionKey`; sibling actions' next-run timers are independent.
- Per-`(bucket, ActionKey)` DRAIN singleton via cluster lock `s3.lifecycle.drain:<bucket>:<rule_hash_hex>:<action_kind>`. Detector skips emitting while a lock is held.
- Manual triggers via new shell commands:
  - `weed/shell/command_s3_lifecycle_run.go` → enqueue with `force=true`.
  - `weed/shell/command_s3_lifecycle_status.go` → print state.

Why a separate lane matters here:
- Long-running batches don't take the default-lane admin lock and therefore never block vacuum/balance/EC.
- Per-lane execution reservation means lifecycle can saturate its own slot pool without starving volume management.
- Operators can dedicate workers to lifecycle by labelling capability `s3.lifecycle.scan` only on those workers.

### Multi-filer durability

`MetaAggregator` is **in-memory only** — `weed/filer/meta_aggregator.go:39`: *"MetaAggregator only aggregates data 'on the fly'. The logs are not re-persisted to disk. The old data comes from what each LocalMetadata persisted on disk."*

So the aggregator can't be the worker's source of truth: a worker restart, a crash, or a peer outage all lose unpersisted aggregated events. The persistent source is per-filer logs at `/topics/.system/log/<date>/<hour-min>.<filerId>` — written by each filer for its own events, durable, and queryable via `Filer.CollectLogFileRefs` and `Filer.ReadPersistedLogBuffer`.

Worker design for multi-filer:

- The persisted log lives in the **shared filer namespace**. Listing `/topics/.system/log/` returns chunk references for every filer's segments; reading is via `Filer.CollectLogFileRefs` and `Filer.ReadPersistedLogBuffer` against any filer endpoint. There is **no need to subscribe to each filer or run one task per filer.**
- Single cluster-singleton reader subscribed to one filer at a time. `Filer.CollectLogFileRefs` returns chunk refs in directory order; the worker heap-merges per-filer chunks client-side via `pb.ReadLogFileRefsWithPosition` (Task #19). Filer forwards metadata only — not a bottleneck.
- Cursors in `reader_state` (one writer):
  - `last_processed_original: map<delay_seconds, map<filer_id, MessagePosition>>` — per delay group, per per-filer log shard.
  - `last_processed_predicate: map<filer_id, MessagePosition>` — per per-filer shard.
  - `MessagePosition = {ts_ns, offset}` is the per-shard tie-breaker; `LogEntry.Offset` is per-buffer (per filerId), not globally unique, so cursor must track each shard.
- Cross-filer event ordering inside the heap is `(event.ts, filer_id, event.offset)`. `filer_id` is the secondary key because `event.offset` is per-filer-buffer and not globally unique; without `filer_id` in the order, two equal-ts events from different filers could swap relative positions across runs. The skip predicate stays per-shard: `(event.ts, event.offset) <= cursor[filer_id]` evaluated against that shard's own cursor.
- Filer-set discovery: list `/topics/.system/log/<date>/` and observe filerId suffixes that appear; new shards get lazy-seeded cursors via `seedNewlyDiscoveredShardsLazily`.
- **Departed-filer cursor GC and lost-log handling.** A filer is "tail-drained" for shard `F` at delay group `D` when there are **no retained log refs** for `F` with position `> reader_state.last_processed_original[D.seconds][F]` (and analogously for the predicate stream). `EarliestRetainedPositionPerShard()` is not sufficient on its own — knowing the earliest retained position doesn't prove there's nothing newer than the cursor. Phase 3 adds `RetainedLogRangePerShard()` returning `map<filer_id, {earliest, latest}>`. Tail-drained cursor entries (`cursor >= range.latest`) are removed at task entry, right after the probe, and the stream key is recorded in `reader_state.tail_drained_streams` so that future probes can distinguish "this exact stream was safely drained then pruned" from "F's logs were pruned before this stream caught up." The marker must be stream-specific: `(ORIGINAL, delay_seconds, filer_id)` or `(PREDICATE, filer_id)`. A drained 30-day stream does not make the 60-day stream safe. Without this GC, an old departed filer would pin the low-water-mark forever, forcing every batch to issue a `CollectLogFileRefs` listing starting from an ancient ts and inflating both listing cost and reader watermark-lag.

  Critically, the **absence** of a retained range for `F` is not sufficient evidence that GC is safe. If `F` has no retained range AND this stream cursor was never observed to reach `range.latest` (no entry in `tail_drained_streams`), the shard's logs were pruned before catch-up — events between `cursor[F]` and the (now-gone) `range.latest` were silently dropped. The reader **must not** GC such cursor entries: they're the only durable evidence that those events existed. Instead, the reader downgrades all reader-driven rules to `mode = scan_only` with `degraded_reason = LOST_LOG`, surfaces `lifecycle_lost_log_total{filer_id}` / `lifecycle_degraded_streams{reason="LOST_LOG"}`, and emits a warning. The cursor stays put until an operator runs `s3.lifecycle.reseed -ack-lost-log --reason <text>`, which clears the lost-log cursor entries and the degraded flag together after safety scans have covered the affected buckets — the operator must explicitly acknowledge the data loss before the system silently advances.

`LogFlushInterval` (the period before in-memory events flush to disk) bounds how stale the persistent view can be. Current constant: `time.Minute` (`weed/filer/filer.go:33`), so the flush gap is on the order of one minute. Workers process events with a `2 × LogFlushInterval` lag from real time (default ~2 minutes), so flushes always complete before the cursor reaches them.

The aggregated in-memory stream is still useful for the *bootstrap detector* (notifying the worker that a fresh policy was just PUT and a tick should run sooner than the next scheduled interval) — but it's a hint, never a substitute for the persisted log when computing watermarks or replaying history.

### Object lock and compliance — out of scope as a special case

Lifecycle does **not** model object lock state. Concretely:

- No tracking of legal-hold or retain-until-date in lifecycle storage.
- No per-rule `blocked` file (compliance ledger). No retain-until rescheduling. No re-evaluation triggered by Extended-change events that release a hold. (The cluster-level `_reader/blockers` file is unrelated — it tracks unprocessable cursor positions, not retained objects.)
- The `LifecycleDelete` server explicitly invokes `s3a.enforceObjectLockProtections` (in `weed/s3api/s3api_object_retention.go`) with `governanceBypassAllowed=false` before dispatching the low-level delete helper. If that check refuses, the server returns `SKIPPED_OBJECT_LOCK`. The worker logs, increments a counter, and advances.

What this means in practice:

- A legal-hold-released object will not be re-attempted on the original-write watermark schedule (the cursor has passed). It is re-attempted on the next periodic safety-scan bootstrap (per-kind cadence). For age-based rules that's `max(MinTriggerAge, 7d)` capped at 30d, which is acceptable for non-compliance lifecycle and irrelevant for compliance flows that should not be expecting lifecycle-driven cleanup anyway.
- A persistently-locked object surfaces as a sustained `skipped_object_lock_total` counter for that bucket/rule. Operators with compliance workloads handle removal of locks separately from lifecycle.

This is a deliberate scope cut. S3 object lock is a separate consistency surface; mixing its scheduling into lifecycle was producing complexity (legal-hold blocked file, retain-until rescheduling, re-eval on Extended change, K-retry guards, status surfacing) disproportionate to the value for typical workloads.

What remains:

- The S3 server's own object-lock enforcement is unchanged. Locked objects are not deleted.
- Lifecycle does not corrupt or interfere with object lock state.
- `skipped_object_lock_total` is exported as a Prometheus metric in Phase 7 so operators see when lifecycle and locks intersect.

If a future need arises for compliance-aware lifecycle (e.g., automated retain-until-expiry deletion), it can be a separate feature with its own data model — not bolted into the event-driven path.

### Blocked cursor handling — pause, don't dead-letter

For lifecycle, automatic dead-lettering is the wrong default. A dead letter would mean "this record is poison, skip it so the worker can keep moving" — but skipping a record means losing a delete decision (correctness). Instead, when an event cannot be processed, the worker **pauses that shard at its current `MessagePosition`** and surfaces a blocker for the operator to resolve.

Rules:

- **`RETRY_LATER`** (transient — `TRANSPORT_ERROR`, network, filer IO): the batch halts at the failing event. Cursor stays at the last resolved position. Next batch retries from the same `MessagePosition`. No `BlockerRecord` is written; the durable `retry_budget` entry for this stream key may be created or incremented (see "Sustained-RETRY_LATER promotion" below).
- **`BLOCKED`** (deterministic — `FATAL_EVENT_ERROR` after K=5 in-memory retries, or `RETRY_LATER` against the same `(stream_kind, shard, delay_seconds, position)` past the durable retry budget — see "Sustained-RETRY_LATER promotion" below): the worker writes a `BlockerRecord`, marks the affected stream paused at `failing_position`, and stops processing that stream until an operator intervenes. Other shards and other streams continue.

```
/etc/s3/lifecycle/_reader/blockers   (chunked, append-and-compact-by-stream)
  enum StreamKind { ORIGINAL = 0; PREDICATE = 1; BOOTSTRAP = 2; PENDING = 3; }

  repeated BlockerRecord {
    StreamKind stream_kind = 1;      // distinguishes the four blockable streams
    string shard           = 2;      // filer_id; populated when stream_kind == ORIGINAL or PREDICATE
    int64  delay_seconds   = 3;      // populated when stream_kind == ORIGINAL only
    MessagePosition position = 4;    // populated when stream_kind == ORIGINAL or PREDICATE

    // Common context. bucket/object_path/version_id are populated for all
    // streams. rule_hash is OPTIONAL: it is populated when the failure is
    // bound to a specific rule (the typical case). For pre-evaluation
    // failures — e.g. handleEvent's fetchLive FATAL where no rule has been
    // evaluated yet — rule_hash is left empty (nil/zero-length bytes).
    // Operators interpreting blockers should treat empty rule_hash as
    // "applies to whatever rule the next replay selects" rather than as a
    // missing field. See handleEvent fetchLive FATAL path for the one site
    // that produces this.
    bytes  rule_hash   = 5;     // optional; empty for pre-evaluation failures
    string bucket      = 6;
    string object_path = 7;
    string version_id  = 8;

    string reason       = 9;         // human-readable cause (e.g. "FATAL_EVENT_ERROR: malformed entry")
    string last_error   = 10;        // raw error string from the last attempt
    int64  first_seen_at_ns = 11;
    int64  last_retry_at_ns = 12;
    int32  retry_count      = 13;
  }
```

**StreamKind semantics:**

- `ORIGINAL`: the reader's per-shard cursor for `(shard, delay_seconds)` is paused at `position`. Subsequent ticks skip that `(shard, delay_seconds)` combination until the blocker is cleared. Other delay groups on the same shard, and the same delay group on other shards, continue.
- `PREDICATE`: the reader's per-shard predicate cursor for `shard` is paused at `position`. Other shards' predicate streams continue. Note that `delay_seconds` is not meaningful for the predicate stream — `BlockerRecord.delay_seconds` is only populated for `ORIGINAL`.
- `BOOTSTRAP`: the bucket bootstrap walker pauses; the bucket's `_bootstrap.last_scanned_path` is the resume point. The walker writes a blocker for the failing `(bucket, object_path)` and refuses to advance until cleared. Other buckets continue.
- `PENDING`: the rule's drain task pauses on the failing `(rule_hash, path, version_id)`. Other rules' drains continue.

**Operator commands:**

- `weed shell s3.lifecycle.blockers list [--stream ...] [--shard ...] [--bucket ...]` — show all blockers.
- `weed shell s3.lifecycle.blockers retry <id>` — re-attempt the failing event/path **right now**, in the shell command itself, bypassing the task-start guard. The shell synchronously re-runs the appropriate primitive (`handleEvent` for `ORIGINAL`/`PREDICATE`, the walker step for `BOOTSTRAP`, `drainPending` step for `PENDING`) against current live state. Per-stream retry-success effects:
  - `ORIGINAL`: on `DONE`/`NOOP_RESOLVED`, remove blocker record AND advance `reader_state.last_processed_original[delay_seconds][shard]` past `position`.
  - `PREDICATE`: on `DONE`/`NOOP_RESOLVED`, remove blocker record AND advance `reader_state.last_processed_predicate[shard]` past `position`.
  - `BOOTSTRAP`: on `DONE`/`NOOP_RESOLVED` for the recorded `(bucket, object_path, version_id, rule_hash)`, remove blocker record. **Do not** advance `_bootstrap.last_scanned_path` directly — the next scheduled bootstrap task picks up from `last_scanned_path` (which already sits at the previous successful entry) and re-walks normally; if the entry is still due it will be re-attempted, if not it'll be skipped. This avoids the operator command racing with concurrent walker progress.
  - `PENDING`: on `DONE`/`NOOP_RESOLVED`, remove blocker record AND tombstone the matching `PendingItem` in the rule's `pending` file (the live retry might have already deleted/marked the object). On `NOOP_RESOLVED` due to `STALE_IDENTITY`, also tombstone the pending entry — it referred to a stale identity.
  - All streams, on failure (`RETRY_LATER` or `BLOCKED` again): update `retry_count` and `last_retry_at_ns` on the blocker record; leave it in place; do not advance any cursor or tombstone any pending. The next scheduled task remains gated by the task-start guard until the blocker clears.

  Idempotency: the live re-attempt uses the same `expected_identity` / `MessagePosition` CAS path as a normal worker invocation, so a concurrent successful delete by something else just produces `NOOP_RESOLVED` (`STALE_IDENTITY` / `NOT_FOUND`) and the blocker resolves cleanly.
- `weed shell s3.lifecycle.blockers resume <id>` — clear the blocker record AND its corresponding `retry_budget` entry, but **leave the cursor at the failing position**. The next reader/drain/bootstrap pass will encounter the event and re-process it normally with a fresh retry-budget counter. Use this when the operator has fixed the underlying cause (corrupted entry repaired, upstream bug patched, etc.) and wants the worker to attempt the same record on its own schedule rather than synchronously via `retry`. Resume does *not* advance past the event — that's `quarantine`.
- `weed shell s3.lifecycle.blockers quarantine <id> --reason <text>` — manual decision to **skip the event and advance the cursor without processing it**. Clears the blocker AND its `retry_budget` entry. Logged with operator identity, reason, and a `lifecycle_quarantined_total` metric increment. This is the **only** path that intentionally drops a delete decision; only the operator can take it; the action is auditable.

In all three resolution paths (`retry` success, `resume`, `quarantine`), the corresponding `retry_budget` entry is cleared along with the `BlockerRecord`. Stale retry-budget state is never left behind.

The cursor advances **only** in two ways: a worker-initiated `DONE`/`NOOP_RESOLVED` outcome, or an operator-initiated `quarantine`. `resume` is "clear the diagnostic, let the worker retry on its own" — it never moves the cursor.

**No automatic skip.** The worker does not advance past a `BLOCKED` event on its own. Watermark lag for that `(shard, stream)` accumulates and triggers the lag-correctness fallback (degraded → scan_only) at the usual thresholds. Persistent operator inattention falls back to safety scans, which is the correctness floor.

**Sustained-RETRY_LATER promotion.** A `RETRY_LATER` outcome (`TRANSPORT_ERROR`, network blip, filer IO error) by itself doesn't write a `BlockerRecord` — the cursor stays and the next batch retries. But if the same logical retry target keeps producing `RETRY_LATER` across many batches, the underlying issue is no longer "transient." The stream would silently lag forever without operator attention.

A small durable counter file at `/etc/s3/lifecycle/_reader/retry_budget` tracks consecutive retries per stream-specific key (the same key shape used by `BlockerRecord` for that stream):

```
// RetryTarget carries the routing/identification fields needed to (a) write a
// BlockerRecord on promotion and (b) point the operator's `blockers retry`
// at the right handler.
//
// We deliberately do NOT carry expected_identity or action. Retry semantics
// are: re-invoke the handler implied by stream_kind. The handler re-fetches
// live state and re-evaluates from scratch:
//   ORIGINAL  / PREDICATE  → re-run handleEvent at (shard, position) — fetches
//                            live, evaluates each candidate rule against current
//                            state, attempts deletes with current-identity CAS.
//   BOOTSTRAP              → re-run walker step at (bucket, object_path) —
//                            fetches the entry, evaluates rule, attempts delete.
//   PENDING                → re-run drainPending step for the matching
//                            PendingItem (which itself carries expected_identity
//                            in the pending file).
// If the object changed between the original failure and the retry, CAS returns
// STALE_IDENTITY → NOOP_RESOLVED and the blocker resolves cleanly. Capturing
// the original action would just be a stale hint.
//
// This also means RetryTarget works uniformly for failure paths that AREN'T
// delete RPCs — fetchLive ERR, pending upsert/demotion failure — because all
// of them are handled by the same per-stream handler on retry.
message RetryTarget {
  StreamKey key                  = 1;        // oneof, matches BlockerRecord stream-specific compaction keys

  // Common context (populated for every stream).
  string    bucket               = 2;
  string    object_path          = 3;
  string    version_id           = 4;
  bytes     rule_hash             = 5;       // for BOOTSTRAP/PENDING this is the rule the walker/drain
                                              // was working under. For ORIGINAL/PREDICATE the failure
                                              // can span multiple candidate rules; rule_hash is the
                                              // one whose evaluation triggered the RETRY_LATER, but
                                              // retry re-evaluates ALL current candidates against
                                              // current state, so a stale rule_hash here doesn't
                                              // mislead replay.
}

message RetryBudget {
  RetryTarget target              = 1;
  int32       consecutive_retries = 2;
  int64       first_retry_at_ns   = 3;
  int64       last_retry_at_ns    = 4;
}

message StreamKey {
  StreamKind stream_kind = 1;
  oneof key {
    OriginalKey  original  = 2;
    PredicateKey predicate = 3;
    BootstrapKey bootstrap = 4;
    PendingKey   pending   = 5;
  }
}

message OriginalKey  { string shard = 1; int64 delay_seconds = 2; MessagePosition position = 3; }
message PredicateKey { string shard = 1; MessagePosition position = 2; }
message BootstrapKey { string bucket = 1; string object_path = 2; string version_id = 3; bytes rule_hash = 4; }
message PendingKey   { string bucket = 1; bytes  rule_hash   = 2; string object_path = 3; string version_id = 4; }
```

Note on `BootstrapKey`/`PendingKey`: these duplicate `bucket`/`object_path`/`version_id`/`rule_hash` from `RetryTarget`'s common context. The duplication is intentional — `StreamKey` is the **lookup/dedup key** for blockers and retry-budget entries (so two callers can find the same record); `RetryTarget`'s common-context fields are populated identically and used for the BlockerRecord/retry payload. For `ORIGINAL`/`PREDICATE`, the common context fields *are* the only place bucket/object info exists (the `StreamKey` carries shard/position only).

Helper functions used by reader/bootstrap/drain:

```
recordRetryLater(target RetryTarget):
    rb := load_or_create(target.key)
    rb.target = target                                    // refresh; the failing target should be stable
    rb.consecutive_retries += 1
    if rb.first_retry_at_ns == 0: rb.first_retry_at_ns = now()
    rb.last_retry_at_ns = now()

    if rb.consecutive_retries < retryBudgetMax and
       now() - rb.first_retry_at_ns < retryBudgetMaxAge:
        save retry_budget
        return RETRY_LATER

    // Threshold tripped — promote to BLOCKED. The blocker must commit durably
    // BEFORE we return BLOCKED, otherwise the caller would pause the stream
    // with no operator-visible record. If the write fails, leave the
    // retry_budget entry in place and return RETRY_LATER; the next batch will
    // re-attempt the promotion.
    ok := writeBlocker(BlockerRecord{
        stream_kind:      target.key.stream_kind,
        ...key fields from target.key...,
        rule_hash:        target.rule_hash,
        bucket:           target.bucket,
        object_path:      target.object_path,
        version_id:       target.version_id,
        reason:           "sustained RETRY_LATER",
        last_error:       "promoted from retry_budget",
        first_seen_at_ns: rb.first_retry_at_ns,
        last_retry_at_ns: rb.last_retry_at_ns,
        retry_count:      rb.consecutive_retries,
    })
    if !ok:
        save retry_budget                                 // keep the budget; try promotion again next batch
        return RETRY_LATER
    delete retry_budget entry for target.key
    return BLOCKED                                        // caller treats as BLOCKED

clearRetryBudget(streamKey):
    delete retry_budget entry for streamKey if present    // success → reset budget
```

Each caller passes a fully-populated `RetryTarget`. The reader path builds it **inside `handleEvent`** (per failing branch — fetch_live FATAL/ERR, deleteAndResolve outcome) so the retry target reflects which branch actually failed; the reader callback itself doesn't construct `RetryTarget`. Bootstrap builds it directly from the entry being walked; pending drain builds it directly from `item`. RetryTarget intentionally carries no `action` or `expected_identity`: operator-driven retry (and shell-side `blockers retry`) replays the appropriate primitive against current live state, which re-fetches and re-evaluates from scratch.

Defaults: `retryBudgetMax = 30` (≈30 minutes at 1-batch-per-minute reader cadence), `retryBudgetMaxAge = 4h`, whichever comes first. The retry_budget file is bounded by the count of positions *actively retrying* (typically 0 or a small handful) and compacts on success.

This gives sustained-transient → BLOCKED a concrete mechanism without a per-event durable record on every `RETRY_LATER`.

The blocker file's compaction key is **stream-specific**, since the identifying tuple differs per stream:

- `ORIGINAL`: `(stream_kind, shard, delay_seconds, position)` — `position` is required because the same shard can have multiple blockers at different `MessagePosition`s within the same delay group (sequential failures the operator hasn't cleared yet); without `position` they'd collapse.
- `PREDICATE`: `(stream_kind, shard, position)` — same reasoning; `delay_seconds` not used.
- `BOOTSTRAP`: `(stream_kind, bucket, object_path, version_id, rule_hash)` — without `version_id` and `rule_hash`, two different rules' walks failing on the same `(bucket, object_path)` would collapse into one record and the operator couldn't distinguish them.
- `PENDING`: `(stream_kind, bucket, rule_hash, object_path, version_id)` — different versions of the same key under the same rule are distinct pending items; without `version_id` they'd collapse.

A re-blocking of the same key overwrites the previous record (preserves `first_seen_at_ns`, refreshes `last_retry_at_ns`, increments `retry_count`) rather than appending a new one. Resolution (via `retry` success, `resume`, or `quarantine`) removes the record by its full key.

### Failure / restart

- The meta log is the durable queue. Watermarks advance only past events that returned `DONE` or `NOOP_RESOLVED`.
- On `RETRY_LATER` (transient — S3 unreachable, network, filer IO): the batch stops at that event; the per-shard cursor stays at the last resolved `(filer_id → MessagePosition)`; the next batch retries the same event from the per-shard skip predicate.
- On `BLOCKED` (deterministic — `FATAL_EVENT_ERROR` after K=5 retries, or sustained `RETRY_LATER` past a per-shard threshold): the worker writes a `BlockerRecord` and pauses that `(shard, delay_seconds)` combination. Other shards and other delay groups continue. The cursor stays at `failing_position` until an operator runs `s3.lifecycle.blockers retry|resume|quarantine`.
- On bootstrap `RETRY_LATER`/`BLOCKED`: the walker stops; `last_scanned_path` stays at the previous successful entry; a `BlockerRecord` is written for the `BLOCKED` case.
- On pending drain `RETRY_LATER`/`BLOCKED`: the pending item stays in the rule's `pending` file; for `BLOCKED` a `BlockerRecord` is written and the rule's drain pauses.
- Mid-tick crash → next tick re-runs from last persisted state.
- Idempotent throughout: identity CAS in the server prevents double-delete; pending upsert keyed by `(path, version_id)` within each rule's directory.

For permanently-failing events: see "Blocked cursor handling" above. The worker never auto-skips a failing event; it pauses and surfaces. Operator decides resume / retry / quarantine.

### Scalability — pending is for exceptions, not normal scheduling

The meta log + per-filer watermarks **are the scheduler** for age-based rules. Per-object pending is reserved for cases the watermark can't express:

| Reason for pending | Bounded by |
|---|---|
| Late predicate change creates new eligibility on a not-yet-due object | rate of late-arriving tag/metadata changes that newly match a rule |

What does **not** go into pending in normal operation:

- **Future age-based deletes.** Discovered by the reader when their original-write event ages past the cutoff. The event log is the durable schedule.
- **Currently-due objects from bootstrap.** Bootstrap walks and deletes them inline; the walk cursor (`last_scanned_path`) is the resume point.
- **Date-rule matches.** `SCAN_AT_DATE` mode runs a single bootstrap at `rule.date`; no pending.

**Steady-state pending size** in a normally-operating cluster: typically zero or low single-digit items per rule, populated only by late predicate-change exceptions (a tag added long after the original PUT that newly matches a rule, where `live.mtime + TTL` is still in the future). Independent of bucket size and write rate. Object-lock skips do *not* go to pending — they're logged-and-counted, with a periodic safety scan handling re-evaluation if the lock is later released.

**S3 unavailable**: reader/bootstrap stop on `TRANSPORT_ERROR`, watermark stalls at the last resolved event, no pending growth. Next read task retries from the same watermark. Events are durable in the meta log for the configured retention. Operators see an alerting signal via watermark-lag metrics.

**High write rate** (e.g. 10k/s log ingest with age=1d): the reader processes events as their `now - MinTriggerAge` cutoff sweeps past them. Each event is one synchronous `LifecycleDelete`. Throughput is bounded by `LifecycleDelete` parallelism (configurable via `lifecycle.delete.concurrency`, default 16 concurrent calls). Chunk reads from the merged log stream parallelize across volume servers (`lifecycle.reader.chunk_concurrency`, default 8). Pending stays empty in normal flow.

**Delete throughput is coupled to reader progress.** Cursor advance is gated on per-event resolution; if `LifecycleDelete` throughput is slower than the rate at which events age into the original-write cutoff, the watermark lags behind real time. If the lag grows past `metaLogRetention`, events would be pruned from the log before they can be processed — without a fallback, silent under-deletion.

Operators have three levers in order of cost:

1. **Raise `lifecycle.delete.concurrency`** to fan out more in-flight `LifecycleDelete` RPCs. Effective until the S3 server pool is the bottleneck.
2. **Raise `lifecycle.reader.chunk_concurrency`** to fan out more in-flight chunk reads from volume servers. Effective until network or volume servers are the bottleneck.
3. **Scale S3 servers / network**.

If those levers don't suffice, see "Future scaling" below.

**Future scaling (non-v1, no committed phase).** If single-worker reader throughput is insufficient even after raising delete concurrency, chunk-read concurrency, and S3 server capacity, the design admits one extension: time-segment leases on the merged log stream. The merged stream's time range `[low_water, cutoff]` is partitioned into N time segments; each segment leased to a separate worker process; each worker processes its segment independently and reports `last_processed_position_per_shard` on completion; a coordinator gates **contiguous watermark commit** — a segment's results only commit (and its watermark advance only persists) once *every earlier segment has committed*. Per-bucket pending writes still use the same `(path, version_id)` dedup key, so cross-segment race on a path is idempotent. This stays worker-side (no per-filer fanout, no filer-side changes); the filer's role is unchanged. Concrete design and implementation are deferred — there is no committed phase for it. Operators hitting this scale should bring it up; until then v1 ships with one reader.

**Correctness fallback when the lag approaches retention.** Alerting alone is not enough — by the time an operator scales capacity, events may already be pruned. The reader monitors **two parallel lag signals**:

1. **Per-delay-group original-write lag** — `original_lag_seconds[delay_seconds] = now - low_water(reader_state.last_processed_original[delay_seconds])`. Affects every rule that uses that delay.
2. **Per-shard predicate lag** — `predicate_lag_seconds[filer_id] = now - reader_state.last_processed_predicate[filer_id].ts_ns`, with the engine-wide aggregate `predicate_lag_seconds = max over filer_id`. Affects every **predicate-sensitive** rule (any rule with tag/size filters), regardless of its delay group. Without this check, predicate-stream blockage would let tag/metadata changes age out of the meta log without any rule entering `scan_only`.

Per-stream thresholds:

| Lag (per stream) | Action |
|---|---|
| `< metaLogRetention × 0.5` | Healthy. Clear `degraded_reason = LAG_HIGH` if previously set. |
| `≥ × 0.5` and `< × 0.8` | Emit `LIFECYCLE_LAG_WARNING` metric + log; surface in `s3.lifecycle.status`. Set `degraded_reason = LAG_HIGH` if not already. |
| `≥ × 0.8` | Affected rules keep `mode = event_driven` AND `degraded_reason = LAG_HIGH`; force the next safety-scan bootstrap to run *immediately* without waiting for the per-kind cadence. Continue event-driven processing in parallel. |
| `≥ × 0.95` | Promote affected rules to `mode = scan_only` durably (set `degraded_reason = LAG_HIGH`, `degraded_since_ns = now`). The reader stops processing the affected stream — the affected delay group, or all predicate-sensitive rules' predicate ingestion, respectively. The safety scan is the only enforcement until the operator explicitly re-enables event-driven mode. |

Affected-rules scope:

- Original-write lag at threshold for delay `D` → rules whose `delay == D`.
- Predicate lag at threshold → all rules with `predicateSensitive == true` (scoped per bucket — promotion to `scan_only` is per-rule, but the trigger is engine-wide).

**Re-enable** after a predicate-stream-induced `scan_only` uses the existing `force_reseed_predicate` path. The `s3.lifecycle.reseed` shell command takes `-predicate` to flag predicate-stream-driven re-enables: it sets `force_reseed_predicate=true` on the seed call, rewinding `last_processed_predicate` to `T_start - bootstrapLookbackMin` per filer, then flips affected rules back to `event_driven`.

**Re-enabling event-driven mode after a `scan_only` fallback** is a delay-group operation, not a per-rule state flip. The cursor `last_processed_original[D.seconds]` is shared across **all rules with delay D** (e.g., two rules with 60-day expiration but different prefixes both consume the 60d cursor's stream). Reseeding for rule A's sake forces rules B, C, … on the same delay D to replay the reseed window too. That replay is idempotent (CAS on identity, pending upserts collapse) and the cost is bounded by the lookback window's events, but it must be acknowledged.

Operator workflow:

1. `weed shell s3.lifecycle.reseed -bucket X -delay <days>` — operates on the delay group, not a single rule. Lists all rules currently in `scan_only` mode for that delay and reports which buckets/rules will replay.
2. The command:
   - Computes `force_reseed_delays = { D }` for the delay being re-enabled. (For multiple delays in one invocation, the set has multiple entries.)
   - Deletes `reader_state.last_processed_original[D.seconds]` (the entire per-filer map for that delay group).
   - Runs a fresh bucket-level bootstrap for **every bucket whose policy includes a rule with delay D** (idempotent, in parallel). Each bootstrap, on completion, calls **`seedReaderCursorsForNewDelayGroups(snapshot, newly_completed, target_modes, T_start, force_reseed_delays={D}, force_reseed_predicate=<true if any affected rule is predicateSensitive else false>)`** — the explicit force arguments are required because the affected rules already have `bootstrap_complete=true` and therefore are NOT in `newly_completed`; without them the seeder's early-exit would leave both the deleted delay cursor un-seeded AND the predicate cursor un-rewound (silently missing tag/metadata changes that happened during the scan_only window for predicate-sensitive rules). The seeder seeds each `D` in `force_reseed_delays` at `T_start - max(D, bootstrapLookbackMin)` per filer with the before-first sentinel; if `force_reseed_predicate=true` it also rewinds the predicate cursor to `T_start - bootstrapLookbackMin` per filer.
   - After the seeder commits, flips affected rules' `mode` from `scan_only` back to `event_driven` durably and clears `degraded_reason`.
3. The reader resumes processing the delay group; rules that were not in scan_only also re-receive the lookback window of events, idempotently. CAS on identity ensures no duplicate deletes; pending upserts collapse on `(path, version_id)`.

Lost-log acknowledgement is a separate, explicit workflow because it admits that some retained metadata history was lost:

1. `weed shell s3.lifecycle.reseed -ack-lost-log --reason <text>` — cluster-wide, only allowed while some rules have `degraded_reason = LOST_LOG`.
2. The command verifies an immediate safety-scan bootstrap has completed for every bucket with reader-driven lifecycle rules after `degraded_since_ns`; if not, it runs those bootstraps first and waits for completion.
3. It deletes the lost-log cursor entries for vanished shard streams, clears any matching `tail_drained_streams` markers, then force-reseeds all reader delay groups and the predicate cursor using the same `force_reseed_delays` / `force_reseed_predicate` path above.
4. After seeding commits, it clears `LOST_LOG`; rules return to `event_driven` only if the normal retention gate still passes. Rules whose `metaLogRetention < eventLogHorizon + bootstrapLookbackMin` remain `scan_only` with `RETENTION_BELOW_HORIZON`.

If only one rule is to be reactivated and the operator wants to avoid replay for sibling rules, the alternative is to *split* the rule's delay into a unique value (e.g., `60d` → `61d`), which gives it its own delay-group cursor at the cost of changing AWS-visible semantics. Generally, delay-group-wide replay is acceptable.

Without this protocol, naive per-rule re-enable would either replay too much (cursor at the old position before the lag spike) or skip newly-eligible events (cursor naively bumped to "now"). The reseed pins the cursor exactly at the safe replay floor.

This guarantees no silent loss: by the time events would be pruned, the affected rules are already running on the bucket-walk fallback, with explicit signal in `status`. `degraded` and `scan_only` are surfaced via:

- `lifecycle_rule_mode{rule_hash, mode}` — `event_driven | scan_at_date | scan_only | disabled`.
- `lifecycle_rule_degraded{rule_hash, reason}` — `NONE | LAG_HIGH | PENDING_FULL | DELETE_FAILURES | OPERATOR_PAUSED`.
- `watermark_lag_seconds{delay_group}` — primary indicator.
- `lifecycle_delete_concurrency_saturation` — fraction of the concurrent-call budget in use.
- `lifecycle_delete_p99_latency` — server-side delete latency.

We deliberately do **not** introduce a durable delete-handoff queue. That would decouple cursor advance from delete completion at the cost of bringing back per-object pending growth and the scalability problems it causes. The simpler answer — auto-fallback to scan_only when lag threatens retention — is what this design commits to.

**Bootstrap of a stale bucket** (lifecycle freshly enabled on a 60k-object bucket where all are currently due): bootstrap walks pages and deletes inline. No pending growth. The walk pauses on `TRANSPORT_ERROR` and resumes from `last_scanned_path`. Total wall time = `bucket_size / parallel_delete_throughput`.

**Pending file format.** Append-only with tombstones; in-memory `(path, version_id) → offset` index for upsert lookups. Compaction triggered when tombstone ratio exceeds 50%. Because pending is bounded by exceptions, the file rarely grows beyond a few MB and compaction is cheap.

**Backpressure cap on pending** (default 50k items per rule): triggered only by an unusual workload — typically a sudden burst of late predicate-change events on a long-TTL rule. The predicate-change stream is **shared across all rules** (one `last_processed_predicate: map<filer_id, MessagePosition>` per shard, advanced by every predicate event the engine routes to *any* rule's candidates). We cannot selectively halt it for a single rule without halting predicate-change processing for all rules. The mechanism is therefore:

1. On the first pending write for the affected rule that would exceed the cap (or any pending write that fails), the reader treats the rule as **degraded** and **atomically switches it to `scan_only` mode** (durable per-rule state write). The current event is then "resolved" for that rule — the next safety-scan bootstrap will re-evaluate any objects whose predicates changed late.
2. Other rules continue their predicate-change processing normally; the global predicate cursor keeps advancing.
3. The reader emits `PENDING_FULL{rule_hash}` (metric + log) so operators see why the rule degraded.
4. The same mechanism handles any unrecoverable `pending` write error (filer IO failure, etc.): attempt the durable demotion to `scan_only`. The cursor only advances after that demotion write *commits*. If the demotion write itself also fails (filer IO continues to be unhealthy), `handleEvent` returns `RETRY_LATER`; the batch halts with the per-shard cursor at the previous event, and the next read task retries. Advance is gated on durable degraded-state persistence, never on alerting alone.

This preserves correctness without coupling one rule's exception load to the global predicate stream. Demoting a rule to `scan_only` is reversible — operators can manually return it to `event_driven` after the load subsides.

Object-lock skips never enter pending; they're logged-and-counted only. Pending is exclusively the late-predicate-change exception path.

**Health signals.** Watermark lag (per delay group AND per shard for predicate), pending size, S3 RPC error rate, `skipped_object_lock_total`, `blocked_streams{stream_kind}` (gauge of active blocker records). Sustained watermark lag is the primary indicator. Active blockers are the operator-attention indicator.

### Observability

CLI:
- `weed shell s3.lifecycle.status -bucket X` — mode, watermark, pending size, last tick, counts.

Headers:
- `X-SeaweedFS-Lifecycle-LastTick` on `GetBucketLifecycleConfiguration`.

Prometheus:
- `seaweedfs_s3_lifecycle_evaluated_total{bucket,rule_hash}`
- `seaweedfs_s3_lifecycle_expired_total{bucket,rule_hash,action}`
- `seaweedfs_s3_lifecycle_metadata_only_total{bucket,rule_hash}` — count of skip-chunk-delete deletes
- `seaweedfs_s3_lifecycle_skipped_object_lock_total{bucket,rule_hash}` — locked objects encountered
- `seaweedfs_s3_lifecycle_blocked_streams{stream_kind,bucket}` — gauge: number of active blocker records by stream_kind (ORIGINAL / PREDICATE / BOOTSTRAP / PENDING)
- `seaweedfs_s3_lifecycle_quarantined_total{stream_kind,bucket}` — counter: operator-quarantined records (auditable; only path that intentionally drops a delete decision)
- `seaweedfs_s3_lifecycle_predicate_lag_seconds{filer_id}` — primary indicator for predicate-stream health
- `seaweedfs_s3_lifecycle_errors_total{bucket,rule_hash,kind}`
- `seaweedfs_s3_lifecycle_pending_size{bucket,rule_hash}`
- `seaweedfs_s3_lifecycle_watermark_lag_seconds{bucket,rule_hash,delay_group}`

Admin UI (the existing plugin lane filter at `weed/admin/view/app/plugin_lane.templ` + `weed/admin/dash/plugin_api.go:filterActivitiesByLane` already surfaces job type `s3_lifecycle` — confirmed by `dash/plugin_api_test.go:204-230`):

1. **Per-bucket lifecycle panel on `s3_buckets.templ`.** Per rule:
   - rule_id (or hash prefix if blank)
   - mode: `event_driven` | `scan_only`
   - bootstrap state: `complete` | `% scanned`
   - watermark lag (`now - last_processed_ts`)
   - pending size
   - counters: evaluated / expired / errors / metadata-only
   - actions: `Run now` (force batch), `Re-bootstrap`, `Pause`

2. **Lifecycle lane summary on `plugin_lane.templ`** when `?lane=lifecycle`:
   - active batches
   - cluster-wide pending sum
   - watermark lag p50/p99
   - off-peak window indicator if `lifecycle.run_hours` is set

3. **JSON API endpoints under `weed/admin/dash/`:**
   - `GET  /api/s3/buckets/{name}/lifecycle/status` — decoded per-rule `state` files
   - `POST /api/s3/buckets/{name}/lifecycle/run` — enqueue forced batch (optional `?rule_hash=`)
   - `POST /api/s3/buckets/{name}/lifecycle/rebootstrap` — reset bootstrap for a rule

4. **PUT XML editor (if/when it lands)**: warn on rules whose TTL exceeds meta-log retention with "Will run in scan_only mode at slower cadence."

### PUT handler becomes (post-Phase 4 below)

1. Validate XML.
2. Persist `lifecycle.xml` xattr.
3. Reconcile per-rule directories under `/etc/s3/lifecycle/<bucket>/`: create new dirs with `bootstrap_complete=false`; preserve `bootstrap_complete=true` and counters for unchanged rule hashes; mark removed rules' dirs for grace-period cleanup. (Reader watermarks live in cluster `reader_state`, not per-rule; unchanged.)
4. Wake the scan scheduler.
5. Return 200.

Constant time, no bucket walk, no `filer.conf` mutation.

### Migration

- **Cannot strip the back-stamp until the worker can take over.** Phasing reflects this.
- Existing entries with `TtlSec` stamps from the old back-stamp keep working via the read-time filer check. Worker plus those stamps converge.
- First worker tick on each existing bucket runs the bootstrap scan once.
- `DeleteBucketLifecycle` clears xattrs; existing `TtlSec` stamps stay (don't aggressively unstamp — would resurrect objects).

## Comparison

| | Today | Proposed |
|---|---|---|
| `PUT` latency | O(N entries) | O(1) |
| Steady-state cost per tick | n/a (no scanner) | O(events × distinct_delay_groups) + pending drain (exceptions only) + amortized periodic safety-scan walk per bucket per safety cadence |
| Bootstrap cost | Hidden in `PUT` | Once per bucket per snapshot, in background; re-runs on safety cadence |
| Tag/size filters | Silent no-op | Live-evaluated; late-tag-add uses pending exception |
| Date filters | Silent no-op | `SCAN_AT_DATE` mode: scheduled bucket-level bootstrap at `rule.date` (no per-object pending) |
| Versioned buckets | Silent no-op | Driven by `.versions` events |
| `AbortIncompleteMultipartUpload` | Silent no-op | Triggered on `.uploads/` events |
| Cold-object handling | Never | Bounded by tick + due-time |
| Source of truth | `filer.conf` + xattr | xattr only |
| Failure recovery | None | Watermark + pending + CAS |

---

# Dev Plan

Phasing changed: **the back-stamp cannot be removed until the worker can take over.** The first behavior-changing PR is the worker, not the PUT slim-down.

## Phase 0 — Confirm assumptions (research, no code)

- Verify `Filer.ReadPersistedLogBuffer` returns events including `Extended` on `NewEntry`.
- Document retention policy of `/topics/.system/log/`. If pruned, find the knob and document it; this sets the threshold for `scan_only` mode.
- Confirm the exact filename scheme inside `<key>.versions/` (sortable timestamp prefix? base32 of ts? affects how we determine "successor" version).
- Capture findings inline in this design doc.

## Phase 1 — `s3lifecycle.Evaluate` (no callers)

- Add `EvaluateAction(rule *Rule, kind ActionKind, info *ObjectInfo, now time.Time) EvalResult`, `ComputeDueAt(rule *Rule, kind ActionKind, info *ObjectInfo) time.Time`, `MinTriggerAge(rule *Rule, kind ActionKind) time.Duration`, `EventLogHorizon(rule *Rule, kind ActionKind) time.Duration`, and `RuleActionKinds(rule *Rule) []ActionKind` in `weed/s3api/s3lifecycle/`. All kind-aware: a single XML rule expands into N compiled actions and each helper operates on one (rule, kind) pair so sibling actions are scheduled, gated, and evaluated independently. `EventLogHorizon` returns `rule.Days` / `rule.NoncurrentDays` / `rule.DaysAfterInitiation` for age-based kinds, and `smallDelay` for `NEWER_NONCURRENT` and `EXPIRED_DELETE_MARKER`. `EXPIRATION_DATE` is not a reader-driven kind and the gate skips it.
- Cover: `ExpirationDays`, `ExpirationDate`, `ExpiredObjectDeleteMarker`, `NoncurrentVersionExpirationDays`, `NewerNoncurrentVersions`, `AbortMPUDaysAfterInitiation`, prefix + tag + size filters, And-combinations.
- Add `RuleHash(rule *Rule) [8]byte` over a length-prefixed canonical form (sorted tags, prefix verbatim, every action's parameters, days/date). Stable across XML reordering, ID renames, and Status flips; resistant to delimiter forgery.
- Unit tests in `evaluate_test.go` per rule shape, including AWS edge cases (date in future, zero days, empty prefix, And).
- No callers yet. **Independently mergeable.**

## Phase 2 — LifecyclePolicyEngine + bucket-level bootstrap + LifecycleDelete RPC + scan_only gate (non-versioned)

- Define protos in `weed/pb/s3_lifecycle.proto`: per-action `LifecycleState` keyed by `(rule_hash, action_kind)` (no watermarks), `PendingItem` (late-predicate exceptions only — no `BlockedItem`, no compliance ledger), `EntryIdentity`. Cluster-level `ReaderState` (watermark maps + `tail_drained_streams`). Top-level `ActionKind` enum referenced by `LifecycleState`, `BootstrapKey`, `PendingKey`, `BlockerRecord`, `RetryTarget`.
- Define `S3LifecycleParams` (subtype `READ` | `BOOTSTRAP` | `DRAIN`) and `ContinuationHint` in `weed/pb/worker_pb/worker.proto`. `S3LifecycleParams.action_kind` is required for DRAIN, ignored by BOOTSTRAP.
- Storage:
  - Per-action: `/etc/s3/lifecycle/<bucket>/<rule_hash_hex>/<action_kind>/{state, pending}`. Each XML rule expands into N action subdirectories under one rule_hash dir.
  - Per-bucket bootstrap progress: `/etc/s3/lifecycle/<bucket>/_bootstrap`.
  - Cluster reader: `/etc/s3/lifecycle/_reader/{reader_state, blockers, retry_budget}`. Single reader task; `reader_state` holds per-filer-shard cursors (`map<delay_seconds, map<filer_id, MessagePosition>>` for originals and `map<filer_id, MessagePosition>` for predicate) — required for resume correctness across the client-side merge. `blockers` is the durable record of paused stream entries (operator-resolvable). `retry_budget` tracks consecutive `RETRY_LATER` per stream key for promotion to BLOCKED at threshold. No reader-group partitioning, no assignment epochs.
- New package `weed/s3api/s3lifecycle/engine/` — `LifecyclePolicyEngine`. Compiles all bucket lifecycle xml into:
  - `bucket → BucketIndex { prefixTrie, tagIndex, versioned }` keyed by `ActionKey`.
  - `originalDelayGroups: map[duration][]ActionKey` for original-write sweeps.
  - `predicateActions: []ActionKey`, `dateActions: map[ActionKey]time.Time`.
  - Snapshot ID; rebuild on observed lifecycle xattr change events.
- Per-`ActionKey` policy CAS via `(rule_hash, action_kind)` membership — no per-bucket etag scheme.
- New package `weed/s3api/s3lifecycle/worker/`:
  - **Bucket-level** bootstrap walker: walks each bucket once per run, evaluates every applicable `ActionKey` per object via the engine (`EvaluateAction(rule, kind, info, now)` for each compiled action). Inline delete for currently-due age actions; skip not-yet-due (log replay handles); skip date actions (SCAN_AT_DATE handles). `last_scanned_path` checkpointed in `_bootstrap`. Per-action completion writes commit *after* the walk.
  - On `TRANSPORT_ERROR` the walker stops; resume from `last_scanned_path` next task.
  - Skip versioned buckets with a logged warning. Implementation lands in Phase 5.
- **Mode gate (lands here):** at engine compile time, for each `ActionKey`, compute `eventLogHorizon(rule, action_kind)` per the table in §"Per-rule mode decision"; if `metaLogRetention < eventLogHorizon(rule, action_kind) + bootstrapLookbackMin`, mark *that action* `scan_only` with `degraded_reason = RETENTION_BELOW_HORIZON`. Applies to all reader-driven kinds (`EXPIRATION_DAYS`, `NONCURRENT_DAYS`, `ABORT_MPU`, `NEWER_NONCURRENT`, `EXPIRED_DELETE_MARKER`); date kind bypasses this gate. Sibling actions of the same rule are evaluated independently — a 90d `EXPIRATION_DAYS` may degrade while a 7d `ABORT_MPU` stays event-driven. Same bootstrap code path; detector emits at the per-kind cadence.
- `SCAN_AT_DATE` mode: detector schedules a single bootstrap at the action's `rule.date`.
- Worker-to-S3 discovery via admin's `ListS3Endpoints` RPC; cache 30s; rotate on RPC failure.
- New gRPC `LifecycleDelete` (`weed/s3api/s3api_internal_lifecycle.go`). Server handler steps 1–7 above; the request carries `(rule_hash, action_kind)` for ActionKey CAS.
- Object lock: `LifecycleDelete` server explicitly calls `s3a.enforceObjectLockProtections` (in `weed/s3api/s3api_object_retention.go`) with `governanceBypassAllowed=false` BEFORE dispatching low-level helpers. If the check refuses, returns `SKIPPED_OBJECT_LOCK`. Worker logs, increments counter, advances. No retain-until rescheduling, no blocked file.
- Admin server: register S3 endpoints; serve `ListS3Endpoints`.
- Wire task types in `weed/worker/tasks/s3lifecycle/`:
  - `s3.lifecycle.bootstrap` per-bucket (not per-action).
  - `s3.lifecycle.read` cluster-singleton (Phase 3 fills in).
  - `s3.lifecycle.drain` per-`ActionKey` for exception drains.
  All bound to `LaneLifecycle`.
- Cluster locks: `s3.lifecycle.read` (singleton), `s3.lifecycle.bootstrap:<bucket>`, `s3.lifecycle.drain:<bucket>:<rule_hash_hex>:<action_kind>`, `s3.lifecycle._reader.seeding` (global, serializes seed writes across bucket bootstraps).
- Detector in `weed/admin/maintenance/`.
- Shell commands `s3.lifecycle.run`, `s3.lifecycle.status`.
- Worker runs alongside the back-stamp.
- Tests: bootstrap fixture bucket with multiple rules — verify one walk evaluates all; not-yet-due not enqueued; date rules deferred to SCAN_AT_DATE; transport error pauses then resumes; scan_only gate triggers on short retention.
- **Regression tests**:
  - `target_modes` computed before seeding: inject a fixture where the seeder is called with rules whose `state.mode` field is at proto zero value; assert the seeder filters using the precomputed `target_modes` map (not the unwritten state field), so EVENT_DRIVEN rules trigger rewind and SCAN_AT_DATE/SCAN_ONLY/DISABLED rules do not. Without this, the seeder's filter would silently fail.
  - `force_reseed_delays` for scan_only re-enable: start with a rule in `mode = scan_only` and `bootstrap_complete = true` (so it never enters `newly_completed`); delete `reader_state.last_processed_original[D.seconds]`; call the seeder with `newly_completed = {}` and `force_reseed_delays = {D}`; assert the cursor is re-seeded at `T_start - max(D, bootstrapLookbackMin)`. Then assert the same call WITHOUT `force_reseed_delays` does **not** re-seed (verifies the early-exit path is correct for routine safety scans).
  - `force_reseed_predicate` for predicate-sensitive scan_only re-enable: start with a predicate-sensitive rule (e.g. `Filter.Tag` filter) in `mode = scan_only`; advance the predicate cursor in `reader_state.last_processed_predicate` past `T_start - bootstrapLookbackMin` to simulate normal reader progress while the rule was scanned-only; emit a tag-add meta event in that interval (would be predicate-relevant if the rule were active); flip the rule via the reseed workflow with `force_reseed_predicate = true`; assert the predicate cursor was rewound to `T_start - bootstrapLookbackMin` per shard; assert the previously-emitted tag-add event is now picked up by the next reader pass and routed through `handleEvent` for the re-enabled rule. Without `force_reseed_predicate` the test must show the cursor un-rewound and the tag-add event silently missed — this is the regression that motivates the parameter.
  - Bootstrap fatal-error blocker: inject a rule with a corrupted entry that causes `LifecycleDelete` to return `FATAL_EVENT_ERROR` deterministically. Verify bootstrap walker calls `deleteAndResolve` (not `lifecycleDeleteRPC` directly), retries K=5 times, writes a durable `BlockerRecord` with `stream_kind = BOOTSTRAP`, returns `BLOCKED`, and `last_scanned_path` is left at the previous successful entry (NOT advanced past the failure). Verify the next bootstrap task refuses to advance past the blocker until the operator clears it.
  - Reader fatal-error blocker: similar, with `stream_kind = ORIGINAL` and `(shard, delay_seconds, position)` populated for the original-write pass; with `stream_kind = PREDICATE` and `(shard, position)` for the predicate pass. The blocked `(shard, stream)` pauses; other shards/streams continue processing in the SAME batch — the BLOCKED outcome adds to in-memory blocked sets and the callback returns `PauseShard(filer_id)`, which removes that shard from the merge heap so subsequent buffered events from it are not delivered (verify via spy on the merge engine). The next reader task loads `_reader/blockers` and uses `MaxMessagePosition` for blocked shards' `start_positions` so no events are re-read until the blocker is cleared.
  - Pending fatal-error blocker: similar, with `stream_kind = PENDING`; the rule's drain pauses; the pending item stays.
  - `writeBlocker` failure path: if the durable blocker append fails, `deleteAndResolve` (and the inline FATAL fetchLive path in `handleEvent`) returns `RETRY_LATER` instead of `BLOCKED`. Verify the cursor stays at the failing position and the next batch retries.
  - Operator commands:
    - `blockers list` filters by `--stream`, `--shard`, `--bucket`.
    - `blockers retry <id>` re-attempts synchronously, with stream-specific success effects:
      - `ORIGINAL`: on `DONE`/`NOOP_RESOLVED` removes the record AND advances `reader_state.last_processed_original[delay_seconds][shard]` past the recorded `position`.
      - `PREDICATE`: on `DONE`/`NOOP_RESOLVED` removes the record AND advances `reader_state.last_processed_predicate[shard]` past the recorded `position`.
      - `BOOTSTRAP`: on `DONE`/`NOOP_RESOLVED` removes the record only; **does not** mutate `_bootstrap.last_scanned_path`. The next scheduled bootstrap task picks up from `last_scanned_path` and re-walks normally.
      - `PENDING`: on `DONE`/`NOOP_RESOLVED` removes the record AND tombstones the matching `PendingItem` in the rule's `pending` file. `STALE_IDENTITY` outcomes also tombstone (the pending entry referred to a stale identity).
      - All streams: on `RETRY_LATER`/`BLOCKED` again, update `retry_count` and `last_retry_at_ns`, leave the blocker, leave any cursors / pending items unchanged.
    - `blockers resume <id>` clears the blocker AND its retry_budget entry but **leaves the cursor at the failing position**; the next reader/drain/bootstrap pass re-processes the event normally with a fresh retry-budget counter. Resume does **not** advance the cursor.
    - `blockers quarantine <id> --reason <text>` is the **only** path that advances the cursor without processing — auditable, increments `lifecycle_quarantined_total`. Clears the blocker AND its retry_budget entry.
  - retry_budget cleanup: any direct-FATAL blocker write (fetchLive FATAL in handleEvent, K-exhausted FATAL_EVENT_ERROR in deleteAndResolve, fetchLive FATAL in drainPending) calls `clearRetryBudget(stream_key)` after the durable blocker write commits — verify no stale retry_budget entries remain after a sequence like ERR → ERR → FATAL.
  - Operator resolution paths all clear retry_budget alongside the blocker record (verify via post-resolution inspection of `_reader/retry_budget`).
  - Predicate-stream lag fallback: simulate predicate-cursor lag past `metaLogRetention × 0.95` for some shard while original-write streams remain healthy; verify all predicate-sensitive rules in affected buckets are durably promoted to `mode = scan_only` with `degraded_reason = LAG_HIGH`; verify rules without tag/size filters are unaffected. Verify `s3.lifecycle.reseed -predicate` re-enables them via the `force_reseed_predicate` path.
  - Sustained `RETRY_LATER` (transient repeated past a threshold) eventually upgrades to `BLOCKED` so the shard doesn't silently lag forever; verify the upgrade fires and the lag-correctness fallback then promotes the rule to scan_only.

## Phase 3 — Shared cluster reader + delay-group sweeps (non-versioned)

- Implement `s3.lifecycle.read` task: cluster singleton, subscribed to one filer at a time. `Filer.CollectLogFileRefs` returns chunk refs in directory order; the worker uses `pb.ReadLogFileRefsWithPosition` (Task #19) to heap-merge per-filer chunks by event ts client-side, with per-shard `MessagePosition` skip filtering. Worker reads chunk bodies in parallel from volume servers. Cursors are per-filer-shard: `last_processed_original: map<delay_seconds, map<filer_id, MessagePosition>>` and `last_processed_predicate: map<filer_id, MessagePosition>` — `LogEntry.Offset` is per-buffer (per filerId), not globally unique, so per-shard cursors are required. Resume skips events with `(ts, offset) <= cursor[filer_id]` (strict `<=`). Listing uses the low-water-mark across per-filer cursors. New shards seeded lazily via `seedNewlyDiscoveredShardsLazily` using `EarliestRetainedPositionPerShard` (Task #19), and seeding clears any matching `tail_drained_streams` marker. Tail-drain GC at task entry uses `RetainedLogRangePerShard` (Task #19) to remove cursor entries where `cursor >= range.latest`, record the matching `tail_drained_streams` key, and downgrade to `scan_only` with `LOST_LOG` if logs vanished before a stream caught up. Failover to another filer endpoint on connection error; per-filer cursor map is portable. Periodic checkpointing (default every 1000 events or 5s) bounds crash redo.
- **Tail-drain tests required in this phase**: (a) cursor at exactly `range.latest` for a shard → tail-drained, entry GC'd, matching `tail_drained_streams` key updated, low-water-mark recovers; (b) cursor before `range.latest` → not GC'd, entry preserved; (c) shard removed from cluster but logs still retained → not GC'd until each stream cursor catches up to `range.latest`; (d) verify low-water-mark advances after GC (otherwise the tail-drain wasn't effective); (e) **lost-log**: shard's logs pruned with a stream cursor still behind `range.latest` and no matching `tail_drained_streams[...]` marker → cursor NOT GC'd, all reader-driven rules promoted to `mode = scan_only` with `degraded_reason = LOST_LOG`, `lifecycle_lost_log_total{filer_id}` increments, warning log emitted; verify `s3.lifecycle.reseed -ack-lost-log --reason <text>` is the only path that clears the lost-log cursor entries and degraded flag together; (f) cross-stream guard: a tail-drained 30-day original stream does not permit GC of the same filer's 60-day original stream or predicate stream; (g) rejoin guard: when a previously tail-drained shard reappears and is lazily seeded, the matching `tail_drained_streams` marker is cleared.
- **Retention mode-gate tests** (per `eventLogHorizon` table, kinds beyond age-based): inject a rule of each reader-driven kind and toggle `metaLogRetention` above and below `eventLogHorizon(rule) + bootstrapLookbackMin`; verify the rule is `event_driven` above and `scan_only` with `degraded_reason = RETENTION_BELOW_HORIZON` below. Cover `EXPIRATION_DAYS`, `NONCURRENT_DAYS`, `ABORT_MPU`, `NEWER_NONCURRENT`, `EXPIRED_DELETE_MARKER`. Verify `EXPIRATION_DATE` bypasses the gate regardless of retention.
- Engine drives event routing: `engine.MatchOriginalWrite(event, delay=D)` and `engine.MatchPredicateChange(event)`.
- **One sweep per delay group** (e.g. `1d`, `30d`, `60d`, `90d`); each sweep advances `reader_state.last_processed_original[D.seconds]` (a `map<filer_id, MessagePosition>` — per-shard cursors, not a single position). Single predicate-change sweep clamped to `min(now - smallDelay, now - flushSafetyLag)` advances `reader_state.last_processed_predicate` (also per-shard).
- One filer fetch per event (`fetchLiveEntryOnce`), reused across all candidate rules.
- Per-event resolution-or-stop: `Resolved` outcomes advance the cursor; `TRANSPORT_ERROR` halts the batch and the cursor stays.
- Engine refreshes when the reader observes a lifecycle-xattr `UpdateEntry` event.
- `s3.lifecycle.drain` task drains per-rule pending exceptions.
- Tests with fixture meta logs:
  - 100 buckets × 5 rules — engine routes events with O(matched-rules) per-event candidate lookup. Verify log I/O scales with `events × distinct_delay_groups` (a small handful of passes), not `events × rule_count` (500x).
  - Original-write events sweep per delay group; rules with same delay share a sweep.
  - Predicate-change pass uses one near-now cutoff for all rules.
  - Engine rebuild on lifecycle PUT observed in log.
  - Late tag-add → predicate pass picks up promptly; rare exception path writes pending.
  - SCAN_AT_DATE rule never adds to pending until rule.date.
  - Rule edit creates new `<new_hash>` dir; unrelated rule's pending unaffected.
  - `SKIPPED_OBJECT_LOCK` → counter increment, log, advance (no pending, no blocked).
  - `TRANSPORT_ERROR` → batch stops; reader resumes from same cursor on next task.
  - Original cursor trails `now - delay`; predicate cursor stays at `now - smallDelay`.

## Phase 4 — Strip `updateEntriesTTL` from `PutBucketLifecycleConfiguration`

- **Now safe**: worker has been the source of truth for at least one release.
- Remove `updateEntriesTTL` and `filer.conf` mutation from `PutBucketLifecycleConfigurationHandler`.
- Reconcile-on-PUT: preserve `bootstrap_complete=true` and counters for unchanged `rule_hash`; new hashes start with `bootstrap_complete=false` (engine_state=pending_bootstrap). Reader cursors live in cluster `reader_state` and are unaffected.
- Regression test: PUT on a 10k-entry bucket completes under 1s.
- Old `TtlSec`-stamped entries continue to work via the filer read-time check; worker takes over going forward.

## Phase 5 — Versioning + MPU

- Implement `ruleAppliesToEntryShape` for `.versions/` paths and the current-pointer directory entry.
- Discover successor's becoming-non-current time from `event.TsNs` (or by listing the `.versions` dir for the next-newer version's mtime if event came from the dir attr update).
- `ExpiredObjectDeleteMarker` triggered when a delete-marker is the sole survivor.
- `AbortIncompleteMultipartUpload` driven by events under `.uploads/`.
- Tests covering: new version PUT, delete marker creation, non-current expiration, expired-delete-marker, MPU abort.

## Phase 6 — Multi-filer durable replay (mostly done in Phase 3)

The shared-reader design (Phase 3) already reads from the shared filer namespace, so multi-filer correctness is largely subsumed. This phase fills in:

- Per-shard cursors keyed by `filerId` are already in the reader-state proto (Phase 3).
- Filer-set discovery: list `/topics/.system/log/<date>/` and observe filerId suffixes (Phase 3).
- Tail-drain GC implementation using `RetainedLogRangePerShard` (Task #19): cursor entries where `cursor >= range.latest` are removed at task entry AND the stream-specific key is recorded in `reader_state.tail_drained_streams`. Phase 3 tests verify single-shard tail-drain; Phase 6 adds multi-filer convergence tests:
  - Two filers actively writing, racing writes against the same key — events from both shards merged correctly, per-shard cursors advance independently.
  - Filer A departs (stops writing); its logs are still retained → cursor for A continues to advance until A's `range.latest`; not GC'd before then.
  - Filer A reaches `range.latest` for one stream (e.g. original delay 30d), then its logs are pruned: probe returns no range, but the matching `tail_drained_streams[(ORIGINAL,30d,A)]` entry is set → that stream cursor is GC'd as safe; no degradation.
  - Filer A later reappears and writes retained logs for that same stream: lazy seeding creates a new cursor and clears `tail_drained_streams[(ORIGINAL,30d,A)]`, so the old safe-drain marker cannot mask a future lost-log event.
  - **Lost-log: Filer A's logs are pruned BEFORE another stream cursor reaches `range.latest`** (probe returns no range AND the matching `tail_drained_streams[...]` entry is unset): the reader does NOT GC that cursor; instead, all reader-driven rules are promoted to `mode = scan_only` with `degraded_reason = LOST_LOG`; `lifecycle_lost_log_total{filer_id="A"}` increments; warning logged. Verify cursor stays put until `s3.lifecycle.reseed -ack-lost-log --reason <text>` clears the lost-log cursor entries and degraded flag.
  - Worker restart mid-merge → resumes per-filer cursors correctly; both shards continue from their persisted positions; `tail_drained_streams` survives restart.
  - Failover to a different primary filer mid-batch — cursor portable, no events skipped or replayed.

## Phase 7 — Observability

- Shell `s3.lifecycle.status` command.
- Admin UI tile.
- `X-SeaweedFS-Lifecycle-LastTick` header.
- Prometheus metrics.

## Phase 8 — `scan_only` cadence tuning + operator hooks

(The gate itself lands in Phase 2; this phase is observability and tuning.)

- Surface `scan_only` mode in shell `s3.lifecycle.status` and admin UI.
- Configurable cadence per rule via shell command (operator override of the per-kind safety-scan cadence default).
- Document in bucket-lifecycle docs: when retention < TTL, the rule runs as periodic scan; trade-offs explained.
- Manual "scan now" via shell command for unscheduled runs.

## Cut points

- Phase 0–2 alone delivers, on **non-versioned** buckets only:
  - Currently-due deletions for age-based rules via bucket-level bootstrap that calls `LifecycleDelete` inline (no pending/drain in this path).
  - Date-rule deletions (`SCAN_AT_DATE` mode: a single scheduled bucket-level bootstrap at `rule.date` deletes all matches inline).
  - Periodic safety scans (re-run bootstrap on the per-kind cadence) catch drift over time.
  Not-yet-due age-based discovery via meta-log events arrives in Phase 3 (the reader). Versioned buckets remain unhandled until Phase 5; the worker skips them with a logged warning until then. The existing back-stamp keeps simple `Expiration.Days` working in parallel.
- Phase 3 brings steady-state event-driven expiration. The reader is a cluster singleton subscribed to one filer; the worker heap-merges per-filer log chunks client-side via `pb.ReadLogFileRefsWithPosition` (Task #19) with per-shard `MessagePosition` cursors. Multi-filer clusters work the same as single-filer clusters from this point forward.
- Phase 4 makes `PUT` constant-time. Required dependency: Phase 2 + Phase 3 shipped and stable.
- Phases 5–8 complete the surface.

---

# Test Plan

Four layers, mapped to existing repo patterns. Each layer has a different cost/scope. Test work lands in the same PR as the code it covers; CI gates are by layer.

## Layer 1 — Unit (pure Go, milliseconds)

Location: `weed/s3api/s3lifecycle/*_test.go` next to source. Standard table-driven Go tests. No fakes, no goroutines.

| File | Coverage |
|---|---|
| `evaluate_test.go` | `EvaluateAction(rule, kind, info, now) → action`: every action kind × (eligible, not-eligible) × every filter shape (prefix, tag, size, And-combinations). AWS edge cases: zero days, future date, empty prefix, empty/duplicate Rule.ID. Multi-action rule: each kind independently. |
| `rule_hash_test.go` | Semantically-equivalent rules hash equal. Reordered tags hash equal. Different rules hash different. ID changes don't affect hash. |
| `due_at_test.go` | `computeDueAt(rule, info)` per kind. Boundary: mtime exactly at threshold, date in past, NewerNoncurrent at exactly N versions. |
| `min_trigger_age_test.go` | `MinTriggerAge` per kind. `EXPIRATION_DATE` returns sentinel; `NEWER_NONCURRENT` returns small. |
| `event_log_horizon_test.go` | `EventLogHorizon` per kind. Age kinds return their day count; `NEWER_NONCURRENT` and `EXPIRED_DELETE_MARKER` return `smallDelay`; `EXPIRATION_DATE` is not called and panics or returns sentinel. |
| `identity_test.go` | `EntryIdentity` builder produces stable hash for same Extended; differs when tags change. Snapshot-restore (different head fid, same mtime) detected. |

Run: `go test ./weed/s3api/s3lifecycle/...`. Phase 1 lands these alongside the function.

## Layer 2 — Component (in-process harness, seconds–minutes)

Location: `test/plugin_workers/lifecycle/` — new package, mirrors `test/plugin_workers/vacuum/` and `test/plugin_workers/volume_balance/`.

Reuses `test/plugin_workers/framework.go` (in-process plugin admin gRPC + worker). Extends it with:

1. **Meta-log fixture loader** — takes a Go slice of `{filerId, tsNs, eventType, oldEntry, newEntry}`, writes into a test filer's `/topics/.system/log/` in the on-disk format. Used by all reader/watermark tests.
2. **Controllable clock** — `s3lifecycle.Now` wired to a test func; advance without sleeping.
3. **Fake S3 LifecycleDelete server** — records calls, returns scripted outcomes, so component tests assert exact RPC sequences.

Files and the cases each owns:

```
test/plugin_workers/lifecycle/
  framework.go                  # harness extensions
  detection_test.go             # detector emits the right tasks given state
  bootstrap_test.go             # Phase 2
  reader_test.go                # Phase 3
  drain_test.go                 # Phase 3
  versioning_test.go            # Phase 5
  multi_filer_test.go           # Phase 6
  metadata_only_test.go         # Phase 2b
  s3_discovery_test.go          # Phase 2
```

Cases:

**Bootstrap** — inline-delete for age rules (currently-due deleted, not-yet-due skipped), `SCAN_AT_DATE` for date rules (date-triggered bucket walk), kill-resume via `last_scanned_path`, walk-cursor drift covered by past-`MinTriggerAge` cursor-seed, scan_only gate, per-rule `bootstrap_complete` set on completion before engine activation.

**Reader / event-driven** — original-write threshold, late tag-add via predicate pass, two watermarks advance independently, cross-filer heap-merge, Extended-equal events filtered, lifecycle-XML xattr update reloads trie.

**Drain / `LifecycleDelete`** — identity CAS reject, per-rule policy CAS (unrelated edit doesn't poison), `SKIPPED_OBJECT_LOCK` → log + counter + advance (no pending, no blocked).

**Metadata-only delete** — `TtlSec > 0` + matching `filer.conf` rule → server invoked with `skip_chunk_delete=true`; intercept fake-filer chunk-deletion queue to assert no enqueue.

**Worker–S3 discovery** — all endpoints unhealthy → continuation-hint backoff, no progress, no data loss; flapping endpoint rotates; S3 down at startup → worker boots, drains catch up on recovery.

Run: `go test ./test/plugin_workers/lifecycle/...`. Phase mapping: each phase ships its half.

## Layer 3 — Real-server integration (AWS SDK + live `weed server`, minutes per scenario)

Location: `test/s3/lifecycle/` — new, copy of `test/s3/retention/` shape (Makefile-driven binary build, server start/stop, AWS SDK clients).

```
test/s3/lifecycle/
  Makefile                          # mirrors test/s3/retention/Makefile
  test_config.json
  s3_lifecycle_basic_test.go              # PUT/GET/DELETE
  s3_lifecycle_put_perf_test.go           # 60k-object PUT < 1s — original ticket regression
  s3_lifecycle_age_test.go                # ExpirationDays end-to-end
  s3_lifecycle_date_test.go               # ExpirationDate end-to-end
  s3_lifecycle_tag_filter_test.go         # tag/size filters
  s3_lifecycle_late_tag_test.go           # tag added at age 30d on 60d rule expires at AWS-correct time
  s3_lifecycle_versioning_test.go         # versioned bucket flows
  s3_lifecycle_mpu_test.go                # AbortIncompleteMultipartUpload
  s3_lifecycle_object_lock_test.go        # retain-until + legal hold interactions
  s3_lifecycle_metadata_only_test.go      # volume-TTL fast path
  s3_lifecycle_scan_only_test.go          # short-retention forces scan_only
```

Time compression: existing build tag `lifecycle_10sec` (`weed/util/constants_lifecycle_interval_10sec.go`) treats 1 day as 10 seconds. Verify it covers `MinTriggerAge` math; add a `bootstrapLookbackMin` constant in the same files so it scales too. Makefile target:

```
test-fast: build-weed-10sec
	go test -tags lifecycle_10sec -timeout 15m ./...
```

Run: `make -C test/s3/lifecycle test-with-server`. Phase mapping: end-to-end tests land in the same phase as the capability they cover.

## Layer 4 — Multi-filer convergence (Phase 6 only)

Location: `test/plugin_workers/lifecycle/multi_filer_test.go` (component) plus a multi-filer scenario in `test/s3/lifecycle/` if needed.

Patterns to copy: `test/multi_master/`, `test/metadata_subscribe/` — both spin up multi-node clusters in-process.

Cases:
- Racing writes to the same key on different filers, **including events with identical `event.ts`** — reader merges per-filer logs by `(event.ts, filer_id, event.offset)` (deterministic ordering across shards because `event.offset` is per-buffer/per-filer and not globally unique). Test asserts the same merge order across runs and that equal-ts events from different filers don't swap relative position.
- Worker restart mid-merge → resumes per-filer watermarks correctly.
- Filer leaves cluster → topology refresh skips it; remaining filers' logs continue.

## CI gating

| Layer | When | Time |
|---|---|---|
| `make test-unit` | every push | < 10s |
| `make test-component` | every push | < 2m |
| `make test-s3-lifecycle` (Layer 3, 10sec build tag) | PRs touching `weed/s3api/s3lifecycle/**`, `weed/worker/tasks/s3lifecycle/**`, or `weed/s3api/s3api_internal_lifecycle.go` | ~20m |
| `make test-multi-filer-lifecycle` | nightly | ~30m |

CI must require `test-component` and `test-s3-lifecycle` green before merging Phase 4 (the PUT-strip). All layers green required for Phase 5+.
