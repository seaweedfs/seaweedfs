# Volume-ID reference lookup and safe cleanup

**Status:** design in progress; no storage-format or destructive behavior change is approved.
**Issue:** [#10711](https://github.com/seaweedfs/seaweedfs/issues/10711)
**Related:** [#7102](https://github.com/seaweedfs/seaweedfs/issues/7102), [#12296](https://github.com/seaweedfs/seaweedfs/issues/12296)

## Problem statement

Filer entries contain `FileChunk` references to volume/file IDs. Removing a physical volume does not by itself establish that every Filer reference to that volume has been removed. The resulting state can include namespace entries whose content is unavailable and deletion retries against missing chunks. At very large namespace sizes, finding all affected paths by walking the Filer tree is expensive.

The primary objective is not to silently delete file metadata. It is to find and report references to a volume efficiently and safely, then let an explicit operation apply a policy that has been agreed upon.

## Existing behavior (source checked at checkout `c2ca83c`)

- `weed shell volume.fsck` already has `-findMissingChunksInFiler`, `-reallyDeleteFilerEntries`, `-volumeId`, and `-purgeConcurrency` options.
- The missing-volume path calls `collectFilerFileIdAndPaths`, traverses Filer metadata from `/`, resolves chunk manifests, and records paths/FIDs per volume ID in temporary files. `-volumeId` narrows selected volumes but does not currently avoid the Filer traversal needed to discover references.
- The destructive path is explicitly expert-only and requires the operator to ensure expected volumes are connected. Manifest resolution failures block purging so unknown sub-chunks are not incorrectly treated as orphaned.
- `FilerStore` exposes generic entry CRUD, directory listing, and KV methods. A separate optional `EntryScanner` capability supports consistent snapshots for stores that implement it. There is no generic volume-reference query in the inspected interface.

These facts make this an optimization and lifecycle-safety problem on top of existing recovery functionality, not a request to invent cleanup from scratch.

## Concrete scenarios and expected behavior

### A. Explicitly retired volume, single-chunk file

A file entry references only volume V; an operator plans to retire V.

- A read-only preflight identifies the exact path and chunk reference.
- The tool must not mutate metadata by default.
- Physical deletion must not proceed through a Filer-aware workflow until the operator chooses a documented disposition (delete the file through Filer, move/replicate its content, or accept data loss).

### B. File spans multiple volumes; one volume is missing

A file entry references chunks on V and W, but V is gone.

- The report must identify the file as affected and show all references, not infer that all its data is gone.
- The implementation must not silently remove the entire file or drop only the V chunk without a defined file-integrity policy.
- Destructive cleanup remains blocked until the chosen behavior is explicit.

### C. Reference changes during lookup

A file is overwritten, renamed, or deleted while a reference scan is running.

- A stale index result must not authorize deletion.
- Before destructive action, re-read the current entry and verify the target volume is still referenced.
- Snapshot-capable scans should be used where available; non-snapshot backends need an explicit consistency limitation or a conservative abort/recheck strategy.

### D. Interrupted cleanup or index inconsistency

The process or store fails partway through a batch, or the auxiliary index is stale.

- Re-running must be idempotent and resumable.
- The auxiliary index is not authoritative. An index error, incomplete rebuild, or unresolved manifest must fail closed for destructive operations.
- A scan/rebuild/reconciliation path must exist before an index can be relied upon.

## Design principles

1. Filer metadata remains authoritative; any volume-ID index is a derived lookup accelerator.
2. Detection, policy selection, and mutation are separate phases: **preflight → explicit decision → revalidate → apply → report**.
3. Default operation is read-only. Destructive behavior requires an explicit flag and must list affected paths/counts before mutation.
4. The operation is bounded, idempotent, resumable, and reports per-item failures.
5. Unknown or incomplete chunk-manifest state blocks destructive cleanup.
6. Index maintenance must cover create, update/overwrite, append/chunk replacement, metadata delete, directory recursive delete, replication/sync, and any repair/import path that writes entries.
7. Backends that cannot maintain the index consistently must retain the safe scan fallback; no backend may silently claim indexed completeness.

## Alternatives and tradeoffs

### Keep full scan only

Lowest implementation risk and backend-neutral. It already exists, but has O(number of Filer entries) cost for a targeted volume.

### Add a derived reverse index

Can reduce candidate lookup cost substantially, but every metadata mutation must maintain it. A stale/missing index can omit references, so it cannot by itself make physical deletion safe. Backend transactions, multi-Filer deployments, store migrations, and rebuild/reconciliation are the hard parts. A generic `FilerStore` API provides uniform semantics but requires implementation or fallback behavior across supported stores; a LevelDB-only implementation is smaller but creates backend-dependent performance/correctness expectations.

### Filer-aware volume-delete guard

A high-level delete operation can preflight references and refuse deletion while references exist. This gives the clearest safety contract but still needs a scalable lookup, and the existing low-level volume-server delete RPC may intentionally remain independent of Filer.

### Event-maintained external index

Operationally useful for custom workflows but may lag or lose events unless consumers checkpoint and reconcile. It must not become the authorization source for a destructive deletion without a verified recovery protocol.

## Proposed staged implementation

1. **Confirm semantics with maintainers:** decide whether volume deletion with live Filer references is refused, requires migration, or is an explicit data-loss operation. Decide the handling of multi-volume files.
2. **Baseline the current recovery path:** add/strengthen tests for `-volumeId`, single-chunk and multi-chunk entries, manifests, concurrent entry mutation, unresolved manifests, and dry-run versus destructive mode. Record scan complexity and output.
3. **Define an optional store capability:** if maintainers agree to indexed lookup, add a capability with explicit completeness/snapshot semantics rather than adding an unconditional method to `FilerStore`. Keep a scan fallback.
4. **Prototype one backend behind the capability:** update the derived index in the same atomic boundary as entry changes where possible; otherwise mark/reconcile dirty state and fail closed. Include migration/rebuild tooling before enabling indexed deletion.
5. **Add read-only audit mode first:** compare index results with authoritative entry reads and report mismatch; do not delete through the index until reconciliation and concurrency tests pass.
6. **Enable explicit batched cleanup only after safety proof:** revalidate each path, use current fsck deletion semantics, resume safely, and show all skipped/failed items.

## Acceptance criteria

- The tool can report every Filer entry referencing a selected volume under its documented consistency model.
- A stale or incomplete index cannot cause a live entry to be deleted or a volume to be declared safe to remove.
- Multi-volume files are handled according to an explicit policy and covered by tests.
- Existing full-scan behavior remains available as a fallback and correctness oracle.
- Failure, restart, concurrent update, unresolved manifest, and non-indexed backend behavior are tested and documented.
- Targeted lookup cost is measured against the current full-tree scan on representative metadata sizes.

## Open questions blocking destructive implementation

1. Should a Filer-aware volume delete refuse any live reference, or offer a migration/deletion workflow?
2. For a file with one missing chunk but surviving chunks, should cleanup retain a broken entry, remove the whole entry, or offer repair/export first?
3. Which Filer stores must support indexed lookup at launch, and is a clearly documented full-scan fallback acceptable?
4. Can store mutations and derived-index updates share a transaction for each supported backend? How are multi-Filer writes serialized?
5. What is the authoritative signal that a volume is intentionally retired versus temporarily unavailable?

## Scope boundary

This note does not authorize automatic deletion of affected Filer entries, a storage-format migration, or a claim that all Filer backends can provide atomic reverse-index maintenance. Those decisions require source-level backend review and maintainer agreement.
## Chosen direction (design decision)

The initial contribution targets **safe, fast reference discovery in `volume.fsck`**, not automatic cleanup inside the low-level volume-server delete RPC. The latter can operate without a Filer and cannot reliably enforce a Filer-wide invariant.

### Operational contract

1. `volume.fsck` remains the authoritative recovery tool. Its current full-scan behavior remains the correctness fallback.
2. A volume-ID reverse index is a derived accelerator, never the source of truth. `-volumeId` must narrow lookup work, not merely filter results after walking every Filer entry.
3. Discovery is read-only by default. Any purge remains an explicit expert operation and must state that removing an entry for a multi-volume file also discards the file's surviving chunks. The existing whole-entry purge semantics are retained; this proposal does not invent partial-file repair.
4. An indexed result is re-read from the current Filer entry before purge. An incomplete, stale, rebuilding, or unsupported index must never be reported as a complete result. In that case the tool either uses the existing full scan or refuses destructive operation with a clear error.
5. Low-level `volume.delete` remains separate. A future Filer-aware preflight may be added, but it must not be coupled to the volume-server RPC or silently mutate namespace metadata.

### Index ownership and rollout

- Expose reverse lookup as an optional `FilerStore` capability with explicit completeness status; preserve compatibility through the existing scan fallback.
- Start with a backend that can update the derived index atomically with entry mutations. Do not claim coverage for other stores until their mutation/transaction semantics are audited.
- Include a rebuild/reconcile operation and a readiness marker. Enable indexed read-only lookup only after rebuild is complete; enable indexed purge only when completeness is still guaranteed.
- Track both ordinary chunk FIDs and recursively resolved chunk-manifest FIDs. Preserve the path and hard-link identity needed by fsck's existing deletion queue.
- Index create, overwrite/update, append or chunk-layout replacement, delete/recursive delete, filer sync/import/repair writes. Any missed write path invalidates completeness and forces fallback.

### Why this direction fits the project and external practice

SeaweedFS already separates Filer namespace metadata from volume-server needle storage, and `volume.fsck` already performs the cross-layer reconciliation. Extending that reconciliation with an optional index is less invasive than making each volume server aware of every Filer. This follows the GFS/HDFS principle that metadata ownership governs namespace deletion and data reclamation, and the JuiceFS pattern of staged deletion/GC. Ceph's orphan-list tooling is a useful caution: discovery results should be audited before large destructive cleanup, especially when an index may be incomplete.

### First implementation boundary

Before production code, complete a mutation-path audit for the candidate backend and add tests for index deltas on insert/update/delete, hard links, manifests, multi-volume entries, rebuild, and interrupted updates. The first code slice should be the optional capability plus a read-only indexed lookup, with scan cross-check tests. Purge must stay on scan/fallback until index completeness, restart recovery, and concurrent mutation behavior are proven.

This decision chooses the conservative answer for unresolved questions: **do not delete automatically; report first; preserve current explicit whole-entry purge behavior; fail closed or fall back when index completeness is unknown.**
## Source audit: implementation constraints

The source audit expanded beyond `command_volume_fsck.go` and confirms that an index must account for store routing and mutation paths:

- `weed/filer/filerstore.go`: `FilerStore` has CRUD and separate KV methods; no transaction contract for one entry mutation plus an index mutation. `EntryScanner` is optional and provides a consistent scan only for stores that implement it.
- `weed/filer/leveldb2/leveldb2_store.go`: LevelDB2 is the default store, but its `BeginTransaction`, `CommitTransaction`, and `RollbackTransaction` are no-ops. `InsertEntry`/`UpdateEntry` write the canonical entry with one `Put`; `DeleteEntry` uses one `Delete`; `DeleteFolderChildren` uses a batch. Therefore `FilerStore.KvPut` cannot be assumed atomic with entry CRUD.
- `weed/filer/leveldb2/leveldb2_store_kv.go`: KV items are sharded independently by their final key byte. A new index key must deliberately colocate with the canonical entry if it is to share a LevelDB batch; current generic KV partitioning does not guarantee this.
- `weed/filer/filerstore_wrapper.go` and `weed/filer/filerstore_translate_path.go`: entries may be routed to path-specific stores and paths may be translated. An index attached only to the default LevelDB2 instance is incomplete for such deployments.
- `weed/filer/filer.go`, `filer_delete_entry.go`, `filer_lazy_remote_listing.go`, `meta_aggregator.go`, `meta_replay.go`, and `filer_sync.go`: writes and deletes also occur through directory promotion/restoration, lazy remote updates, metadata replay, and sync/import, not just ordinary upload.
- `weed/filer/filerstore_hardlink.go`: hard-link metadata has a separate KV path. A path-level index must use resolved logical chunks and preserve link identity; raw chunks on the path entry may not be the complete reference set.

### Revised implementation choice

Do **not** add an online reverse index to LevelDB2 in the first code patch. Its no-op transactions and eight independently partitioned DBs mean a correct index needs backend-local atomic batches, per-key mutation serialization, a rebuild/readiness protocol for existing data, and explicit behavior during path-specific routing and hard-link resolution. A superficially working index can silently omit references after a crash or after one bypass mutation path.

The safe first engineering deliverable is a backend capability/rebuild prototype only after its online-vs-offline contract is explicit. Until then, keep the production path unchanged and make a measured scan/rebuild prototype or test harness outside destructive fsck behavior. The acceptance bar before indexed purge remains: no full scan per targeted lookup, no false complete result during rebuild or after a failed mutation, and existing full-scan output agrees with indexed results on a stable snapshot.

### Questions to resolve in code review

1. Can LevelDB2 use one `leveldb.Batch` for the canonical entry plus all index delta rows while serializing concurrent updates to that entry?
2. How is a full rebuild performed at 50M entries without either pausing writes for an unacceptable duration or publishing a mixed-time index?
3. Should query scope span all path-routed Filer stores by invoking each backend's optional capability, with per-store fallback for unsupported stores?
4. What canonical chunk expansion is required for hard links and nested chunk manifests, and can that expansion be performed from the same metadata snapshot?
5. What should `-volumeId` promise on unsupported or unready index coverage: automatic full-scan fallback, explicit "slow fallback" output, or a fail-closed error for purge?
## First implemented slice

The first bounded change is in `weed/shell/command_volume_fsck.go`: `-volumeId` continues to reject IDs absent from the master in normal fsck modes, but `-findMissingChunksInFiler` may now select such IDs explicitly. The Filer traversal can then report and, only when the existing expert purge option is enabled, queue entries whose chunks refer to the removed volume. This uses the established fsck reconciliation path and does not add an index or change the default destructive policy.

Regression coverage in `weed/shell/command_volume_fsck_missing_volume_test.go` checks that the scan selects references for the requested missing ID, ignores unrelated missing IDs, and preserves strict unknown-ID validation outside the reverse Filer scan.

This makes targeted cleanup after volume removal possible but **does not reduce the full Filer scan cost**. Persistent reverse-index work remains deferred until its atomic update and rebuild/readiness protocol are proven. Verification: `go test ./weed/shell -run "TestVolumeFsck(FindMissingFilerEntriesForDeletedSelectedVolume|SelectedVolumeIDValidation)$" -count=1` and `go test ./weed/shell` both pass.
### Live three-service scenario

After the unit/package tests, a disposable local cluster with separate real master, volume, and Filer processes was exercised end to end:

1. PUT `/fsck-probe.txt` through the live Filer and confirm the Filer metadata contains a chunk FID.
2. Use `weed shell volume.delete` under the required lock to remove the referenced physical volume.
3. Run `volume.fsck -findMissingChunksInFiler -volumeId=<deleted-id> -cutoffTimeAgo=0`.
4. Verify fsck accepts the ID absent from the master and reports `/fsck-probe.txt` as referencing a missing volume. No purge flag was used; data/metadata were not modified by fsck.

The first attempt without `-cutoffTimeAgo=0` did not report the just-written file, which is expected: the default 5-hour cutoff protects uploads whose metadata/volume state may still be settling. The test therefore explicitly disables the cutoff only in this disposable scenario. Fsck also reported a `/topics/.system/log/...` entry referencing the same deleted volume, demonstrating that volume retirement can affect system/topic metadata as well as the user path. This is why the default remains report-only and why cleanup must enumerate every affected path before an expert purge.
Observed run: a three-process local cluster successfully wrote the probe (HTTP 201), removed the selected volume through `weed shell volume.delete`, then ran fsck. The read-only fsck output reported the deleted ID and `/fsck-probe.txt` as `volume not found`; it also reported one `/topics/.system/log/...` reference. No purge option was used. This confirms both that the targeted missing-ID path works with real master/Filer/volume processes and that a physical volume can contain references for more than the user object.

The first live attempt omitted `-cutoffTimeAgo=0`; it did not report the new file because the default five-hour cutoff intentionally excludes recent chunks. The final run set zero cutoff only in an isolated disposable dataset. The master/volume/Filer processes were stopped and their temporary data directory removed afterward.
The repeatable opt-in test is `weed/shell/command_volume_fsck_live_test.go` (build tag `integration`). It starts real master, volume, and Filer processes using isolated temporary directories and reserved HTTP/gRPC port pairs. It checks the default five-hour cutoff does not report the just-written probe, the explicit zero-cutoff scan does report the deleted volume's probe path, and report-only fsck leaves the Filer entry intact. Run it with:

```sh
go test -tags integration ./weed/shell -run TestVolumeFsckFindsReferenceAfterRealVolumeDeletion -count=1 -v
```

No Toxiproxy or Chaos Mesh was needed for this scenario: the failure under test is a confirmed physical volume deletion, which is exercised directly against the real services. A future test for intermittent network partitions should use a fault proxy only if it needs deterministic latency/drop control; that is distinct from this delete-and-reconcile contract.

## Edge-case hardening added after review

- **Master absence is ambiguous:** an ID absent from the master can mean either intentional retirement or a temporarily unavailable server. Read-only `-findMissingChunksInFiler` reports this distinction explicitly. If `-reallyDeleteFilerEntries` is used for selected IDs absent from the master, the new `-confirmMissingVolumeDeletion` acknowledgement is required after the operator verifies expected volumes are online.
- **Collection scope:** if a missing volume ID is combined with a non-global `-collection` filter, the command refuses to guess the volume's collection after its metadata is gone.
- **Unresolved manifest:** an entry whose manifest cannot be resolved is reported but is not queued for Filer-entry deletion. The scanner cannot prove what child FIDs the manifest contains.
- **Multiple missing chunks in one entry:** at most one Filer DELETE is queued per entry path, while each missing FID is still reported. Hard-linked distinct paths retain the existing same-worker serialization.

The live integration scenario also stops the volume-server process after a second upload while preserving the Filer entry. The destructive scan must either fail because the selected volume remains master-listed but unreadable, or require `-confirmMissingVolumeDeletion` if the master has already removed it. In both cases the Filer entry remains. This models fail-stop service loss; Toxiproxy/Chaos Mesh is reserved for a separate network-blackhole test where the server remains alive but cannot be reached.

## Latest edge-case policy

- A selected volume ID missing from the master is ambiguous: it may have been retired intentionally or lost temporarily during an outage. The read-only reverse scan is allowed with a warning. A targeted `-reallyDeleteFilerEntries` purge for such IDs additionally requires `-confirmMissingVolumeDeletion`; otherwise fsck refuses before scanning.
- A non-global `-collection` filter is rejected when a selected ID is absent because the deleted volume's collection cannot be verified.
- When one Filer entry has several missing chunks, report each missing FID but queue the Filer path once. Do not queue that path if its chunk manifest could not be resolved.
- The opt-in server test now exercises both physical deletion and fail-stop volume-server loss. If the master still knows the volume, the unavailable index scan fails before any purge; if the master has dropped the ID, an unconfirmed purge is refused. In both cases the Filer entry is checked afterward.

The corresponding commands and tests have passed locally: `go test ./weed/shell` and `go test -tags integration ./weed/shell -run TestVolumeFsckFindsReferenceAfterRealVolumeDeletion -count=1 -v`.
### Collection-scope and outage semantics

When a selected missing ID is paired with a non-global `-collection` value, fsck rejects the selection because it cannot recover the removed volume's collection. For an unfiltered volume-ID list with a non-global collection filter, it still reports master-absent references but does not queue those paths for purge; they cannot be safely attributed to that collection. This distinction is covered by a live test.

The new `-confirmMissingVolumeDeletion` flag is required for an explicit targeted purge of IDs absent from the master. It does not prove the volume was intentionally retired; the operator must verify that expected servers are online. If the volume is still master-listed but unavailable, collecting its volume index fails before the Filer purge path.
The opt-in integration scenario now covers the destructive branch only after an explicit acknowledgement: a direct Filer entry referring to the retired volume is removed and returns HTTP 404 afterward, while a second entry with an unresolved manifest remains in the Filer. It also verifies that a server-stop scenario cannot purge the offline probe without the missing-volume acknowledgement, and that a non-global collection filter reports but does not enqueue the missing-volume path.
