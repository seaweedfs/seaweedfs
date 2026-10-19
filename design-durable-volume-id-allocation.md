# Design: Durable Volume-ID Allocation Boundary

Issue: https://github.com/seaweedfs/seaweedfs/issues/11763

## Problem

`Topology.NextVolumeId()` allocates `GetMaxVolumeId() + 1`. The high-water
mark is only as durable as the Raft state that carries it
(`MaxVolumeIdCommand`), and is otherwise rebuilt from volumes reported by
*connected* volume servers (`UpAdjustMaxVolumeId` on the heartbeat path).

If the master loses its Raft state while a volume server is offline, the
rebuilt bound misses that server's volumes. `NextVolumeId()` then reissues
ids the offline server still holds. When the server rejoins,
`SyncDataNodeRegistration` / `RegisterVolumeLayout` registers the duplicate
into a different collection's layout — two live volumes now share one vid in
different collections, and lookups become ambiguous.

`IsWarmingUp()` does not help: it only covers `WarmupPulseMultiplier`
heartbeat intervals after a leader change, and a permanently offline server
never checks in within that window.

### Invariant

A volume id, once allocated, must never be reissued while any volume server
might still hold a volume with that id. The allocation boundary must
therefore be durable somewhere that survives master Raft-state loss, and the
master must refuse to allocate when it cannot establish a safe boundary.

## Options considered

### A. Durable counter in an external store (etcd)

CAS a `max_volume_id` key in etcd (clientv3 is already an indirect
dependency via the filer store). Strictly correct and simple to reason
about.

Downsides: a new hard runtime dependency for deployments that do not already
run etcd — including the single-master / two-volume-server topology from the
issue, which is exactly the shape that hits this. It also does nothing for
clusters that do not opt in.

Verdict: viable as an *opt-in* backend (phase 2 below), not acceptable as
the only fix.

### B. Block growth until all previously-seen servers check in

Needs a roster of previously-seen servers — which lives in the lost Raft
state. And even with a roster, "not yet rejoined" is indistinguishable from
"decommissioned for good", so the block can be unbounded. Verdict: rejected
as specified; the freeze mechanism below keeps the spirit (fail closed on
*detected* loss, operator-gated) without pretending the roster is known.

### C. Federate the high-water mark to the volume servers (recommended)

The bound doesn't have to live on the master at all: every volume server can
carry a persisted copy, learned passively. The master echoes its current
`maxVolumeId` in every heartbeat response; each server folds it into a
durably persisted `max_volume_id_seen` and reports that value back in every
heartbeat. The allocation bound is then `max(raft state, local mirror, every
connected server's report)`.

A permanently offline server is no longer special: while it was up, the echo
already distributed its high vids to every *other* server, and any single
surviving veteran restores the bound on reconnect.

Verdict: recommended. No new infrastructure, monotonic by construction,
rolling-upgrade safe.

### D. Detection only (duplicate-vid alarm on registration)

Cheap, useful forensics, but contains nothing — the ambiguous lookup state
already exists by the time it fires. Verdict: include as a safety net, not
the fix.

## Design: four layers + optional external backend

### L1 — Master-side synchronous mirror

Write the bound to a plain file in the master meta dir *before* committing
to Raft:

- In `Topology.NextVolumeId()`, after computing `next` and before the Raft
  apply, persist `next` to `<mdir>/max_volume_id` (write tmp + rename +
  fsync). A Raft failure after the mirror write only over-estimates the
  bound — a skipped vid is safe.
- At startup (both Raft implementations), `UpAdjustMaxVolumeId(mirror)`
  before the server can become leader.

This is a one-file change of scope that already fixes every case where
`mdir` survives (ephemeral Raft state, `-resumeState=false`, lost snapshot).
It does not help when `mdir` itself is ephemeral — note the default is
`os.TempDir()`, so the docs/startup warning below matter.

### L2 — Federated high-water mark (core fix)

Protocol additions (`master.proto`):

```proto
message Heartbeat {
  ...
  // Highest volume id this server has ever observed, persisted locally.
  // Fed by HeartbeatResponse.max_volume_id echoes and by AllocateVolume
  // requests; never decreases.
  uint32 max_volume_id_seen = 28;
  // Topology id of the master cluster this server last heartbeated to.
  // Non-empty once the server has heard one echo; used to detect master
  // state loss.
  string last_seen_topology_id = 29;
}

message HeartbeatResponse {
  ...
  uint32 max_volume_id = 10;
  string topology_id = 11;
}
```

Volume server:

- Maintain `maxVidSeen` in `Store`, bootstrapped at load time to
  `max(local volume ids, local EC shard vids, persisted stamp)` — so even a
  server upgraded mid-incident reports a bound covering its own holdings.
- Persist `{max_volume_id_seen, last_seen_topology_id}` to a stamp file in
  each volume dir (`<dir>/.vid_stamp`, JSON, write-tmp-rename-fsync).
  Multiple dirs are independent filesystems; each keeps its own stamp and
  the reported value is the max across them. Corrupt/missing stamp = treat
  as 0 and log; local volume scan still floors the bound.
- Update the stamp (monotonic CAS) on:
  - every `HeartbeatResponse` carrying `max_volume_id` / `topology_id`;
  - every `AllocateVolume` request, *before* the volume files are created
    (this includes replica placements from `volume.copy` — a copied vid was
    still allocated once, so folding it in is correct);
  - local volume/EC-shard creation from any path.
- Report `max_volume_id_seen` and `last_seen_topology_id` in
  `CollectHeartbeat()`.

Master:

- `heartbeatResponse()` includes `MaxVolumeId: uint32(Topo.GetMaxVolumeId())`
  and `TopologyId`.
- On each heartbeat: `Topo.UpAdjustMaxVolumeId(heartbeat.MaxVolumeIdSeen)`
  — monotonic, idempotent, and safe against stale or downgraded reporters.
- Track `last_seen_topology_id` reports for L3.

Safety argument: after a total master state loss, the first heartbeat from
any veteran restores a bound ≥ every vid that was committed more than one
echo interval before the loss, because that vid was already echoed to — and
persisted by — every connected server. A vid committed *within* the final
echo interval is known only to its allocate target (which stamped it before
creating the volume), so a collision additionally requires that target to be
the one that stays offline and to have actually received the allocation.
Residual hole: vid committed, `AllocateVolume` delivered, then master loses
*all* state within <1 echo interval, and that target never rejoins while a
reissue happens. Closing that residual entirely requires synchronous
durability the master cannot wipe — i.e. option A — which is why it stays
on the roadmap as the strict backend.

Over-reporting is harmless (skipped ids), under-reporting is the bug; every
feed into `maxVidSeen` is an upper bound on "still possibly allocated".

### L3 — State-loss detection and growth freeze

`TopologyId` is already a cluster identity that survives Raft-state cleanup
when `mdir` is kept (`recoverTopologyIdFromSnapshot`), so a *changed*
topology id means total loss — or a foreign server. Detection: a heartbeat
reports a non-empty `last_seen_topology_id` different from the leader's own.

On detection:

- Set `Topo.growthFrozen` (atomic), emit `MasterVolumeGrowthFrozen` metric
  and an ERROR log naming the expected vs. reported topology ids.
- `NextVolumeId()` fails with an explicit "growth frozen after master state
  loss" error; `findAndGrow` propagates it so assign/grow RPCs fail fast
  instead of allocating on an unproven boundary.
- Operator clears it via `weed shell` (`volume.growth.resume`) / HTTP
  endpoint after verifying the cluster inventory; or it auto-clears after
  `master.volumeGrowth.stateLossRecoveryWait` when that is > 0 (default 0 =
  manual). This is the issue's requested "block new growth until recovery"
  made explicit rather than window-based.

Genuine ambiguity remains: with no raft history and no veteran reported yet,
"fresh cluster" and "post-loss, veterans still offline" look identical until
the first veteran heartbeat. To keep fresh-cluster bootstrap unaffected
while shrinking that window: when the leader self-minted a topology id with
no recovered raft state, `NextVolumeId()` additionally waits out one
warmup-length delay (`IsWarmingUp`-style, counted from leader election) so
veterans get a pulse to identify themselves. A fresh cluster just grows a
few seconds later; a wiped cluster hears a mismatch (or at minimum a raised
`max_volume_id_seen`) inside the same window.

False-positive case: a volume dir cloned into a *different* cluster reports
a foreign topology id and freezes growth there too. That is arguably a real
safety signal (foreign volumes are dangerous), and the escape hatch is one
command; the freeze can also be disabled with
`-volumeGrowth.freezeOnTopologyMismatch=false` for operators who migrate
disks deliberately. Deleting `.vid_stamp` before repurposing a dir also
clears the report at the source.

### L4 — Duplicate-vid detection on registration

Independent of L1–L3, make the collision loud instead of silent:

- In `SyncDataNodeRegistration`, `IncrementalSyncDataNodeRegistration`, and
  `ApplyVolumeChanges`, before registering `v` check whether `v.Id` is
  already live under a *different* layout key — `(collection,
  replicaPlacement, ttl, diskType)` — or in `ecShardMap`. Same vid across
  replicas of one layout is normal; same vid in two layout keys is always a
  bug.
- On conflict: `stats.MasterDuplicateVolumeId` metric (labels: vid, both
  collections), ERROR log naming both data nodes, and mark the newly
  reported instance read-only via the existing `SetReplicaReadOnlyFlag`
  machinery so an ambiguous vid stops accumulating new writes pending
  operator resolution. The volume still registers — hiding the second copy
  from lookups would make its data silently unreachable, which is worse
  than ambiguous-but-alerted.
- Add `volume.check.duplicate_ids` shell command that scans all layouts +
  `ecShardMap` and reports conflicts, for post-incident audit of
  already-corrupted clusters (the issue's repro produces exactly this
  state).

### Phase 2 (optional): pluggable external counter

For deployments that want the strict guarantee with no residual window,
introduce a `VolumeIdAllocator` interface used by `NextVolumeId()`:

- `raft` backend: current behavior + L1 mirror (default).
- `etcd` backend: CAS `seaweedfs/<topologyId>/max_volume_id` via clientv3
  `Txn(If revision==r).Then(put v+1)`; retries on contention. Configured via
  `master.volumeIdStore = "etcd"` + endpoints.

With the etcd backend active, L2's echo/report still runs (it feeds
monitoring and L4) but is no longer load-bearing. Not needed for the
default path; scoped here so the door stays open.

## Compatibility

- All changes are new proto fields and new files; mixed-version clusters
  behave exactly as today (absent `max_volume_id_seen` reports contribute
  nothing; absent echoes leave stamps at their local bootstrap value).
  Protection engages when both master and a given volume server run the new
  build.
- The bound is monotonic everywhere — no rollback risk from replayed or
  out-of-order reports.
- Downgrade: a master losing the new fields just rebuilds the bound from
  live volumes as before — status quo, not a new failure mode.

## Operational notes

- `mdir` defaults to `os.TempDir()`, which many systems wipe on reboot —
  the issue's "ephemeral /data" is close to the *default* configuration.
  Add a startup warning when `mdir` resolves under the OS temp dir and
  volumes exist, and document that persistent `mdir` is required for L1 to
  help. L2/L3 still protect clusters where `mdir` is lost.
- `.vid_stamp` must be excluded from volume-file globbing and preserved on
  disk re-UUID operations.
- Failure to persist a stamp logs loudly but does not kill the heartbeat;
  the local volume scan bound still applies.

## Test plan

1. Unit (topology): mirror write/load, corrupt mirror, `NextVolumeId`
   ordering (mirror before raft apply); `UpAdjustMaxVolumeId` from reported
   `max_volume_id_seen`.
2. Unit (storage): stamp bootstrap = max(local vids, EC vids, file);
   monotonic update from echo and from `AllocateVolume`; corrupt stamp
   tolerated.
3. Topology integration test reproducing the issue in-process: register
   high vids on node B, discard the topology, rebuild with only node A's
   reports plus stamps → `NextVolumeId` must exceed B's holdings; and the
   same test *without* stamps to prove the bug still exists absent L2.
4. Freeze path: mismatch report → `NextVolumeId` error → resume command →
   growth proceeds.
5. Duplicate detection: register same vid under two collections → metric,
   read-only mark, `volume.check.duplicate_ids` reports it.
6. Mixed-version: heartbeat without new fields, response without new
   fields — no panic, no behavior change.

## Implementation sketch (files)

- `weed/pb/master.proto`: 4 new fields (regen `master_pb`).
- `weed/topology/topology.go`: mirror write in `NextVolumeId`; mirror load +
  stamp-report adjust + freeze check; `growthFrozen` flag + resume API.
- `weed/server/raft_server.go`, `raft_hashicorp.go`: load mirror at startup.
- `weed/server/master_grpc_server.go`: echo fields in `heartbeatResponse()`;
  consume `MaxVolumeIdSeen`/`LastSeenTopologyId` per heartbeat; duplicate
  detection calls.
- `weed/topology/topology.go` registration paths: pre-register duplicate
  check (helper on Topology).
- `weed/storage/store.go` (+ `disk_location.go`): `maxVidSeen` state, stamp
  persistence, bootstrap, heartbeat field.
- `weed/server/volume_grpc_admin.go`: stamp on `AllocateVolume`.
- `weed/server/volume_grpc_client_to_master.go`: apply echoed bound.
- `weed/shell`: `volume.growth.resume`, `volume.check.duplicate_ids`.
- `weed/stats`: `MasterDuplicateVolumeId`, `MasterVolumeGrowthFrozen`.
