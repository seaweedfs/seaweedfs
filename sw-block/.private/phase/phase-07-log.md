# Phase 07 Log

## 2026-03-30

### Opened

`Phase 07` opened as:

- real-system integration / product-path decision

### Starting basis

1. `Phase 06`: complete
2. broader runnable engine stage accepted
3. next work moves from engine-stage validation to real-system service-slice integration

### Delivered

1. Phase 07 P0
   - service-slice plan defined
   - implementation slice proposal delivered
   - bridge layer introduced as:
     - `sw-block/bridge/blockvol/` for current bridge work
     - `weed/storage/blockvol/v2bridge/` as the deferred real integration target
   - stable identity mapping made explicit:
     - `ReplicaID = <volume-name>/<server-id>`
   - engine / blockvol policy boundary made explicit
   - initial bridge tests delivered (`8`)
2. Phase 07 P1
   - real blockvol reader integrated via `weed/storage/blockvol/v2bridge/reader.go`
   - real pinner integrated via `weed/storage/blockvol/v2bridge/pinner.go`
   - one real catch-up executor path integrated via `weed/storage/blockvol/v2bridge/executor.go`
   - direct real-adapter tests delivered in:
     - `weed/storage/blockvol/v2bridge/bridge_test.go`
   - accepted with explicit carry-forward:
     - interim `CommittedLSN = CheckpointLSN` limits post-checkpoint catch-up semantics and is not final V2 commit truth
     - acceptance is for the real integrated bridge path, not for general post-checkpoint catch-up viability
3. Phase 07 P2
   - real service-path failure replay accepted
   - accepted replay set includes:
     - changed-address restart
     - stale epoch / stale session invalidation
     - unrecoverable-gap / needs-rebuild replay
     - explicit post-checkpoint boundary replay
   - evidence kept explicitly scoped:
     - real `v2bridge` WAL-scan execution proven
     - general integrated post-checkpoint catch-up semantics not overclaimed under the interim model
4. Phase 07 P3
   - product-path decision accepted
   - first product path chosen as:
     - `RF=2`
     - `sync_all`
     - existing master / volume-server heartbeat path
     - V2 engine recovery ownership with `v2bridge` real storage truth
   - pre-hardening prerequisites made explicit
   - intentional deferrals and non-claims recorded
   - `Phase 07` completed

### Next

1. Phase 08 pre-production hardening
2. real master/control delivery integration
3. integrated catch-up / rebuild execution closure
