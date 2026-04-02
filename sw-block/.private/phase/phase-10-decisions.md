# Phase 10 Decisions

## Decision 1: Phase 10 is control-plane closure, not backend execution rework

`Phase 09` already closed the main backend execution gaps on the chosen path.

`Phase 10` should therefore focus on:

1. real control delivery
2. reassignment / result convergence
3. identity cleanup

It should not reopen accepted backend execution semantics unless a true control-plane bug forces a narrow correction.

## Decision 2: Phase 10 remains bounded to the chosen path

Default scope remains:

1. `RF=2`
2. `sync_all`
3. existing master / volume-server heartbeat path

Future durability modes or wider topology support should not be absorbed casually into this phase.

## Decision 3: Identity cleanup belongs to control-plane closure

The current local server identity remains transport-shaped (`listenAddr`).

`Phase 10` is the right place to strengthen this because identity coherence affects:

1. assignment truth
2. sender/replica identity continuity
3. end-to-end control-path correctness

## Decision 4: Rebuild-server idempotence cleanup is bounded residual work, not the phase itself

The repeated-primary-assignment warning around rebuild-server start is a valid residual note.

It may be addressed in `Phase 10` only if:

1. it is directly relevant to real control/runtime ownership or assignment idempotence
2. it stays bounded

It must not turn `Phase 10` into a broad runtime polish phase.

## Decision 5: P1 identity and control-truth closure accepted

`P1` closes the stable-identity/control-truth gap on the chosen block assignment wire.

Accepted properties:

1. stable server identity is now carried additively on the block assignment proto wire:
   - scalar `replica_server_id`
   - per-replica `server_id`
2. generated protobuf output, not hand-maintained output, is now the accepted basis for the wire shape
3. master create-path and chosen failover/primary-refresh assignment generation now preserve stable identity on the chosen path
4. volume-server block/control path now uses the same canonical `volumeServerId` as the main volume server
5. `ControlBridge` continues to fail closed when stable identity is missing

Evidence closure:

1. proto/decode proof now covers stable identity round-trip
2. real ingress proof now covers:
   - proto assignment
   - decode
   - `ProcessAssignments()`
   - `ControlBridge`
   - engine sender `ReplicaID`
3. canonical local identity proof now covers non-default local ID
4. missing-ID fail-closed proof is covered directly

## Decision 6: P2 reassignment/result convergence accepted under the chosen-path volume-server ingress bound

`P2` closes the main reassignment/result-convergence gap on the chosen path without reopening accepted backend execution semantics.

Accepted properties:

1. reassignment through the accepted chosen-path ingress now proves old sender truth is removed and new sender truth is created
2. stale runtime ownership is now proved as a live drain case, not only a bookkeeping absence case
3. reported truth is now checked through `CollectBlockVolumeHeartbeat()`, the same reporting surface used by the live heartbeat loop
4. the accepted no-split-truth claim is bounded to:
   - engine sender truth
   - stale-runtime residue removed
   - heartbeat output truth
5. `P2` does not claim full master-driven failover/gRPC-infrastructure closure beyond the accepted volume-server-side ingress boundary

Evidence closure:

1. real reassignment proof covers `vs2 -> vs3` sender replacement on the chosen path
2. stale-owner proof now blocks a live old goroutine and verifies drain during reassignment
3. heartbeat proof now checks actual heartbeat output rather than local helper state
4. delivery wording is bounded so it does not overclaim a proved live replacement owner in the no-split-truth test

## Decision 7: P3 bounded repeated-assignment/idempotence cleanup accepted on the chosen path

`P3` closes the bounded repeated-assignment residual left after accepted `P2`.

Accepted properties:

1. repeated unchanged chosen-path assignment is now skipped before duplicate V2 orchestrator/recovery work is started
2. the corresponding V1 primary-replication setup path is also absorbed idempotently for unchanged truth
3. changed chosen-path assignment still takes the accepted replacement/update path rather than being suppressed incorrectly
4. `P3` remains bounded cleanup and does not claim general multi-replica idempotence or broad production hardening

Evidence closure:

1. repeated-assignment proof now checks stable V2 event count rather than only stable helper/reporting state
2. changed-assignment guard proof keeps accepted replacement behavior intact
3. externally visible heartbeat state remains coherent after repeated unchanged assignment
