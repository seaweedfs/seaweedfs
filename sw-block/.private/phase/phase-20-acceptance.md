# Phase 20 Product Acceptance Checklist

Date: 2026-04-06
Status: closure implemented; tester validation pending

## Reading

`Phase 20` is now architecture-complete and the targeted host/runtime closure
slice has been implemented for the bounded `RF=2 sync_all` acceptance path.

The V2 engine already has a product-shaped semantic contract. The remaining
acceptance work in this document was host/runtime closure:

1. make `write` vs `flush` vs `durability` explicit
2. close fresh replica bootstrap as a bounded protocol session
3. centralize host observations back into one protocol seam
4. derive serving/publish boundaries from one closed contract
5. remove pre-product assumptions from adapter and proof paths

As of this update, the hard-blocker closure set below has been implemented and
retested on the bounded acceptance subset named by this checklist. This does
not automatically mean every broader `weed/server` or master/integration suite
outside the bounded `Phase 20` acceptance scope has been reclassified yet.

Interpret the current state in two layers:

1. implementation closure: the bounded host/runtime contract is now wired and developer-validated on the named proof subset
2. acceptance closure: still requires tester validation and regression-grade test case freezing before the strongest product claim should be made

## Closure Update

The following closure points are now in place on the bounded acceptance path:

1. `WriteLBA()` is documented and used as write-back admission only
2. `SyncCache()` is the explicit durability fence for `sync_all`
3. fresh and late-attached replicas replay retained WAL backlog before live tail
4. catch-up progress and classified failure now re-enter the core event seam
5. `publish_healthy` and serving gates remain derived from core-owned protocol truth
6. scalar identity paths now fail closed instead of synthesizing address-derived replica IDs

Focused verification used for this closure pass:

1. `go test ./weed/storage/blockvol/test/component -run "TestBootstrap_|TestPublishHealthy_|TestReplicaReadAfterShip"`
2. `go test ./weed/server -run "TestP16B_RunCatchUp_UpdatesCoreProjectionFromLiveRecovery|TestBlockService_(CollectBlockVolumeHeartbeat_PrimaryPublishHealthyUsesCoreTruth|ReadinessSnapshot_PrefersCorePublicationHealth|ApplyAssignments_PrimaryScalarReplicaAddrWithoutServerID|ApplyAssignments_PrimaryRole_UsesCoreStartRecoveryTaskForCatchUp|NeedsRebuildObserved_InvalidatesOnlyTargetReplica)|TestP10P1_"`

## Validation Status

Use the following interpretation for every row in this checklist:

1. `Implemented`: code path is present and intended semantics are enforced
2. `Developer-validated`: targeted unit/component/server proof exists and passed in this closure pass
3. `Tester-validated`: named acceptance case has been run by tester or runner and is frozen as regression evidence

Current `Phase 20` reading:

1. the hard-blocker closure set is `Implemented`
2. the hard-blocker closure set is `Developer-validated` on the bounded acceptance subset
3. the hard-blocker closure set is not yet globally `Tester-validated` just because the developer proof passed
4. tester automation now has metadata-driven suite entries for both `Stage 0` and `Stage 1`
5. `Stage 0` bootstrap closure is now proven on real hosts: `create -> 10s wait -> 4k fsync -> publish_healthy`
6. the remaining hardware failure has been isolated to `Stage 1` sustained workload under the default `64MB` WAL budget, so overall acceptance still remains pending

## Tester Validation Still Required

Even for rows that are already closed by implementation, tester validation is
still required before treating the closure as durable acceptance evidence.

Minimum tester-side acceptance cases to freeze:

1. `WriteLBA != durability`: plain write return must not be used as commit proof; `SyncCache()` / `sync_all` remains the durability fence
2. fresh replica bounded catch-up: `freeze -> replay -> target reached -> live enable`
3. late attach no-gap path: retained WAL backlog must be shipped before current live tail
4. catch-up fail-closed classification: timeout and retention loss must stop catch-up and re-enter rebuild escalation semantics
5. `publish_healthy` contract: transport contact alone must not produce healthy publication without recovery and durability closure
6. stable identity fail-closed: missing `ServerID` must reject or degrade identity closure rather than deriving identity from address shape

Tester evidence should be recorded as named cases in `phase-20-test.md`,
testrunner scenarios, or equivalent acceptance artifacts so the closure is not
only "currently believed" but regression-frozen.

Current tester status:

1. the metadata-driven suite pipeline now runs end-to-end: build, deploy, remote scenario execution, and evidence collection
2. `P20-H0` is now a passing hardware artifact for the bounded bootstrap claim and should be treated as the `Stage 0` closure case
3. the failing `record-before` workload has been moved conceptually into `Stage 1`, where it now reads as a WAL-budget / sustained-I/O issue rather than a bootstrap protocol gap
4. this means tester infrastructure is real and reusable, `Stage 0` is closed, and the next hardware blocker is the master-managed WAL-size gap for `Stage 1`

This checklist is intentionally concrete. Each row should answer:

1. what area is being judged
2. what the system does today
3. what must be true before product signoff
4. whether it blocks `T6/T7`
5. what the cheapest valid proof tier is

## Acceptance Matrix

| Area | Current state | Required for product | Blocks T6/T7? | Best test level |
|---|---|---|---|---|
| `WriteLBA()` external guarantee | explicit write-back admission only; documented in `blockvol.go` | keep as non-durability API unless product contract changes | Yes | design doc + unit |
| `SyncCache()` durability boundary | explicit durability fence through `groupCommit.Submit()` / distributed sync path | keep as the clear durability commit point for `sync_all` proofs and operator reasoning | Yes | component |
| `sync_all` observable truth | success is tied to the barrier-backed durability boundary, not plain write return | keep success meaning "all required replicas durable before return" at the chosen commit boundary | Yes | component |
| `write` vs `replicated` vs `durable` contract | closed on the bounded acceptance path; focused tests no longer treat write as commit | preserve one contract across code, docs, and tests | Yes | design doc + component |
| FUA / fsync / flush fence meaning | fence exists in runtime pieces but product statement is incomplete | must say exactly which operation is the durability fence for clients | No | unit + docs |
| Fresh replica entry condition | fresh / late-attached replicas now enter bounded catch-up before live tail | keep every fresh replica on the explicit session path before live shipping | Yes | component |
| Frozen catch-up target | host execution now uses the bounded target path and does not clear live gate early | keep target frozen through replay completion on the acceptance path | Yes | component |
| Live-tail enable condition | live-tail gate remains blocked during active session and clears only after bounded catch-up completion | preserve "no live tail before target reached" semantics | Yes | component |
| LSN gap prevention on late attach | retained backlog is now replayed before post-attach live entries are sent | preserve bounded WAL catch-up before allowing current live tail | Yes | component |
| Timeout outcome during catch-up | classified failure now re-enters one observation seam; broader retry/replan policy remains bounded by current runtime behavior | preserve explicit classification and fail-closed escalation on the acceptance path | Yes | component |
| Retention loss during catch-up | classified as fail-closed rebuild escalation on the acceptance path | preserve "retention lost => stop catch-up and escalate" behavior | Yes | component |
| `ShipperConfiguredObserved` seam | implemented and usable | keep as protocol observation, not as semantic shortcut | No | component |
| `ShipperConnectedObserved` seam | implemented but only part of the lifecycle | must remain distinct from barrier durability and target reached | Yes | component |
| Replay progress observation | centralized as `RecoveryProgressObserved` on the bounded live path | keep emitting bounded progress facts for catch-up sessions | No | component |
| Catch-up target reached observation | explicit completion event now closes bounded catch-up on the live path | keep a clear "target reached / catch-up completed" observation | Yes | component |
| Timeout classification observation | routed back through the recovery/runtime seam on the bounded path | keep timeout outcomes in one protocol seam | Yes | component |
| Retention-loss observation | routed back as fail-closed rebuild escalation on the bounded path | keep retention-loss outcomes re-entering engine truth through one seam | No | component |
| Transport contact vs session completion | partly separated now | must stay strictly separate: contact is weaker than durable completion | Yes | component |
| `publish_healthy` contract | derived from core-owned readiness / recovery / durability truth on the bounded path | keep it derived from one protocol contract: no active recovery, valid transport, required barrier durability, accepted mode | Yes | component |
| Frontend serving gate | `T4` gate remains fail-closed and is aligned to core projection mode on the bounded path | keep serving aligned with the same contract that governs publish/readiness | Partially | integration |
| `bootstrap_pending -> publish_healthy` closure | bounded path now requires catch-up completion plus durability proof, not partial contact alone | preserve closure before publication and serving | Yes | component |
| Replica durable boundary surface | `MinReplicaFlushedLSNAll()` exists | must be the same boundary used by publish and operator surfaces | No | unit + component |
| Rebuild entry condition | engine emits rebuild commands | must stay session-owned, not become an ad-hoc host decision | No | component |
| Rebuild completion host convergence | completion path exists | host must clear recovery state, align publication state, and re-enter normal protocol flow | No | integration |
| WAL catch-up to snapshot/build escalation | not yet fully closed as one runtime contract | must have explicit, testable boundary for "continue WAL catch-up" vs "switch to build" | No | component |
| Snapshot/build under same protocol model | still partly separate from WAL-first path | should converge into the same session-aware host execution pattern | No | design doc + component |
| RF=2 single-replica stable identity | scalar and slice paths now preserve explicit identity; missing identity fails closed | keep every adapter path on stable replica identity | Yes | component |
| ReplicaID derivation consistency | bounded path uses the same `path/serverID` convention across bridge, registry, host runtime, and shippers | preserve one identity rule across the stack | No | unit |
| Assignment conversion edge cases | missing / mismatched identity data now fails closed on the bounded path | keep empty/missing identity from silently degrading to address shape | No | unit |
| Tests that treat `WriteLBA()` as commit | focused component tests updated away from that assumption | keep `WriteLBA != durability` unless contract changes | Yes | test audit |
| Tests that treat `SyncCache()` as barrier path | focused component tests preserve `SyncCache()` as the acceptance durability seam | preserve this as the main durability acceptance seam | No | component |
| Bootstrap tests for bounded catch-up | acceptance subset now proves `freeze -> replay -> target reached -> live enable` behavior | keep the bounded catch-up acceptance chain explicit | Yes | component |
| Contract matrix coverage | first closure set exists for write / barrier / catch-up / publish / identity | continue broadening the matrix beyond the first acceptance subset as hardening | Yes | component + integration |

## Signoff Reading

### Must close before `T6/T7` signoff

These rows were the hard blockers for the bounded `Phase 20` closure pass and
are now closed on the named acceptance subset at the `Implemented +
Developer-validated` level:

1. `WriteLBA()` / `SyncCache()` / `sync_all` contract closure
2. fresh replica bounded catch-up before live tail
3. timeout / retention-loss classification for catch-up
4. `publish_healthy` alignment with the same protocol contract
5. RF=2 stable identity on all shipping paths
6. test audit for incorrect `WriteLBA == commit` assumptions

### Important but not immediate hard blockers

These remain useful product hardening follow-ups, but do not block the bounded
`Phase 20` closure statement if the scope remains explicit:

1. replay progress observation
2. snapshot/build convergence into the same host-side protocol model
3. full acceptance-oriented contract matrix beyond the first closure set

## Recommended Exit Rule

For the bounded acceptance path, `Phase 20` may be treated as implementation-closed once every row marked
`Blocks T6/T7? = Yes` is either:

1. closed by implementation plus the named proof tier, or
2. explicitly scoped out with a written non-product claim

For stronger product acceptance wording, those same rows should additionally
have tester-owned acceptance cases or runner scenarios frozen as regression
evidence.

## One-Sentence Gap

The engine already knew the protocol; this closure pass brings the bounded
host/runtime and data plane into that protocol through `write`, `SyncCache`,
`catch-up`, `barrier`, `publish`, and `serve`.
