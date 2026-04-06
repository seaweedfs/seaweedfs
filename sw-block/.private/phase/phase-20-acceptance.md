# Phase 20 Product Acceptance Checklist

Date: 2026-04-06
Status: active

## Reading

`Phase 20` is now architecture-complete but not yet product-complete.

The V2 engine already has a product-shaped semantic contract. The remaining
acceptance gap is host/runtime closure:

1. `write` vs `flush` vs `durability` is not fully explicit
2. fresh replica bootstrap is not fully protocol-aware
3. host observations are not yet a complete protocol seam
4. serving/publish boundaries are not yet derived from one closed contract
5. some adapter and test paths still reflect pre-product assumptions

This checklist is intentionally concrete. Each row should answer:

1. what area is being judged
2. what the system does today
3. what must be true before product signoff
4. whether it blocks `T6/T7`
5. what the cheapest valid proof tier is

## Acceptance Matrix

| Area | Current state | Required for product | Blocks T6/T7? | Best test level |
|---|---|---|---|---|
| `WriteLBA()` external guarantee | implicit `WAL append + ShipAll`; returns before `groupCommit.Submit()` | must be explicitly documented and enforced as write-back admission, not durability completion | Yes | design doc + unit |
| `SyncCache()` durability boundary | durability lives in `groupCommit.Submit()` and distributed sync path | must be the clear durability commit point for `sync_all` proofs and operator reasoning | Yes | component |
| `sync_all` observable truth | barrier result is in distributed sync path, not plain write path | success must mean "all required replicas durable before return" at the chosen commit boundary | Yes | component |
| `write` vs `replicated` vs `durable` contract | currently easy to misread; some tests still treat write as commit | must be a closed contract used consistently by code, docs, and tests | Yes | design doc + component |
| FUA / fsync / flush fence meaning | fence exists in runtime pieces but product statement is incomplete | must say exactly which operation is the durability fence for clients | No | unit + docs |
| Fresh replica entry condition | shipper can be created from assignment / `SetReplicaAddrs()` without a completed bounded session | fresh replica must enter an explicit session before consuming live tail | Yes | component |
| Frozen catch-up target | engine has `FrozenTargetLSN`; host execution does not yet fully enforce it end-to-end | host must freeze target before replay and must not drift target during bounded catch-up | Yes | component |
| Live-tail enable condition | phase gate now blocks live shipping during active session | must additionally prove gate clears only after bounded catch-up reaches its target | Yes | component |
| LSN gap prevention on late attach | current live-tail gate prevents one illegal path, but backlog is not yet moved | must perform bounded WAL catch-up before allowing current live tail | Yes | component |
| Timeout outcome during catch-up | no full retry vs rebuild classification closure yet | must distinguish replan/continue from escalate-to-build | Yes | component |
| Retention loss during catch-up | some low-level signals exist, but end-to-end protocol handling is incomplete | if pinned WAL is lost, host must emit escalation and stop pretending catch-up is possible | Yes | component |
| `ShipperConfiguredObserved` seam | implemented and usable | keep as protocol observation, not as semantic shortcut | No | component |
| `ShipperConnectedObserved` seam | implemented but only part of the lifecycle | must remain distinct from barrier durability and target reached | Yes | component |
| Replay progress observation | not yet centralized as a first-class protocol observation | must emit bounded progress facts for catch-up sessions | No | component |
| Catch-up target reached observation | currently implied by completion path, not exposed as its own acceptance item | must emit and consume a clear "target reached / catch-up completed" observation | Yes | component |
| Timeout classification observation | not centralized | must feed runtime timeout outcomes back through one protocol seam | Yes | component |
| Retention-loss observation | partially local, partially implicit | must re-enter engine truth through one observation seam | No | component |
| Transport contact vs session completion | partly separated now | must stay strictly separate: contact is weaker than durable completion | Yes | component |
| `publish_healthy` contract | currently depends on multiple readiness signals and durability hints | must be derived from one protocol contract: no active recovery, valid transport, required barrier durability, accepted mode | Yes | component |
| Frontend serving gate | `T4` gate exists and enforces degraded / rebuild fail-closed | must be aligned with the same protocol contract that governs publish/readiness | Partially | integration |
| `bootstrap_pending -> publish_healthy` closure | now closer, but not fully closed under bounded catch-up semantics | must require catch-up completion and durability proof, not just partial transport success | Yes | component |
| Replica durable boundary surface | `MinReplicaFlushedLSNAll()` exists | must be the same boundary used by publish and operator surfaces | No | unit + component |
| Rebuild entry condition | engine emits rebuild commands | must stay session-owned, not become an ad-hoc host decision | No | component |
| Rebuild completion host convergence | completion path exists | host must clear recovery state, align publication state, and re-enter normal protocol flow | No | integration |
| WAL catch-up to snapshot/build escalation | not yet fully closed as one runtime contract | must have explicit, testable boundary for "continue WAL catch-up" vs "switch to build" | No | component |
| Snapshot/build under same protocol model | still partly separate from WAL-first path | should converge into the same session-aware host execution pattern | No | design doc + component |
| RF=2 single-replica stable identity | some server paths still drop `ServerID` | every adapter path must preserve stable replica identity | Yes | component |
| ReplicaID derivation consistency | convention exists (`path/serverID`) | must stay identical across `v2bridge`, registry, dispatcher, host runtime, and shippers | No | unit |
| Assignment conversion edge cases | mostly covered but not yet treated as product acceptance rows | must fail closed on empty/missing/mismatched identity data | No | unit |
| Tests that treat `WriteLBA()` as commit | still present in some component tests | must be audited and corrected so `WriteLBA != durability` unless contract changes | Yes | test audit |
| Tests that treat `SyncCache()` as barrier path | mostly correct today | preserve this as the main durability acceptance seam | No | component |
| Bootstrap tests for bounded catch-up | current coverage proves gate behavior more than full closure | must prove `freeze -> replay -> target reached -> live enable` | Yes | component |
| Contract matrix coverage | partial and distributed | need one acceptance-oriented test per boundary: write / ship / flush / barrier / catch-up / publish | Yes | component + integration |

## Signoff Reading

### Must close before `T6/T7` signoff

These rows are hard blockers:

1. `WriteLBA()` / `SyncCache()` / `sync_all` contract closure
2. fresh replica bounded catch-up before live tail
3. timeout / retention-loss classification for catch-up
4. `publish_healthy` alignment with the same protocol contract
5. RF=2 stable identity on all shipping paths
6. test audit for incorrect `WriteLBA == commit` assumptions

### Important but not immediate hard blockers

These should be closed as product hardening, but do not necessarily block the
first `T6/T7` signoff if the bounded scope is explicit:

1. replay progress observation
2. snapshot/build convergence into the same host-side protocol model
3. full acceptance-oriented contract matrix beyond the first closure set

## Recommended Exit Rule

`Phase 20` should not be called product-complete until every row marked
`Blocks T6/T7? = Yes` is either:

1. closed by implementation plus the named proof tier, or
2. explicitly scoped out with a written non-product claim

## One-Sentence Gap

The engine already knows the protocol. `Phase 20` becomes product-complete only
when the host/runtime and data plane obey that protocol all the way through
`write`, `flush`, `catch-up`, `barrier`, `publish`, and `serve`.
