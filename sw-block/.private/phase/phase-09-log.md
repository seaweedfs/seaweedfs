# Phase 09 Log

## 2026-03-31

### Opened

`Phase 09` opened as:

- production execution closure

### Starting basis

1. `Phase 08`: closed
2. chosen candidate path exists for `RF=2 sync_all`
3. main remaining heavy engineering work is backend production-grade execution

### Next

1. `Phase 09 P0` planning for:
   - real `TransferFullBase`
   - real `TransferSnapshot`
   - real `TruncateWAL`
   - stronger live runtime execution ownership

### P0 Technical Pack

Purpose:

- provide the minimum design/algo/test detail needed to start `Phase 09`
- keep the work centered on backend production execution closure
- avoid broad scope growth into control-plane redesign or product-surface work

#### Execution-closure target

`Phase 09` should close this gap:

- current path is candidate-safe but still partially validation-grade
- next path must be backend-production-grade for the chosen `RF=2 sync_all` path

This phase should not try to make every surrounding product surface complete.
It should make the backend execution path real enough that later phases can build on it.

#### What "real" means in this phase

Use these definitions.

1. Real `TransferFullBase`
   - not only "extent is accessible"
   - must read and transfer real block/base contents through the execution path
   - completion must depend on the transfer actually occurring

2. Real `TransferSnapshot`
   - not only "checkpoint exists and is readable"
   - must read and transfer the snapshot/base image through the execution path
   - tail replay must remain aligned with the transferred snapshot boundary

3. Real `TruncateWAL`
   - not only "replica ahead detected"
   - must execute the physical correction required by the chosen path
   - completion must depend on truncation having actually happened

4. Stronger live runtime execution ownership
   - V2 recovery execution should be driven by a stronger live runtime path than test-only orchestration
   - the volume-server path should own plan / execute / cancel / cleanup more directly
   - avoid split ownership where tests prove the path but the running service still does not drive it coherently

#### Recommended slice order inside Phase 09

Keep the phase substantial, but still ordered by dependency:

1. `P1` full-base execution closure
   - make `TransferFullBase` real
   - prove rebuild path no longer depends on accessibility-only validation

2. `P2` snapshot execution closure
   - make `TransferSnapshot` real
   - prove snapshot/tail rebuild path can use a real transferred base

3. `P3` truncation execution closure
   - make `TruncateWAL` real
   - prove replica-ahead path is executable, not only detectable

4. `P4` stronger live runtime ownership
   - move the accepted execution logic closer to the real volume-server/runtime loop
   - prove cleanup / cancel / replacement under the stronger live path

This order is recommended because:

1. transfer closure is the largest production blocker
2. truncation depends on a clearer execution contract
3. runtime ownership should build on real execution, not on validation-grade stubs

#### Design rules

1. engine still owns policy
   - do not move catch-up / rebuild / truncation decision logic into `v2bridge` or `blockvol`

2. `v2bridge` owns real execution translation
   - implement real transfer/truncate behavior there or through bounded runtime hooks
   - keep it as the execution adapter, not the policy owner

3. `blockvol` owns storage/runtime reality
   - WAL
   - checkpoint
   - extent/snapshot data
   - low-level execution primitives

4. chosen-path bounds remain explicit
   - `RF=2`
   - `sync_all`
   - existing master / volume-server heartbeat path

#### Primary reuse / update targets

For each target, state whether the expected action is:

1. `update in place`
2. `reference only`
3. `copy is allowed`

For `Phase 09`, the main targets are:

1. `weed/storage/blockvol/v2bridge/executor.go`
   - action: `update in place`
   - why:
     - this is the direct V2 execution adapter
     - real transfer/truncate closure belongs here first
   - boundary:
     - add real execution behavior
     - do not add policy decisions here

2. `weed/storage/blockvol/blockvol.go`
   - action: `update in place`
   - why:
     - authoritative runtime/storage hooks live here
     - transfer/truncate/recovery primitives may need to be exposed or tightened here
   - boundary:
     - expose/execute real runtime behavior
     - do not let old replication semantics redefine V2 truth

3. `weed/storage/blockvol/rebuild.go`
   - action: `reference first`, then `update in place if needed`
   - why:
     - existing rebuild transport/runtime reality may be reused
   - boundary:
     - reuse transfer reality
     - keep rebuild-source choice and recovery policy in the V2 engine

4. `weed/server/volume_server_block.go`
   - action: `update in place`
   - why:
     - stronger live runtime execution ownership will likely terminate here
   - boundary:
     - strengthen live orchestration/runtime handoff
     - do not collapse V2 boundaries into server-local convenience semantics

5. `weed/storage/blockvol/v2bridge/control.go`
   - action: `reference only` for `Phase 09` unless execution work exposes a real gap
   - why:
     - `Phase 09` is not the main control-plane closure phase

6. product surfaces (`CSI`, `NVMe`, `iSCSI`)
   - action: `reference only`
   - why:
     - not in scope for this phase
     - avoid accidental scope growth

7. old V1 shipper/rebuild execution paths
   - action: `reference only`, `copy is not default`
   - why:
     - they are reality/integration references
     - not the default semantic template for V2

When `sw` submits a slice package in this phase, include a short reuse note:

- files updated in place
- files used as references only
- any file copied from older code and why copy was safer than in-place update

#### Validation expectations

For each execution closure target, require:

1. one-chain proof
   - engine plan
   - engine executor/runtime driver
   - `v2bridge`
   - real `blockvol` operation
   - completion
   - cleanup

2. physical-effect proof
   - the operation must do real transfer/truncate work, not only validation

3. fail-closed behavior
   - partial failure
   - cancellation
   - replacement
   - all release resources correctly

4. observability
   - logs explain:
     - why the execution started
     - what exact execution path ran
     - why it completed / failed / cancelled

#### Suggested validation package

Keep the package focused:

1. one full-base execution test
2. one snapshot execution test
3. one truncation execution test
4. one live runtime ownership test
5. one cleanup/adversarial package covering:
   - cancel
   - replacement
   - partial failure

Avoid:

1. broad matrix growth before these closures are real
2. product-surface tests (`CSI` / `NVMe` / `iSCSI`) in this phase
3. new protocol exploration

#### Assignment template for `sw`

1. Goal
   - Build the `Phase 09` production-execution closure package for the chosen candidate path.

2. Required outputs
   - explicit definition of "real" for each target:
     - `TransferFullBase`
     - `TransferSnapshot`
     - `TruncateWAL`
     - stronger runtime ownership
   - slice/package order inside `Phase 09`
   - implementation plan for the first heavy closure target
   - expected tests/evidence for each target

3. Hard rules
   - no protocol redesign
   - no broad scope growth into product surfaces
   - no moving policy logic into `v2bridge` / `blockvol`
   - keep the chosen-path bound explicit

4. Delivery order
   - first hand to architect review
   - only after architect review passes, hand to tester validation

5. Reject before handoff if
   - "real" is still defined as accessibility-only validation
   - target order ignores execution dependency
   - runtime ownership remains too vague to verify

#### Assignment template for `tester`

1. Goal
   - Validate that the `Phase 09` execution-closure plan is concrete enough to support substantial engineering work.

2. Validate
   - each target has a real/physical-effect definition
   - each target has a one-chain proof expectation
   - cleanup and fail-closed expectations are explicit
   - the phase remains bounded to the chosen path

3. Output
   - pass/fail on execution-target clarity
   - findings on vague "real" definitions, missing cleanup proofs, or hidden scope growth

---

### P0 Deliverable

Accepted summary:

1. engine owns recovery policy and plan selection
2. volume-server runtime owns execution-time addresses and task lifetime
3. `v2bridge` owns execution translation
4. `blockvol` owns local install / truncate primitives and crash-safe persistence

Accepted phase order:

1. `P1` full-base execution closure
2. `P2` snapshot execution closure
3. `P3` truncation execution closure, if still required by the chosen path
4. `P4` stronger live runtime ownership

Accepted bounds:

1. `RebuildAddr` remains a runtime input, not new engine policy state
2. V1 rebuild transport reality may be reused
3. V1 lifecycle semantics must not be copied wholesale
4. one-chain proofs must go through engine executor path, not only direct bridge calls

---

### P1 Technical Pack

Goal:

- close the largest production blocker by making `TransferFullBase` a real execution path with explicit local install ownership

Current gap:

1. `v2bridge/executor.go:TransferFullBase` only validates accessibility
2. no bytes move from rebuild server to replica
3. no bounded `blockvol` primitive makes the transferred base authoritative locally
4. current proof shape can still drift into bridge-only testing

What `P1` must close:

1. real TCP/base transfer
2. real local install of the transferred base
3. executor-chain completion only after transfer + install succeed
4. one engine-chain proof plus one bridge/component proof

#### Design / Algorithm Focus

1. execution boundary
   - `RebuildExecutor.Execute()` remains the policy/execution owner at engine level
   - `v2bridge.TransferFullBase()` performs transport + adaptation
   - `blockvol` owns the bounded primitive that installs the transferred base locally

2. runtime input boundary
   - `rebuildAddr` is a runtime input to executor construction
   - source of truth is the current assignment/runtime state, not new engine policy state
   - `P1` tests may inject it directly
   - `P4` will make that runtime wiring live

3. reuse boundary
   - reuse `weed/storage/blockvol/rebuild.go` as transport/reference reality
   - do not copy V1 rebuild lifecycle ownership wholesale
   - if V1 code mixes transport with WAL reset / dirty-map clear / role change, split the concepts before reuse

4. install boundary
   - `P1` must name the local install step explicitly
   - acceptable shape:
     - `blockvol` exposes a narrow install primitive, or
     - `v2bridge` writes through a narrow `blockvol`-owned API that durably commits the received base
   - unacceptable shape:
     - bytes written directly with no named authoritative-install boundary
     - cleanup/install silently left to some later V1 path

5. rebuild-boundary contract
   - for `full_base`, the engine freezes a minimum target `targetLSN`
   - the backend may produce an actual rebuilt boundary `achievedLSN`
   - accepted completion rule is:
     - `achievedLSN >= targetLSN`
   - accepted only if:
     - local runtime state and engine-visible completion both align to the same `achievedLSN`
   - rejected if:
     - local state advances to a newer boundary but engine/accounting still records only the older `targetLSN`
   - do not require exact-target extent equality on this backend:
     - the mutable extent image is backend reality
     - exact-target extent export would require a different protocol

#### Reuse / Update Instructions

1. `weed/storage/blockvol/v2bridge/executor.go`
   - `update in place`
   - add real `TransferFullBase`
   - invoke the local install primitive

2. `weed/storage/blockvol/blockvol.go`
   - `update in place`
   - add the minimal local install primitive and any small exported accessor needed by the bridge

3. `weed/storage/blockvol/rebuild.go`
   - `reference only`
   - reuse:
     - server-side full-extent streaming
     - client-side transport/message shape
   - do not inherit:
     - V1 rebuild lifecycle ownership
     - role transition logic
     - implicit second-stage cleanup semantics

4. `weed/storage/blockvol/repl_proto.go`
   - `reference only`

#### Validation Focus

Required proofs:

1. component proof
   - direct bridge-level test proves:
     - TCP connection established
     - extent chunks transferred
     - local install primitive invoked
     - transferred base is readable locally after completion

2. one-chain proof
   - `engine plan -> RebuildExecutor.Execute() -> v2bridge.TransferFullBase() -> blockvol install primitive -> completion -> cleanup`

3. fail-closed proof
   - connection refused
   - epoch mismatch
   - partial transfer / mid-stream failure
   - resource cleanup remains explicit after failure

4. observability
   - logs should distinguish:
     - transfer started
     - install started
     - install completed
     - execution failed / cancelled

5. boundary-alignment proof
   - if the achieved rebuild boundary is newer than the frozen minimum target, that newer boundary must become the single shared truth for:
     - engine-visible rebuild completion
     - local checkpoint/base boundary
     - `nextLSN`
     - receiver progress
     - flusher checkpoint state

Reject if:

1. proof only calls `executor.TransferFullBase()` directly
2. bytes move but no authoritative local install step is defined
3. completion can happen before local install is durable enough for the chosen path
4. V1 rebuild lifecycle code is copied wholesale into V2 bridge/runtime
5. `rebuildAddr` ownership becomes implicit or stale-prone
6. rebuild completes with split truth:
   - local runtime reflects `achievedLSN`
   - engine/accounting still reflects only `targetLSN`

#### Suggested First Cut

1. add any minimal `blockvol` accessor needed by the bridge, such as `Epoch()`
2. add the narrow local install primitive in `blockvol`
3. implement real `TransferFullBase` in `v2bridge/executor.go`
4. add one bridge/component test
5. add one engine-chain test through `RebuildExecutor.Execute()`

#### Assignment For `sw`

1. Goal
   - deliver `P1` full-base execution closure for the chosen `RF=2 sync_all` path

2. Required outputs
   - real `TransferFullBase`
   - explicit local install primitive ownership
   - one component proof
   - one engine-chain proof
   - short reuse note:
     - files updated in place
     - files used as references only
     - any copied code and why

3. Hard rules
   - no protocol redesign
   - no silent inheritance of V1 lifecycle semantics
   - no proof shape that bypasses `RebuildExecutor.Execute()`
   - keep `RebuildAddr` as runtime input only

4. Reject before handoff if
   - install ownership is still vague
   - only bridge-level proof exists
   - completion claims exceed the delivered path

#### Assignment For `tester`

1. Goal
   - validate that `P1` proves real full-base execution closure rather than transport-only activity

2. Validate
   - transfer has real physical effect
   - install boundary is explicit
   - one-chain engine proof exists
   - fail-closed behavior is asserted
   - reuse boundaries stayed intact

3. Reject if
   - tests only prove direct bridge calls
   - local install is not named or not verified
   - evidence overclaims snapshot/tail or live runtime ownership

#### Carry-forward note

`P2` may share the same transfer/install helper if that helper is genuinely transport-common and does not blur the boundary between:

1. full-base closure
2. snapshot+tail closure

For `P1`, exact-target extent equality is not required on this backend.
The required property is:

1. `achievedLSN >= targetLSN`
2. the achieved boundary is stable
3. engine and local runtime converge to that same achieved boundary

---

### P1 Completion Record

Date: 2026-03-31
Status: accepted (rev 5.1)
Revisions: 5.1 (rev 1→2: install state handoff + second catch-up, rev 2→3: pre-flush + plan-bound validation, rev 3→4: achievedLSN surfaced + no split truth, rev 4→5: receiver progress alignment, rev 5→5.1: flush fail-closed + stale-higher reset + live receiver closure)

#### Accepted contract

- `TransferFullBase(committedLSN) → (achievedLSN, error)`
- `achievedLSN >= committedLSN` (validated)
- Engine records progress at `achievedLSN`
- Local runtime (checkpoint, nextLSN, flusher, receiver) converges to `achievedLSN`
- No split truth between engine accounting and local state

#### Files changed

| File | Action |
|------|--------|
| `blockvol/blockvol.go` | Updated: RebuildInstaller (full state handoff), SyncReceiverProgress, ReceivedLSN, ApplyRebuildEntry |
| `blockvol/rebuild.go` | Updated: handleFullExtent pre-flush |
| `v2bridge/executor.go` | Updated: real TransferFullBase (TCP + install + second catch-up + achievedLSN) |
| `v2bridge/transfer_test.go` | New: 11 P1 tests |
| `v2bridge/bridge_test.go` | Updated: NewExecutor signature |
| `v2bridge/execution_chain_test.go` | Updated: NewExecutor signature |
| `v2bridge/failure_replay_test.go` | Updated: NewExecutor signature |
| `v2bridge/hardening_test.go` | Updated: NewExecutor signature |
| `sw-block/engine/replication/executor.go` | Updated: RebuildIO interface (achievedLSN return), RebuildExecutor uses achievedLSN |

#### Test inventory (11 P1 tests)

| Test | Proves |
|------|--------|
| RealTCP | Component: TCP + install |
| OneChain | Engine chain → InSync |
| NonEmptyReplica | Stale state cleared |
| UnflushedEntries | Pre-flush correctness |
| AchievedConvergence | achievedLSN=25 > target=20, full convergence |
| StaleHigherThanAchieved | stale higher runtime state resets to achieved boundary |
| LiveReceiverConvergence | active receiver `receivedLSN` converges to achieved boundary |
| ConnectionRefused | Fail-closed |
| EpochMismatch | Fail-closed |
| NoAddress | Fail-closed |
| PartialTransfer | Fail-closed |

---

### P2 Technical Pack

Date: 2026-04-01
Goal: make `TransferSnapshot` a real execution path for `snapshot_tail`, with an exact snapshot boundary, authoritative local install, and exact tail replay to the planned target

#### Current gaps `P2` must close

1. `weed/storage/blockvol/v2bridge/executor.go`
   - `TransferSnapshot(snapshotLSN)` is still validation-only
   - it checks local checkpoint visibility but performs no real transfer or install

2. `weed/storage/blockvol/snapshot_export.go`
   - `ExportSnapshot()` / `ImportSnapshot()` are real I/O primitives
   - but the exported manifest does not currently carry the snapshot/base LSN
   - so the receiver cannot prove that the imported base equals the engine's requested `snapshotLSN`

3. `weed/storage/blockvol/snapshot_export.go`
   - current import resets dirty map and WAL
   - but it does not yet converge rebuild runtime state to the imported snapshot boundary:
     - `super.WALCheckpointLSN`
     - `nextLSN`
     - flusher checkpoint
     - receiver progress
   - that is not enough for rebuild completion ownership

4. `weed/storage/blockvol/v2bridge/pinner.go`
   - `HoldSnapshot(checkpointLSN)` proves the checkpoint is currently trusted
   - it does not itself create or hold an immutable snapshot object for later transfer
   - `P2` needs an exact, stable source image rather than "whatever checkpoint exists when transfer starts"

5. Engine/runtime proof is missing
   - there is no one-chain proof yet for:
     - `engine plan -> RebuildExecutor -> v2bridge.TransferSnapshot -> blockvol snapshot install -> WAL tail replay -> InSync`

#### Design / Algorithm Focus

1. snapshot-boundary contract
   - unlike `P1 full_base`, `P2 snapshot_tail` should be exact at the base boundary
   - the transferred base must correspond to the engine-requested `snapshotLSN`
   - do not silently accept "newer checkpoint that still covers the target" as the base contract for `P2`

2. stable-source contract
   - snapshot transfer must read from an immutable snapshot object owned for this execution attempt
   - acceptable shapes:
     - export an existing snapshot whose base LSN equals `snapshotLSN`
     - create a temporary runtime-owned snapshot whose base LSN equals `snapshotLSN`, export it, then release it
   - reject if:
     - the source only proves current checkpoint `>= snapshotLSN`
     - the transfer reads directly from mutable live extent with no exact snapshot object

3. explicit metadata contract
   - the transfer must carry the base boundary explicitly
   - acceptable shapes:
     - extend the snapshot manifest with `BaseLSN`
     - or carry an equivalent exact-boundary field on the transport before install begins
   - reject if the receiver infers boundary only from current local/remote runtime state

4. local install contract
   - `blockvol` must own the bounded local install primitive for imported snapshot state
   - after snapshot install and before tail replay begins, local runtime must converge to the imported snapshot boundary:
     - checkpoint/base boundary = `snapshotLSN`
     - `nextLSN = snapshotLSN + 1`
     - flusher checkpoint = `snapshotLSN`
     - receiver progress = `snapshotLSN`
   - this is the `snapshot_tail` equivalent of the `P1` install handoff

5. tail alignment contract
   - after base install, WAL replay must start from `snapshotLSN`
   - replay semantics remain:
     - `StreamWALEntries(startExclusive=snapshotLSN, endInclusive=targetLSN)`
     - applied range is `snapshotLSN+1 ... targetLSN`
   - no replay before base install is durably complete
   - no replay beyond `targetLSN`

6. completion contract
   - for `snapshot_tail`, full rebuild completion should converge exactly to `targetLSN`
   - required final truth after tail replay:
     - engine-visible rebuild progress/completion = `targetLSN`
     - local checkpoint / head / `nextLSN` / flusher / receiver are aligned with replayed state
   - reject split truth at either stage:
     - stage A: post-import base boundary differs across local runtime fields
     - stage B: final engine/accounting truth differs from local replayed truth

7. scope guard
   - `P2` may add the minimum runtime-owned snapshot helper needed to mint/hold an exact exported snapshot
   - `P2` should not absorb the broader live runtime ownership work reserved for later `P4`

#### Reuse / Update Instructions

1. `weed/storage/blockvol/v2bridge/executor.go`
   - `update in place`
   - implement real `TransferSnapshot(snapshotLSN)`
   - preserve explicit boundary between:
     - snapshot base transfer/install
     - WAL tail replay

2. `weed/storage/blockvol/snapshot_export.go`
   - `update in place`
   - preferred place to extend export/import metadata with exact base-boundary information
   - may also host a narrow rebuild-oriented install helper if that stays bounded and explicit

3. `weed/storage/blockvol/blockvol.go`
   - `update in place` if a narrow runtime convergence helper is needed after snapshot import
   - reuse `SyncReceiverProgress()` / receiver access patterns from `P1` where possible

4. `weed/storage/blockvol/snapshot.go`
   - `reference only` unless a tiny exported helper is truly needed
   - reuse:
     - snapshot creation semantics
     - exact `BaseLSN` meaning
     - snapshot immutability model

5. `weed/storage/blockvol/rebuild.go`
   - `reference only`
   - reuse transport/message style only if helpful
   - do not copy V1 rebuild lifecycle code wholesale into `P2`

6. `weed/storage/blockvol/repl_proto.go`
   - `update in place` only if a narrow snapshot-transfer message or exact-boundary carrier is required
   - keep protocol change bounded to `P2` execution closure

7. copy guidance
   - no wholesale copy from V1 rebuild or generic artifact tooling into `sw-block`
   - prefer:
     - `update in place`
     - `reference only`
   - if any code must be copied, name it explicitly and justify why reuse-in-place was unsafe or impossible

#### Validation Focus

Required proofs:

1. component proof
   - direct bridge-level proof that `TransferSnapshot(snapshotLSN)`:
     - transfers a real snapshot image
     - carries exact base-boundary metadata
     - invokes authoritative local snapshot install
     - leaves the replica readable at the imported snapshot boundary before tail replay

2. exact-boundary proof
   - prove the imported snapshot base equals the planned `snapshotLSN`
   - recommended adversarial case:
     - plan at checkpoint `N`
     - primary later advances to checkpoint `N+k`
     - transfer must either:
       - still export exact base `N`, or
       - fail closed
     - it must not silently import base `N+k` and continue as if base `N` was transferred

3. one-chain proof
   - `engine plan -> RebuildExecutor.Execute() -> TransferSnapshot(snapshotLSN) -> blockvol install -> StreamWALEntries(snapshotLSN, targetLSN) -> InSync`

4. convergence proof
   - after import and before tail replay:
     - checkpoint/base boundary
     - `nextLSN`
     - flusher checkpoint
     - receiver progress
     all equal `snapshotLSN`
   - after tail replay completes:
     - engine progress/completion = `targetLSN`
     - local runtime truth also reflects `targetLSN`

5. cleanup proof
   - temporary snapshot / hold is released on:
     - success
     - transfer failure
     - install failure
     - cancel/abort
   - no pin/snapshot leak remains after session exit

6. fail-closed proof
   - exact boundary unavailable
   - manifest or transport boundary mismatch
   - checksum / payload corruption
   - partial transfer / mid-stream failure
   - local install failure
   - cleanup remains explicit after failure

Reject if:

1. `TransferSnapshot()` still only validates checkpoint visibility
2. base-boundary metadata remains implicit
3. a newer checkpoint is silently accepted as if it were the planned `snapshotLSN`
4. tail replay starts before local base install convergence is durable
5. tests bypass `RebuildExecutor.Execute()`
6. `P2` overclaims broader runtime-ownership closure that belongs to later slices

#### Suggested First Cut

1. extend snapshot-transfer metadata to carry exact `BaseLSN`
2. add the narrow source-side helper that exports a stable snapshot image for the requested boundary
3. add the narrow receiver-side install/convergence helper for imported snapshot state
4. implement real `TransferSnapshot(snapshotLSN)` in `v2bridge/executor.go`
5. add one bridge/component proof
6. add one engine-chain proof
7. add one adversarial boundary-drift or fail-closed proof

#### Assignment For `sw`

1. Goal
   - deliver `P2` snapshot execution closure for the chosen `RF=2 sync_all` path

2. Required outputs
   - real `TransferSnapshot(snapshotLSN)`
   - exact snapshot/base-boundary metadata
   - explicit local snapshot install ownership
   - one component proof
   - one engine-chain proof
   - one boundary-drift or fail-closed proof
   - short reuse note:
     - files updated in place
     - files used as references only
     - any copied code and why

3. Hard rules
   - no fallback to "current checkpoint is good enough"
   - no hidden base-boundary inference
   - no replay before durable base install
   - no proof shape that bypasses `RebuildExecutor.Execute()`
   - keep broader runtime ownership out unless directly required for exact snapshot execution

4. Reject before handoff if
   - the source image is not exact and stable
   - imported base boundary is not explicitly verifiable
   - local install ownership is still generic/implicit
   - completion claims exceed snapshot execution closure

#### Assignment For `tester`

1. Goal
   - validate that `P2` proves real `snapshot_tail` execution closure rather than checkpoint visibility plus tail replay on paper

2. Validate
   - snapshot image is physically transferred
   - exact base-boundary metadata is carried and verified
   - local runtime converges to `snapshotLSN` before replay
   - full chain reaches exact `targetLSN` after replay
   - temporary snapshot / holds are released on all exit paths
   - reuse boundaries stayed intact

3. Reject if
   - tests only prove export/import in isolation
   - base transfer silently accepts a newer checkpoint than the plan requested
   - runtime state after import is not aligned to `snapshotLSN`
   - evidence overclaims `TruncateWAL` or broad runtime ownership closure

#### Carry-forward note

`P2` may reuse parts of `ExportSnapshot()` / `ImportSnapshot()` if the execution boundary stays explicit.
It should not collapse:

1. generic snapshot artifact import/export
2. rebuild-specific exact-boundary execution ownership

`P3` remains the first slice that closes real `TruncateWAL`.

---

### P2 Completion Record

Date: 2026-04-01
Status: accepted (rev 2)
Revisions: 2 (rev 1→2: single-executor tail replay closure, direct-to-disk snapshot streaming, bounded limitation for partial install explicitly documented)

#### Accepted contract

- `TransferSnapshot(snapshotLSN)` performs real TCP snapshot transfer with exact boundary verification
- `SnapshotArtifactManifest.BaseLSN` carries the exact transferred snapshot boundary
- local snapshot install converges runtime to `snapshotLSN` before tail replay begins
- tail replay executes through the same executor path and remains bounded to `targetLSN`
- post-replay engine-visible completion and local runtime converge to the same final target boundary

#### Files changed

| File | Action |
|------|--------|
| `weed/storage/blockvol/v2bridge/executor.go` | Updated: real `TransferSnapshot`, single-executor remote tail replay, shared remote replay helper |
| `weed/storage/blockvol/rebuild.go` | Updated: exact snapshot export handler on rebuild server |
| `weed/storage/blockvol/repl_proto.go` | Updated: `RebuildSnapshot` request type |
| `weed/storage/blockvol/snapshot_export.go` | Updated: `BaseLSN` manifest metadata |
| `weed/storage/blockvol/v2bridge/snapshot_transfer_test.go` | New: P2 execution-closure tests |
| `weed/storage/blockvol/v2bridge/execution_chain_test.go` | Updated: snapshot-tail notes now point to real P2 path |
| `weed/storage/blockvol/v2bridge/hardening_test.go` | Updated: snapshot-tail notes now point to real P2 path |

#### Test inventory (6 P2 tests)

| Test | Proves |
|------|--------|
| `RealTCP` | Component: TCP snapshot transfer + exact boundary install |
| `SnapshotTailRebuild_OneChain` | Single-executor engine chain → tail replay → `InSync` |
| `BoundaryDrift` | Newer checkpoint is rejected, not silently accepted |
| `NoAddress` | Fail-closed |
| `RuntimeConvergence` | stale higher runtime state converges down to exact `snapshotLSN` |
| `TempSnapshotCleaned` | temp snapshot ownership released on success and failure |

#### Adversarial closure summary

Validated adversarial families now cover:

1. repeated full-base rebuild with era replacement
2. bounded second catch-up when the source advances beyond the frozen target
3. snapshot rebuild replacing a higher local state with the exact planned snapshot boundary
4. repeated snapshot rebuild with era replacement

---

### P3 Technical Pack

Date: 2026-04-01
Goal: make `TruncateWAL(truncateLSN)` a real execution path for the replica-ahead recovery case, with exact boundary convergence and one-chain proof through the accepted catch-up executor path

#### Current gaps `P3` must close

1. `weed/storage/blockvol/v2bridge/executor.go`
   - `TruncateWAL(truncateLSN)` is still a stub
   - no physical correction is executed on the replica

2. `sw-block/engine/replication/executor.go`
   - the catch-up executor currently records truncation in engine/session state only
   - it does not call any real I/O hook before marking truncation complete

3. `sw-block/engine/replication/executor.go` / engine I/O boundary
   - the current `CatchUpIO` interface only supports WAL streaming
   - there is no engine-owned I/O contract yet for real truncation execution

4. `weed/storage/blockvol`
   - there is no bounded local primitive yet that explicitly converges replica runtime state down to an exact `truncateLSN`
   - current runtime helpers were built for forward install/rebuild convergence, not for authoritative rollback of local ahead state

5. one-chain proof is missing
   - there is no proof yet for:
     - `engine plan(replica ahead) -> CatchUpExecutor.Execute() -> v2bridge.TruncateWAL(truncateLSN) -> blockvol local correction -> completion -> InSync`

#### Design / Algorithm Focus

1. truncation-boundary contract
   - unlike `P1`, truncation does not accept a conservative achieved boundary
   - unlike `P2`, truncation does not transfer a new base image
   - `P3` should converge to one exact correction boundary:
     - `truncateLSN`
   - after completion, neither engine/accounting nor local runtime may remain ahead of `truncateLSN`

2. chosen-path physical correction contract
   - `P3` must execute the physical correction required by the current chosen path for `replica_ahead_needs_truncation`
   - rejected shapes:
     - bookkeeping-only truncation
     - engine records `truncated_to=X` but local runtime still reflects `> X`
   - accepted shape:
     - one bounded local primitive or helper makes the replica's local WAL/runtime truth no greater than `truncateLSN`

3. runtime convergence contract
   - required local truth after truncation completes:
     - `WALHeadLSN <= truncateLSN`
     - `nextLSN = truncateLSN + 1` if local runtime exposes next-write position
     - receiver progress is not left above `truncateLSN`
     - engine-visible truncation completion records the same exact boundary
   - if current runtime needs additional metadata convergence for recovery correctness, that convergence must be explicit and tested

4. bounded-scope contract
   - `P3` closes the replica-ahead physical correction required by the current chosen path
   - do not overclaim a general divergent-base rollback protocol unless that is actually delivered
   - broader runtime ownership and live service-loop coordination remain `P4`

5. executor ownership contract
   - engine still owns the decision that truncation is required and freezes `truncateLSN`
   - `CatchUpExecutor` owns the execution lifecycle
   - `v2bridge` owns real execution translation
   - `blockvol` owns any narrow storage/runtime primitive required for exact local correction

6. fail-closed contract
   - session completion must depend on truncation having actually occurred
   - if local correction fails, the executor must not record truncation as complete
   - cleanup/release remains explicit on failure/cancel

#### Reuse / Update Instructions

1. `sw-block/engine/replication/executor.go`
   - `update in place`
   - extend the catch-up executor path so truncation can call real I/O before `RecordTruncation(...)`

2. `sw-block/engine/replication/executor.go` interface section
   - `update in place`
   - add the minimum engine-owned I/O contract needed for real truncation execution
   - keep policy in engine, execution in bridge

3. `weed/storage/blockvol/v2bridge/executor.go`
   - `update in place`
   - implement real `TruncateWAL(truncateLSN)`
   - keep it execution-only; no local recovery policy decisions

4. `weed/storage/blockvol/blockvol.go`
   - `update in place` if a narrow local correction primitive is required
   - prefer a named, bounded primitive over ad hoc field mutation in the bridge

5. `weed/storage/blockvol/rebuild.go`
   - `reference only`
   - use only if a small existing reset/convergence pattern is genuinely reusable
   - do not inherit V1 rebuild lifecycle code wholesale

6. `weed/storage/blockvol/v2bridge/*_test.go`
   - `update in place`
   - add direct component proof plus one-chain proof through the catch-up executor

7. copy guidance
   - prefer `update in place`
   - use `reference only` for V1/runtime patterns
   - no broad copy into `sw-block` or a parallel truncation implementation unless reuse-in-place is impossible

#### Validation Focus

Required proofs:

1. component proof
   - direct bridge-level proof that `TruncateWAL(truncateLSN)` performs real local correction
   - local runtime after completion is no longer ahead of `truncateLSN`

2. one-chain proof
   - `engine plan(replica ahead) -> CatchUpExecutor.Execute() -> v2bridge.TruncateWAL(truncateLSN) -> blockvol local correction -> completion -> InSync`

3. exact-boundary proof
   - truncation converges to the requested `truncateLSN`
   - it must not:
     - leave local truth above `truncateLSN`
     - truncate below `truncateLSN`

4. runtime proof
   - stale-higher local state is corrected down to `truncateLSN`
   - active receiver progress does not remain above `truncateLSN`
   - follow-on recovery/catch-up can proceed from the truncated boundary without gap confusion

5. fail-closed proof
   - local correction failure prevents completion
   - cancel/failure releases resources cleanly
   - no "truncation recorded" event without real local correction

6. adversarial proof
   - recommended cases:
     - replica ahead with active receiver state
     - repeated truncation across different eras
     - truncation followed by resumed catch-up from the exact boundary

Reject if:

1. `TruncateWAL()` remains a stub
2. engine still records truncation without calling real I/O
3. local runtime can remain ahead after truncation completion
4. proof bypasses `CatchUpExecutor.Execute()`
5. `P3` overclaims stronger runtime ownership that belongs to `P4`

#### Suggested First Cut

1. extend the engine catch-up I/O boundary to include real truncation
2. add the narrow `blockvol` local correction primitive needed for exact truncate convergence
3. implement real `v2bridge.TruncateWAL(truncateLSN)`
4. wire the catch-up executor to call truncation I/O before `RecordTruncation(...)`
5. add one direct bridge/component proof
6. add one catch-up one-chain proof
7. add one stale-higher / repeated-era adversarial proof

#### Assignment For `sw`

1. Goal
   - deliver `P3` truncation execution closure for the chosen `RF=2 sync_all` path

2. Required outputs
   - real `TruncateWAL(truncateLSN)`
   - explicit local correction ownership
   - catch-up executor wired to real truncation I/O
   - one component proof
   - one engine-chain proof
   - one adversarial proof
   - short reuse note:
     - files updated in place
     - files used as references only
     - any copied code and why

3. Hard rules
   - no bookkeeping-only truncation
   - no completion before local correction
   - no hidden policy inside the bridge/runtime
   - no proof shape that bypasses `CatchUpExecutor.Execute()`
   - keep broader runtime ownership out of `P3`

4. Reject before handoff if
   - exact truncation boundary is still implicit
   - local runtime can remain ahead after completion
   - engine completion still depends only on recorded bookkeeping
   - claims exceed truncation execution closure

#### Assignment For `tester`

1. Goal
   - validate that `P3` proves real replica-ahead correction rather than truncation detection plus bookkeeping

2. Validate
   - truncation has real local effect
   - one-chain proof exists through `CatchUpExecutor`
   - local runtime converges to exact `truncateLSN`
   - no completion occurs without local correction
   - reuse boundaries stayed intact

3. Reject if
   - tests only prove direct helper calls without catch-up executor closure
   - local runtime remains ahead after completion
   - truncation proof overclaims broader live runtime ownership

#### Carry-forward note

`P3` should close real `TruncateWAL` for the current chosen path.
It should not absorb:

1. general divergent-base rollback semantics beyond the delivered path
2. broader live runtime ownership / service-loop integration

`P4` remains the first slice that closes stronger live runtime ownership.

---

### P3 Completion Record

Date: 2026-04-02
Status: accepted (rev 6)
Revisions: 6 (rev 1→2: data-truth proof for unflushed-ahead case, rev 2→3: narrowed Option A contract introduced, rev 3→4: execution-time escalation to rebuild + `ioMu.Lock()`, rev 4→5: atomic safety check under flusher pause, rev 5→6: exact predicate tightened to `checkpointLSN == truncateLSN`)

#### Accepted contract

- `P3` does not claim exact local truncation for all replica-ahead cases
- local truncation is allowed only when `checkpointLSN == truncateLSN`
- if `checkpointLSN > truncateLSN`, truncation is unsafe because ahead entries already contaminated extent
- if `checkpointLSN < truncateLSN`, truncation is unsafe because part of the kept range may still exist only in WAL
- unsafe cases escalate to rebuild rather than recording truncation success
- safe truncation converges runtime truth to `truncateLSN`

#### Files changed

| File | Action |
|------|--------|
| `sw-block/engine/replication/executor.go` | Updated: `CatchUpIO.TruncateWAL`, truncation-only skip, escalation via `ErrTruncationUnsafe` |
| `weed/storage/blockvol/blockvol.go` | Updated: `ErrTruncationUnsafe`, `TruncateToLSN()` with flusher pause, exclusive I/O lock, exact safety predicate |
| `weed/storage/blockvol/flusher.go` | Updated: `Pause()` helper |
| `weed/storage/blockvol/v2bridge/executor.go` | Updated: real `TruncateWAL`, bridge mapping from `blockvol.ErrTruncationUnsafe` to `engine.ErrTruncationUnsafe` |
| `weed/storage/blockvol/v2bridge/truncate_test.go` | New: P3 truncation execution tests including mixed-case escalation |

#### Test inventory (9 P3 tests)

| Test | Proves |
|------|--------|
| `RealCorrection` | safe-case truncate restores base data |
| `OneChain` | engine chain → truncate → `InSync` |
| `ActiveReceiverCorrected` | receiver progress converges down to `truncateLSN` |
| `ThenCatchUp` | truncate-safe case resumes writes without gap |
| `NilVol` | fail-closed |
| `FlushedAheadEscalates` | `checkpoint > target` escalates |
| `FlushedAheadNotInSync` | one-chain escalation to `NeedsRebuild` |
| `MixedCase_CheckpointBelowTarget` | `checkpoint < target` escalates |
| `RepeatedEras` | repeated safe truncations preserve exact boundaries |

#### Residual note

The truncation-safe vs rebuild-required split is accepted at execution time for `P3`.
Moving that split earlier into planning/runtime classification remains a later improvement, not a `P3` blocker.

---

### P4 Technical Pack

Date: 2026-04-02
Goal: strengthen live runtime ownership so the accepted `P1` / `P2` / `P3` execution logic is driven coherently by the real volume-server/runtime path instead of depending primarily on bounded test/executor wiring

#### Layer 1: Semantic Core

##### Problem statement

`P1` / `P2` / `P3` now close the main execution primitives on the chosen path:

1. full-base rebuild
2. snapshot-tail rebuild
3. replica-ahead truncation with rebuild escalation when local truncate is unsafe

What is still weak is runtime ownership:

1. who starts these execution paths in the live volume-server path
2. who cancels/replaces them on epoch/assignment change
3. who owns runtime inputs such as current rebuild addresses and receiver/runtime state
4. who guarantees cleanup when live ownership changes mid-flight

`P4` should make the live runtime path own those things more directly.

##### Backend reality

1. engine policy is already accepted and should remain the policy owner
2. `v2bridge` already implements accepted execution primitives
3. `blockvol` already holds the storage/runtime truth and low-level primitives
4. current strong proofs are still biased toward bounded executor/test wiring
5. the running volume-server path still needs stronger explicit ownership of:
   - start
   - cancel
   - replacement
   - cleanup
   - current runtime inputs

##### High-level algorithm

Use this runtime model:

1. current assignment/runtime state determines whether recovery execution should exist
2. the live volume-server/runtime path creates or replaces one accepted execution owner per replica target
3. that owner binds:
   - engine plan/session
   - current runtime inputs (for example rebuild address / receiver state)
   - accepted `v2bridge` execution implementation
4. if assignment/epoch/runtime identity changes:
   - invalidate the old owner
   - release accepted resources
   - either stop cleanly or create a replacement owner
5. completion is accepted only if:
   - engine-visible completion is reached
   - runtime cleanup/release semantics also complete coherently

##### Pseudo code

```text
on runtime assignment/update:
    derive current runtime inputs
    decide whether a recovery execution owner should exist

if no owner should exist:
    cancel and release any current owner
    return

if existing owner is stale for current epoch/assignment/runtime inputs:
    cancel and release stale owner

if a new owner is required:
    build accepted engine plan
    bind current runtime inputs
    bind accepted v2bridge executor
    start execution under runtime ownership

while owner is active:
    if epoch/assignment/runtime input changes:
        cancel and release
        restart or escalate according to accepted engine policy

on success:
    complete engine session
    release runtime-owned resources
    publish stable in-sync runtime state
```

##### State / convergence contract

`P4` should make these truths explicit:

1. engine still owns policy truth
2. runtime path owns live execution lifetime truth
3. current runtime inputs are not hidden test injections
4. at any time, there is at most one current live owner for a given recovery execution target
5. replacement/cancel path does not leave split ownership between:
   - old executor/session/resources
   - new runtime assignment

##### Reject shapes

Reject before implementation if the design still relies on:

1. test-only creation of execution owners
2. hidden runtime input injection that is not sourced from live assignment/runtime state
3. cancel/replace behavior that only invalidates engine state but leaves runtime-side work alive
4. one-chain proofs that still bypass the live runtime path
5. claims that `P4` closes heartbeat/gRPC-level control-plane ownership

#### Layer 2: Execution Core

##### Current gaps `P4` must close

1. live volume-server/runtime ownership of accepted execution start is still too implicit
2. runtime replacement/cancel/cleanup semantics are not yet the primary proof target
3. accepted execution inputs such as current rebuild address/runtime receiver state are not yet owned clearly enough by the live path
4. one-chain proof through the live runtime path is still weaker than bounded executor proof

##### Reuse / update instructions

1. `weed/server/volume_server_block.go`
   - `update in place`
   - make this the primary live ownership surface if the current VS path is where accepted recovery work starts/stops

2. `weed/storage/blockvol/v2bridge/control.go`
   - `update in place`
   - strengthen runtime-to-engine/executor wiring for current addresses and runtime state

3. `weed/storage/blockvol/v2bridge/executor.go`
   - `reference only` unless a tiny runtime binding hook is required
   - accepted execution semantics from `P1` / `P2` / `P3` should not be reopened casually

4. `sw-block/engine/replication/*`
   - `update in place` only where runtime ownership needs explicit lifecycle hooks
   - keep policy inside engine; do not move policy into the VS/runtime path

5. `weed/storage/blockvol/blockvol.go`
   - `reference only` unless a narrow runtime cleanup/cancel primitive is truly required

6. copy guidance
   - prefer `update in place`
   - use `reference only` for accepted executor/runtime patterns
   - do not copy accepted engine/executor logic into a second live-path implementation

##### Validation focus

Required proofs:

1. live-path start proof
   - accepted recovery execution can be started from the real runtime path with current runtime inputs

2. cancel / replace proof
   - assignment or epoch change cancels the stale live owner
   - resources are released
   - replacement ownership is explicit and singular

3. cleanup proof
   - success, failure, and cancel all clean up runtime-owned resources coherently

4. one-chain proof
   - `live runtime path -> engine plan/session -> v2bridge execution -> cleanup/completion`

5. no-split-ownership proof
   - do not leave old runtime work alive after replacement
   - do not leave engine state claiming ownership while runtime path has moved on

Reject if:

1. proof still relies mainly on direct executor construction in tests
2. runtime address/input sourcing remains implicit
3. old live work can survive replacement/cancel
4. `P4` overclaims broader control-plane closure

##### Suggested first cut

1. identify the exact live runtime start/replace/cancel surface for the chosen path
2. wire current runtime inputs explicitly from that live surface into accepted execution ownership
3. add one live-path start proof
4. add one cancel/replace proof
5. add one cleanup proof

##### Assignment for `sw`

1. Goal
   - deliver `P4` stronger live runtime ownership for the chosen `RF=2 sync_all` path

2. Required outputs
   - one explicit live ownership path
   - explicit start / cancel / replace / cleanup semantics
   - one live-path one-chain proof
   - one replacement/cancel proof
   - short reuse note:
     - files updated in place
     - files used as references only
     - any copied code and why

3. Hard rules
   - do not reopen accepted `P1` / `P2` / `P3` execution semantics unless required by a runtime ownership bug
   - do not move policy out of engine
   - do not turn `P4` into heartbeat/gRPC control-plane closure

##### Assignment for `tester`

1. Goal
   - validate that accepted execution is now owned coherently by the live runtime path

2. Validate
   - live start path is real
   - cancel/replace path is explicit
   - cleanup is explicit on success/failure/cancel
   - no split ownership remains after runtime change

3. Reject if
   - tests still mostly bypass the live runtime path
   - runtime input ownership is still hidden
   - evidence overclaims control-plane closure

---

### P4 Completion Record

Date: 2026-04-02
Status: accepted (rev 7)
Revisions: 7 (rev 1: initial live runtime ownership wiring, rev 2: session invalidation / pointer identity / scoped rebuild address, rev 3: real supersede semantics + real `ProcessAssignments` path, rev 4: `cancelAndDrain` serialization + real-volume executor path, rev 5: replacement test moved onto real volume path, rev 6: direct serialized-drain proof via bounded test hook, rev 7: manager-requested tightening of live-path and shutdown proofs)

#### Accepted contract

- `P4` closes stronger live runtime ownership for the chosen path, not broader control-plane closure
- the live volume-server path now owns:
  - start
  - cancel
  - replacement
  - cleanup
  for accepted recovery execution
- engine remains the policy owner
- runtime replacement is serialized: old owner drains before replacement starts
- shutdown drains live recovery owners before volumes close

#### Files changed

| File | Action |
|------|--------|
| `weed/server/volume_server_block.go` | Updated: `ProcessAssignments()` and `Shutdown()` wired to `RecoveryManager` |
| `weed/server/block_recovery.go` | New/updated: `RecoveryManager`, serialized cancel-and-drain ownership, bounded test hook |
| `weed/server/block_recovery_test.go` | New/updated: real-path live ownership proofs and tightened shutdown/replacement tests |

#### Test inventory (4 P4 tests)

| Test | Proves |
|------|--------|
| `LivePath_RealVol_ReachesPlan` | real `ProcessAssignments -> plan_catchup -> exec_catchup_started -> exec_completed -> in_sync` |
| `SerializedReplacement_DrainsBeforeStart` | old owner alive, old `done` open pre-supersede, old `done` closed before replacement proceeds |
| `ShutdownDrain` | live blocked task exists before shutdown and is drained to zero active tasks |
| `RebuildAddrScoped` | rebuild address sourced by matching volume path |

#### Architect judgment

`P4` is accepted.
The accepted `Phase 09` execution targets are now all closed on the chosen path:

1. `P1` full-base execution closure
2. `P2` snapshot execution closure
3. `P3` truncation execution closure
4. `P4` stronger live runtime ownership

#### Residual note

1. repeated primary assignment on the same volume still logs a low-severity rebuild-server double-start warning
2. broader control-plane closure remains outside `Phase 09`

---

### Phase 09 Closeout

Date: 2026-04-02
Status: complete

`Phase 09` is closed.

Closed slices:

1. `P0` production execution closure plan
2. `P1` full-base execution closure
3. `P2` snapshot execution closure
4. `P3` truncation execution closure
5. `P4` stronger live runtime ownership

Phase-level outcome:

The chosen `RF=2 sync_all` path now has accepted backend execution closure for:

1. full-base rebuild
2. snapshot-tail rebuild
3. truncation-safe replica-ahead correction with rebuild escalation
4. live runtime ownership on the volume-server path

Carry-forward remains bounded to later phases:

1. low-severity rebuild-server double-start/idempotence cleanup
2. broader control-plane closure
3. future durability modes / `RF>2` / product-surface rebinding
