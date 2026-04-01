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
