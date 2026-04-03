Purpose: append-only technical pack and delivery log for `Phase 13` sync replication correctness.

---

### `CP13-1` Technical Pack

Date: 2026-04-02
Goal: freeze a focused test-first baseline for sync replication correctness before major implementation work so `Phase 13` closes real gaps rather than validating against moving expectations

#### Layer 1: Semantic Core

##### Problem statement

`Phase 12` accepted bounded hardening and a first launch envelope on the chosen path.
That acceptance did not prove that cross-machine `RF=2 sync_all` already has a fully explicit replicated-durability model for:

1. reconnect after outage
2. catch-up from retained WAL
3. durable-progress truth at barrier time
4. retention vs rebuild boundary

The first checkpoint therefore accepts only one bounded thing:

1. a frozen failing/passing baseline for the replication gaps that `Phase 13` will close

It does not accept:

1. protocol implementation by implication
2. broad performance or rollout claims
3. happy-path-only validation

##### State / contract

`CP13-1` must make these truths explicit:

1. the target replication gaps are named before implementation
2. at least one current-code failure exists for each major missing protocol property
3. already-correct behavior may remain green and should be recorded as such
4. later checkpoints must refer back to this baseline rather than redefining success after the fact

##### Reject shapes

Reject before implementation if the checkpoint:

1. adds tests only after protocol code lands
2. reports “some failures happened” without mapping failures to named gaps
3. mixes proxy coverage and true proof coverage without distinction
4. quietly turns the baseline into broad workload benchmarking

#### Layer 2: Execution Core

##### Current gaps `CP13-1` must expose

1. canonical replica endpoint truth may be weaker than real cross-machine requirements
2. barrier correctness may still depend on sender-side progress rather than flushed durability truth
3. reconnect / catch-up behavior may fail or degrade unclearly after outage
4. retention / rebuild boundary may be implicit instead of explicit

##### Suggested file targets

1. `weed/storage/blockvol/blockvol_test.go`
2. `weed/storage/blockvol/replica_test.go`
3. `weed/storage/blockvol/dist_group_commit_test.go`
4. `weed/storage/blockvol/wal_shipper_test.go`
5. `weed/storage/blockvol/test/component/`
6. `weed/storage/blockvol/testrunner/scenarios/internal/` for bounded real-node scenarios when justified

##### Validation focus

Required proofs:

1. baseline-freeze proof
   - the focused tests are added before the major protocol checkpoints land
2. gap-visibility proof
   - named protocol gaps fail clearly on current code or are marked as bounded witness coverage
3. boundedness proof
   - the checkpoint remains test-first baseline work, not hidden implementation

Reject if:

1. tests are too indirect to say which gap they expose
2. failing behavior is captured only in chat or terminal output, not in a baseline artifact
3. baseline wording already claims the later protocol is fixed

##### Suggested first cut

1. prepare a compact test inventory grouped by:
   - address truth
   - durable progress truth
   - reconnect / catch-up
   - retention / rebuild boundary
2. run on current code
3. freeze one baseline report with explicit categories:
   - `FAIL`
   - `PASS`
   - `PASS*`

##### Assignment For `tester`

1. Goal
   - add and run the focused replication-gap tests before `sw` starts major protocol work
2. Required outputs
   - one frozen baseline report
   - one explicit list of current expected failures
   - one explicit list of already-green behaviors
3. Hard rules
   - do not strengthen the current implementation first
   - do not let component or real-node tests replace the smaller protocol-gap tests
   - do not turn proxy passes into full-proof claims

##### Assignment For `sw`

1. Goal
   - start `Phase 13` immediately by making the baseline package real without pre-solving the protocol gaps
2. Allowed work before baseline freeze
   - test harness support that does not change protocol behavior
   - small cleanup required to make the baseline runnable
   - test additions or renames that make the current gaps explicit
3. Hard rules
   - do not pre-fix reconnect / catch-up / retention semantics before the baseline is captured
   - do not weaken current degraded-mode signaling just to make tests pass

##### `P1` Start Pack For `sw`

`sw` may start now, but only inside this bounded `CP13-1` package.

---

###### Task 1: Baseline Inventory Freeze

> **Note:** This section was the initial expected inventory written before the baseline run.
> It has been **superseded** by the actual frozen baseline in `phase-13-cp1-baseline.md`.
> See that file for the real PASS / FAIL / PASS* results from running on current code.
>
> Key corrections from the actual run:
> - Many tests labeled `FAIL expected` here actually **PASS** on current code (CP13-3/4/5/6 behavior already implemented in earlier phases)
> - Many tests labeled `verify` turned out to be **PASS** (real proof, not just witness)
> - Only **4 tests actually FAIL** and **3 are PASS*** — see `phase-13-cp1-baseline.md` for the authoritative list

###### Task 2: Runnable Baseline Harness

Fix only the minimum harness friction needed to run the baseline cleanly:

- if any test cannot compile or run due to missing test helpers, add the helpers
- if any test panics on setup (not on the gap itself), fix the setup
- do NOT change protocol behavior to make failing tests pass
- do NOT add new protocol code (reconnect, retention, rebuild)

Scope guard: if a fix touches `wal_shipper.go`, `replica_apply.go`, `dist_group_commit.go`, or `blockvol.go` beyond test-helper support, it is out of bounds for Task 2.

###### Task 3: Focused Gap Tests

Add or tighten the smallest set of tests that exposes the current gap on present code:

- if the inventory has a `verify` entry that turns out to be proxy coverage (passes but doesn't actually prove the property), reclassify it as `PASS*`
- if a gap has no test at all, add the minimum test that fails on current code
- prefer unit/protocol tests; add component tests only where the unit test cannot expose the gap
- each new test must have a comment naming which CP13 checkpoint it maps to

Hard rule: do NOT add tests that only pass after protocol work. The baseline must fail cleanly on current code.

###### Task 4: Frozen Baseline Report

Run the full inventory on current code and produce one explicit report:

```
CP13-1 Baseline Report
Date: YYYY-MM-DD
Commit: <hash>

Category 1: Address Truth
  PASS   TestCanonicalizeAddr_WildcardIPv4_UsesAdvertised
  PASS   TestCanonicalizeAddr_...
  PASS*  TestBug3_ReplicaAddr_MustBeIPPort_WildcardBind — documents gap, not proof
  ...

Category 2: Durable Progress Truth
  FAIL   TestReplicaProgress_BarrierUsesFlushedLSN — barrier doesn't gate on flushedLSN
  PASS   TestDistSync_SyncAll_NilGroup_Succeeds
  ...

Category 3: Reconnect / Catch-up
  FAIL   TestReconnect_CatchupFromRetainedWal — no catch-up protocol
  ...

Category 4: Retention / Rebuild Boundary
  FAIL   TestWalRetention_RequiredReplicaBlocksReclaim — WAL reclaim not replica-aware
  PASS   TestHeartbeat_ReportsPerReplicaState
  ...

Summary: X PASS / Y FAIL / Z PASS* out of N total
```

The report must be saved to `sw-block/.private/phase/phase-13-cp1-baseline.md`.

---

###### Required output from `sw`

1. one delivery note naming:
   - files changed (test helpers, test additions, renames)
   - tests added or strengthened
   - which gaps are now exposed by the baseline
2. one frozen result summary (`phase-13-cp1-baseline.md`)
3. one explicit statement of what `sw` did NOT fix

###### Hard boundary

`sw` may do Tasks 1-4 now. `sw` may NOT:

1. implement reconnect handshake protocol (→ CP13-5)
2. implement WAL retention policy (→ CP13-6)
3. implement rebuild fallback behavior (→ CP13-7)
4. change barrier protocol to return flushedLSN (→ CP13-3)
5. add CatchingUp or NeedsRebuild state transitions (→ CP13-4, CP13-5)
6. change `wal_shipper.go` Ship/Connect behavior beyond test-helper wiring

These are explicitly reserved for CP13-2 through CP13-7. The baseline must expose the gaps without closing them.

###### Reject if `sw`

1. starts implementing reconnect protocol, retention policy, or rebuild behavior before the baseline is frozen
2. buries the real gap under large harness churn
3. upgrades witness coverage into proof coverage without saying so
4. turns `CP13-1` into `CP13-2+` by stealth

---

#### Gap → Checkpoint Mapping

| Gap | Exposed by baseline tests | Closed by checkpoint |
|-----|--------------------------|---------------------|
| ReplicaReceiver returns `:port` not `ip:port` | TestBug3 (PASS*) | CP13-2 |
| Barrier doesn't gate on replicaFlushedLSN | TestReplicaProgress_BarrierUsesFlushedLSN (FAIL) | CP13-3 |
| No CatchingUp state, no barrier rejection during catch-up | TestBarrier_DuringCatchup_Rejected (FAIL) | CP13-4 |
| No reconnect handshake or WAL catch-up replay | TestReconnect_CatchupFromRetainedWal (FAIL) | CP13-5 |
| WAL reclaim not replica-aware | TestWalRetention_RequiredReplicaBlocksReclaim (FAIL) | CP13-6 |
| No NeedsRebuild transition on WAL gap | TestReconnect_GapBeyondRetainedWal_NeedsRebuild (FAIL) | CP13-5 + CP13-7 |
| Post-reconnect barrier hangs | TestBug2 (FAIL) | CP13-5 |

#### Short judgment

`CP13-1` is acceptable when:

1. the phase has a frozen, named failing baseline
2. the failing baseline maps cleanly to later checkpoints
3. already-correct behavior is distinguished from true gaps
4. no implementation overclaim sneaks into the checkpoint
