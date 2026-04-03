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
> - Many tests labeled `FAIL expected` here actually **PASS** on current code — current code already passes tests associated with later checkpoint themes
> - Many tests labeled `verify` turned out to be **PASS** — whether that constitutes checkpoint closure requires dedicated review
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

---

### `CP13-2` Technical Pack

Date: 2026-04-03
Goal: close the canonical replica addressing gap so replication endpoint truth is always exported as routable authoritative `ip:port` rather than wildcard listener strings or incomplete address forms

#### Layer 1: Semantic Core

##### Problem statement

`CP13-1` froze the current replication baseline and left one explicit `PASS*` gap for canonical addressing:

1. `TestBug3_ReplicaAddr_MustBeIPPort_WildcardBind`

This means current code has evidence that the wildcard-bind address path is still only partially closed.

`CP13-2` therefore accepts only one bounded thing:

1. canonical endpoint truth for replica addresses used by the replication path

It does not accept:

1. durable-progress truth
2. reconnect handshake or catch-up
3. retention policy or rebuild fallback
4. broad networking redesign

##### State / contract

`CP13-2` must make these truths explicit:

1. exported replica addresses used by replication are canonical routable `ip:port`
2. wildcard listener strings and bare `:port` are not valid published replication truth
3. any canonicalization occurs at the production truth surface, not only in tests or log formatting
4. loopback stays loopback only when explicitly intended rather than by accidental leakage

##### Reject shapes

Reject before implementation if the checkpoint:

1. fixes only one string formatting path while another exported address path still leaks wildcard/bare-port truth
2. relies on test-side normalization rather than production-side canonicalization
3. mixes endpoint canonicalization with reconnect/rebuild protocol work

#### Layer 2: Execution Core

##### Current gap `CP13-2` must close

1. replica receiver / registration code may still export `:port` or wildcard-bind forms instead of routable `ip:port`
2. authoritative registry / heartbeat truth may diverge from local listener truth if canonicalization is partial

##### Suggested file targets

1. `weed/storage/blockvol/replica_receiver.go`
2. `weed/storage/blockvol/replica_meta.go`
3. nearby address-canonicalization helpers
4. `weed/server/master_block_registry.go`
5. heartbeat / registration path carrying replica endpoints, only if alignment is required

##### Validation focus

Required proofs:

1. wildcard-bind canonicalization proof
   - exported replication endpoint becomes canonical `ip:port`
2. no-leak proof
   - wildcard/bare-port listener strings do not escape into replication truth
3. boundedness proof
   - checkpoint remains about endpoint truth, not later protocol behavior

Reject if:

1. `TestBug3_ReplicaAddr_MustBeIPPort_WildcardBind` passes only because the test normalizes the string itself
2. one endpoint path is fixed while another published path still leaks stale/non-canonical truth
3. delivery claims imply reconnect/durability closure

##### Suggested first cut

1. identify the authoritative source of replica `DataAddr` / `CtrlAddr`
2. canonicalize there to routable `ip:port`
3. verify the same canonical truth is what registration / heartbeat / registry consumers observe
4. flip the `PASS*` address witness into a real proof

##### Assignment For `sw`

1. Goal
   - deliver bounded canonical replica addressing on the production truth surface
2. Required outputs
   - one implementation update at the real endpoint-truth surface
   - one focused proof package centered on wildcard-bind/exported-address correctness
   - one delivery note explaining:
     - files updated in place
     - why the chosen canonicalization point is authoritative
     - what later checkpoints remain untouched
3. Hard rules
   - do not touch reconnect handshake / catch-up logic
   - do not touch retention / rebuild policy
   - do not count test-side normalization as a real fix

##### Assignment For `tester`

1. Goal
   - validate that `CP13-2` closes canonical endpoint truth and nothing broader
2. Validate
   - wildcard-bind export becomes canonical `ip:port`
   - no wildcard/bare-port leak remains on the published replication path
   - no-overclaim around `CP13-3+`
3. Reject if
   - the proof stays witness-only
   - canonicalization is partial or inconsistent across exported surfaces
   - the delivery claims reconnect or durability semantics changed

#### Short judgment

`CP13-2` is acceptable when:

1. the wildcard-bind address witness becomes a real endpoint-truth proof
2. exported replica addresses are canonical `ip:port`
3. no wildcard/bare-port leak remains on the published replication path
4. the checkpoint stays clearly separate from `CP13-3+`

---

### `CP13-2` Delivery Pack

Bounded contract:

1. `CP13-2` accepts canonical replica addressing only
2. it does not accept durable-progress truth, reconnect/catch-up, retention, rebuild, or rollout claims

What `sw` should deliver:

1. one production-side canonicalization fix for replica endpoint truth
2. one focused proof package showing exported replication endpoints are canonical `ip:port`
3. one delivery note with:
   - changed files
   - proof shape
   - no-overclaim statement

Recommended delivery shape:

1. code:
   - update the authoritative endpoint-truth surface in place
2. tests:
   - strengthen `TestBug3_ReplicaAddr_MustBeIPPort_WildcardBind` from witness to proof
   - add one adjacent no-leak assertion only if needed to prove the production path rather than one helper
3. note:
   - explain why the fix closes canonical endpoint truth
   - explain why `CP13-3+` remains untouched

Review checklist:

1. does the fix happen at the real truth surface?
2. does exported replication truth now always use canonical `ip:port`?
3. are wildcard/bare-port leaks actually eliminated?
4. is the checkpoint still bounded to address truth?

---

### `CP13-3` Technical Pack

Date: 2026-04-03
Goal: make durable replication progress explicit and authoritative so sync correctness is tied to replica flushed durability rather than sender-side send progress or informal success heuristics

#### Layer 1: Semantic Core

##### Problem statement

`CP13-1` baseline shows many durable-progress tests already pass on current code, but baseline evidence alone does not close the checkpoint.

`CP13-3` therefore accepts only one bounded thing:

1. durable progress truth for the replication path

It does not accept:

1. reconnect/catch-up protocol
2. retention policy
3. rebuild fallback
4. broad replication state-machine closure

##### State / contract

`CP13-3` must make these truths explicit:

1. `replicaFlushedLSN` means replica-side WAL durability, not sender transmission progress
2. barrier success for sync correctness depends on durable progress truth
3. sender-side sent/shipped LSN remains diagnostic and must not authorize sync success
4. barrier/control surfaces expose durable progress explicitly enough to prove correctness

##### Reject shapes

Reject before implementation or review if the checkpoint:

1. treats passing baseline tests as automatic closure without restating the durable-progress contract
2. leaves sender-side send progress able to masquerade as durable authority
3. mixes durable-progress work with reconnect/rebuild protocol changes

#### Layer 2: Execution Core

##### Current gap `CP13-3` must close

1. durable progress truth may still be only implicitly inferred rather than explicitly owned and reviewed
2. baseline evidence suggests current code may already satisfy much of the contract, but that must be confirmed in a bounded proof package

##### Suggested file targets

1. `weed/storage/blockvol/replica_apply.go`
2. `weed/storage/blockvol/wal_shipper.go`
3. `weed/storage/blockvol/dist_group_commit.go`
4. protocol message definitions used by the barrier/control path

##### Validation focus

Required proofs:

1. barrier-uses-flushed proof
   - barrier success is grounded in `replicaFlushedLSN`
2. monotonicity proof
   - flushed progress is monotonic within epoch
3. no-false-authority proof
   - receive/sent/shipped progress alone does not count as durability authority
4. boundedness proof
   - checkpoint remains about durable-progress truth, not `CP13-4+`

Reject if:

1. a passing test still depends on sender-side send progress rather than replica durability
2. the proof package cannot explain why receive progress and flushed progress are distinct
3. delivery wording implies reconnect or retention semantics were closed

##### Suggested first cut

1. restate the durable-progress contract explicitly against current code
2. review the existing PASS baseline tests and identify which are:
   - real proof of the contract
   - adjacent support evidence
   - out of scope for `CP13-3`
3. make only the minimum code/test adjustments needed to close any remaining contract gap
4. produce one delivery note that explains why durable-progress truth is now explicit and bounded

##### Assignment For `sw`

1. Goal
   - deliver bounded durable-progress truth on the production replication path
2. Required outputs
   - one explicit durable-progress contract summary
   - one focused proof package centered on:
     - `replicaFlushedLSN`
     - barrier response truth
     - sender-side progress remaining diagnostic only
   - one delivery note explaining:
     - files updated in place
     - which baseline PASS tests now count as real `CP13-3` proof
     - what later checkpoints remain untouched
3. Hard rules
   - do not broaden into reconnect handshake / catch-up
   - do not broaden into retention or rebuild policy
   - do not treat “tests already pass” as sufficient without contract-level review

##### Assignment For `tester`

1. Goal
   - validate that `CP13-3` closes durable-progress truth and nothing broader
2. Validate
   - barrier success is tied to flushed durability
   - flushed progress is monotonic and not advanced on mere receive
   - sender-side shipped/sent progress remains diagnostic only
   - no-overclaim around `CP13-4+`
3. Reject if
   - durable-progress authority is still implicit or mixed
   - evidence relies on transport progress rather than replica durability
   - the delivery claims reconnect/retention/rebuild closure

#### Short judgment

`CP13-3` is acceptable when:

1. durable progress truth is explicit and authoritative
2. barrier correctness is tied to replica flushed durability
3. sender-side send progress is clearly non-authoritative
4. the checkpoint stays clearly separate from `CP13-4+`

---

### `CP13-3` Delivery Pack

Bounded contract:

1. `CP13-3` accepts durable-progress truth only
2. it does not accept reconnect/catch-up, retention, rebuild, or rollout claims

What `sw` should deliver:

1. one focused contract review of `replicaFlushedLSN` / barrier durable-progress truth
2. one bounded code/test package only if needed to close the contract
3. one delivery note with:
   - changed files
   - proof shape
   - which baseline PASS tests are promoted into real `CP13-3` evidence
   - no-overclaim statement

Recommended delivery shape:

1. contract:
   - define why `replicaFlushedLSN` is the authority for sync correctness
2. code/tests:
   - keep updates minimal and local to durable-progress surfaces
   - strengthen or narrow existing tests only where needed to prove the contract cleanly
3. note:
   - explain which proof cases are primary versus support evidence
   - explain why `CP13-4+` remains untouched

Review checklist:

1. is durable progress explicitly defined as replica durability?
2. does barrier success rely on flushed truth rather than send progress?
3. are sent/shipped progress variables clearly diagnostic only?
4. is the checkpoint still bounded to durable-progress truth?

---

### `CP13-4` Technical Pack

Date: 2026-04-03
Goal: make replica state and barrier eligibility explicit so only `InSync` replicas can satisfy sync durability while non-eligible states fail closed instead of drifting into accidental success

#### Layer 1: Semantic Core

##### Problem statement

`CP13-1` baseline and later review show state/eligibility behavior is partly present in current code, but the checkpoint is not closed until the state contract is made explicit and reviewed as a bounded object.

`CP13-4` therefore accepts only one bounded thing:

1. replica state and barrier eligibility truth

It does not accept:

1. reconnect/catch-up protocol
2. retention policy
3. rebuild fallback
4. broad rollout or performance claims

##### State / contract

`CP13-4` must make these truths explicit:

1. the replication path uses a bounded state set:
   - `Disconnected`
   - `Connecting`
   - `CatchingUp`
   - `InSync`
   - `Degraded`
   - `NeedsRebuild`
2. only `InSync` replicas count toward sync durability
3. non-eligible states must reject or fail closed rather than silently participating in barrier success
4. state eligibility is separate from later reconnect/rebuild protocol closure

##### Reject shapes

Reject before implementation or review if the checkpoint:

1. treats passing baseline tests as automatic closure without restating the state/eligibility contract
2. leaves one or more non-eligible states able to satisfy sync durability implicitly
3. mixes state/eligibility work with reconnect, retention, or rebuild implementation

#### Layer 2: Execution Core

##### Current gap `CP13-4` must close

1. state names and barrier eligibility rules may still be implicit or scattered
2. baseline evidence suggests some fail-closed behavior already exists, but that must be confirmed in a bounded proof package

##### Suggested file targets

1. `weed/storage/blockvol/wal_shipper.go`
2. `weed/storage/blockvol/dist_group_commit.go`
3. `weed/storage/blockvol/shipper_group.go`
4. focused protocol/adversarial tests for state eligibility

##### Validation focus

Required proofs:

1. only-in-sync proof
   - only `InSync` replicas satisfy barrier eligibility for sync durability
2. fail-closed proof
   - degraded/non-eligible replicas do not silently count toward `sync_all`
3. state-boundary proof
   - barrier rejects or excludes disallowed states explicitly
4. boundedness proof
   - checkpoint remains about state/eligibility truth, not `CP13-5+`

Reject if:

1. a passing test still allows a non-eligible state to count toward durable success
2. the proof package cannot explain how `CatchingUp`, `Degraded`, or `NeedsRebuild` are excluded
3. delivery wording implies reconnect/retention/rebuild closure

##### Suggested first cut

1. restate the replica-state eligibility contract explicitly against current code
2. review the existing PASS baseline tests and identify which are:
   - primary proof of the eligibility contract
   - adjacent support evidence
   - out of scope for `CP13-4`
3. make only the minimum code/test adjustments needed to close any remaining eligibility gap
4. produce one delivery note that explains why state/eligibility truth is now explicit and bounded

##### Assignment For `sw`

1. Goal
   - deliver bounded replica state / barrier eligibility truth on the production replication path
2. Required outputs
   - one explicit state/eligibility contract summary
   - one focused proof package centered on:
     - only `InSync` counts
     - non-eligible states fail closed
     - barrier admission/exclusion rules
   - one delivery note explaining:
     - files updated in place
     - which baseline PASS tests now count as real `CP13-4` proof
     - what later checkpoints remain untouched
3. Hard rules
   - do not broaden into reconnect handshake / catch-up
   - do not broaden into retention or rebuild policy
   - do not treat “tests already pass” as sufficient without contract-level review

##### Assignment For `tester`

1. Goal
   - validate that `CP13-4` closes replica state / barrier eligibility and nothing broader
2. Validate
   - only `InSync` replicas count toward sync durability
   - degraded/non-eligible replicas fail closed
   - state-boundary rejection/exclusion is explicit
   - no-overclaim around `CP13-5+`
3. Reject if
   - non-eligible states still leak into durable success
   - evidence relies on reconnect or rebuild behavior not in scope
   - the delivery claims reconnect/retention/rebuild closure

#### Short judgment

`CP13-4` is acceptable when:

1. the state set and eligibility rules are explicit
2. only `InSync` replicas count toward sync durability
3. non-eligible states fail closed rather than drifting into false success
4. the checkpoint stays clearly separate from `CP13-5+`

---

### `CP13-4` Delivery Pack

Bounded contract:

1. `CP13-4` accepts replica state / barrier eligibility only
2. it does not accept reconnect/catch-up, retention, rebuild, or rollout claims

What `sw` should deliver:

1. one focused contract review of replica state and barrier eligibility
2. one bounded code/test package only if needed to close the contract
3. one delivery note with:
   - changed files
   - proof shape
   - which baseline PASS tests are promoted into real `CP13-4` evidence
   - no-overclaim statement

Recommended delivery shape:

1. contract:
   - define the bounded state set and why only `InSync` is eligible
2. code/tests:
   - keep updates minimal and local to state/eligibility surfaces
   - strengthen or narrow existing tests only where needed to prove the contract cleanly
3. note:
   - explain which proof cases are primary versus support evidence
   - explain why `CP13-5+` remains untouched

Review checklist:

1. is the replica state set explicit?
2. does `sync_all` count only `InSync` replicas?
3. do non-eligible states fail closed?
4. is the checkpoint still bounded to state/eligibility truth?

---

### `CP13-5` Technical Pack

Date: 2026-04-03
Goal: make reconnect after replica disturbance explicit and correct so a replica with prior durable progress resumes through handshake truth and retained-WAL catch-up rather than unsafe bootstrap or barrier-level guesswork

#### Layer 1: Semantic Core

##### Problem statement

`CP13-4` accepted the replica state / barrier-eligibility contract.
That closed who may satisfy sync durability, but it did not close how a previously-synced replica returns to `InSync` after disturbance.

`CP13-1` baseline leaves the remaining reconnect cluster explicit:

1. `TestAdversarial_ReconnectUsesHandshakeNotBootstrap` fails
2. `TestAdversarial_CatchupMultipleDisconnects` fails
3. `TestAdversarial_CatchupDoesNotOverwriteNewerData` fails because catch-up does not complete cleanly enough to exercise the safety invariant
4. `TestReconnect_GapBeyondRetainedWal_NeedsRebuild` is still only `PASS*` because it proves bounded failure, not full rebuild-state closure

`CP13-5` therefore accepts only one bounded thing:

1. reconnect handshake + WAL catch-up truth for recoverable retained-WAL gaps

It does not accept:

1. replica-aware WAL retention policy
2. full `NeedsRebuild` lifecycle closure
3. rebuild execution/orchestration
4. broad rollout or performance claims

##### State / contract

`CP13-5` must make these truths explicit:

1. fresh replicas with no prior durable progress may bootstrap
2. replicas with prior durable progress must reconnect using explicit resume truth, not bare bootstrap
3. if the retained WAL covers the replica gap, catch-up must replay the missing WAL and only then allow barrier success
4. if the gap cannot be recovered within retained WAL, reconnect must fail closed in this checkpoint and leave full rebuild closure to `CP13-7`
5. repeated disconnect/reconnect cycles must remain bounded and recoverable rather than drifting into permanent degraded failure

##### Reject shapes

Reject before implementation or review if the checkpoint:

1. allows a previously-synced replica to succeed without using reconnect handshake truth
2. allows barrier success before catch-up has restored `InSync`
3. mixes reconnect/catch-up work with retention or rebuild lifecycle closure
4. claims full `NeedsRebuild` semantics as if `CP13-7` were already closed

#### Layer 2: Execution Core

##### Current gap `CP13-5` must close

1. degraded shippers with prior durable progress still fail to reconnect and catch up cleanly before barrier
2. repeated disconnect/reconnect cycles do not recover robustly
3. catch-up safety cases are blocked because reconnect/catch-up does not complete cleanly enough to exercise the intended invariant
4. gap-beyond-retained-WAL behavior is only bounded-failure evidence today, not a fully-reviewed reconnect contract

##### Suggested file targets

1. `weed/storage/blockvol/wal_shipper.go`
2. reconnect / catch-up helpers adjacent to `wal_shipper`
3. `weed/storage/blockvol/sync_all_protocol_test.go`
4. `weed/storage/blockvol/sync_all_adversarial_test.go`
5. nearby protocol message helpers only if the reconnect contract requires them

##### Validation focus

Required proofs:

1. reconnect-discriminator proof
   - fresh shipper bootstrap remains separate from reconnect-with-progress
2. catch-up proof
   - recoverable retained-WAL gap replays missing WAL and returns the shipper to `InSync`
3. repeated-recovery proof
   - multiple disconnect/reconnect cycles recover without hanging or silently bypassing catch-up
4. bounded-failure proof
   - unrecoverable gaps fail closed and do not claim reconnect success
5. boundedness proof
   - checkpoint remains about reconnect/catch-up truth, not `CP13-6+`

Reject if:

1. reconnect success is inferred only from a final passing `SyncCache` without proving the handshake/catch-up path
2. a proof relies on retention tuning or rebuild execution not accepted in this checkpoint
3. delivery wording collapses bounded failure on unrecoverable gaps into full `NeedsRebuild` lifecycle closure

##### Suggested first cut

1. restate the reconnect decision matrix explicitly against current code:
   - no prior durable progress -> bootstrap
   - prior durable progress + recoverable gap -> handshake + catch-up
   - prior durable progress + unrecoverable gap -> fail closed here, rebuild later
2. close the failing adversarial reconnect tests first
3. then tighten one safety proof that catch-up replays missing WAL without overwriting newer replica data
4. keep full rebuild-state closure and retention-policy broadening out of this checkpoint

##### Assignment For `sw`

1. Goal
   - deliver bounded reconnect handshake + WAL catch-up truth on the production replication path
2. Required outputs
   - one explicit reconnect/catch-up contract summary
   - one focused code/test package centered on:
     - reconnect discriminator
     - retained-WAL catch-up replay
     - repeated disconnect/reconnect recovery
     - bounded failure on unrecoverable gaps
   - one delivery note explaining:
     - files updated in place
     - which failing baseline tests are now closed by `CP13-5`
     - which `PASS` / `PASS*` baseline tests are promoted into real `CP13-5` evidence
     - what later checkpoints remain untouched
3. Hard rules
   - do not broaden into replica-aware retention policy
   - do not claim full `NeedsRebuild` lifecycle closure
   - do not use “final SyncCache passed” as the only reconnect proof

##### Assignment For `tester`

1. Goal
   - validate that `CP13-5` closes reconnect handshake / WAL catch-up and nothing broader
2. Validate
   - previously-synced replicas reconnect through resume truth rather than bootstrap
   - recoverable retained-WAL gaps replay and re-enter `InSync`
   - repeated disconnect/reconnect cycles recover
   - unrecoverable gaps fail closed without overclaiming `CP13-7`
   - no-overclaim around `CP13-6+`
3. Reject if
   - reconnect proof is indirect or only inferred from a final success result
   - evidence depends on retention/rebuild logic not actually accepted here
   - the delivery claims full rebuild or retention closure

#### Short judgment

`CP13-5` is acceptable when:

1. reconnect path selection is explicit and correct
2. retained-WAL catch-up closes the recoverable-gap path back to `InSync`
3. repeated disconnect/reconnect recovery is bounded and test-backed
4. unrecoverable gaps fail closed without pretending `CP13-7` is done
5. the checkpoint stays clearly separate from `CP13-6+`

---

### `CP13-5` Delivery Pack

Bounded contract:

1. `CP13-5` accepts reconnect handshake + WAL catch-up only
2. it does not accept replica-aware WAL retention, full rebuild fallback, or rollout claims

What `sw` should deliver:

1. one focused contract review of reconnect discriminator, resume truth, and catch-up admission
2. one bounded code/test package only where current code still fails the reconnect/catch-up contract
3. one delivery note with:
   - changed files
   - proof shape
   - which failing baseline tests are now closed
   - which baseline `PASS` / `PASS*` tests are promoted into real `CP13-5` evidence
   - no-overclaim statement

Recommended delivery shape:

1. contract:
   - define bootstrap vs reconnect and recoverable vs unrecoverable gap outcomes
2. code/tests:
   - keep updates local to reconnect/catch-up surfaces
   - make the handshake/catch-up path directly observable in proofs
3. note:
   - distinguish primary proof from support evidence
   - explain why `CP13-6+` remains untouched

Review checklist:

1. does prior durable progress force reconnect rather than bootstrap?
2. does recoverable gap replay happen before barrier success?
3. do repeated disconnect/reconnect cycles recover cleanly?
4. do unrecoverable gaps fail closed without overclaiming rebuild closure?
5. is the checkpoint still bounded to reconnect/catch-up truth?

---

### `CP13-6` Technical Pack

Date: 2026-04-03
Goal: make WAL retention explicit and replica-aware so reclaim preserves the retained-WAL window needed by recoverable replicas while bounded budgets still escalate safely when WAL cannot be held forever

#### Layer 1: Semantic Core

##### Problem statement

`CP13-5` accepted reconnect handshake + catch-up truth for recoverable gaps.
That closes how a recoverable replica returns to `InSync`, but it does not close how long the primary must preserve WAL so that recoverable path remains valid.

`CP13-1` baseline leaves the retention cluster explicit:

1. `TestWalRetention_RequiredReplicaBlocksReclaim` already passes and suggests the core hold-back behavior may exist
2. `TestWalRetention_TimeoutTriggersNeedsRebuild` already passes and suggests bounded timeout escalation may exist
3. `TestWalRetention_MaxBytesTriggersNeedsRebuild` is still `PASS*` because the code logs that max-bytes triggering is not fully implemented and the shipper stays degraded

`CP13-6` therefore accepts only one bounded thing:

1. replica-aware WAL retention truth and bounded retention-budget escalation

It does not accept:

1. full `NeedsRebuild` lifecycle closure
2. rebuild execution/orchestration
3. broad rollout or performance claims

##### State / contract

`CP13-6` must make these truths explicit:

1. recoverable replicas with prior durable progress may hold WAL needed for catch-up
2. retention inputs must be based on replica-aware durable progress, not sender-side guesses or primary-local convenience
3. reclaim may proceed only when a replica no longer needs the retained WAL window or when bounded budgets explicitly escalate the situation
4. timeout and max-bytes budgets must have real fail-closed consequences, not comment-only or log-only placeholders
5. full rebuild closure after escalation remains `CP13-7`

##### Reject shapes

Reject before implementation or review if the checkpoint:

1. reclaims WAL still needed by a recoverable replica
2. treats timeout/max-bytes behavior as accepted without a real state effect
3. mixes retention truth with full rebuild lifecycle closure
4. reopens `CP13-5` reconnect semantics instead of preserving them

#### Layer 2: Execution Core

##### Current gap `CP13-6` must close

1. max-bytes retention behavior is still only partial evidence (`PASS*`), not accepted truth
2. retention budgeting must be reviewed as an explicit contract rather than inferred from scattered passing tests
3. reclaim and escalation rules must remain aligned with the recoverable catch-up contract from `CP13-5`

##### Suggested file targets

1. `weed/storage/blockvol/flusher.go`
2. `weed/storage/blockvol/shipper_group.go`
3. `weed/storage/blockvol/wal_shipper.go` only if retention accounting truly depends on replica-state inputs there
4. `weed/storage/blockvol/sync_all_protocol_test.go`
5. nearby WAL-retention helpers and bounded component tests only if needed

##### Validation focus

Required proofs:

1. hold-back proof
   - required WAL is not reclaimed while a recoverable replica still needs it
2. timeout-budget proof
   - bounded timeout escalation works and releases the WAL hold safely
3. max-bytes-budget proof
   - bounded max-bytes escalation has a real state effect and is not just logged
4. compatibility proof
   - retention truth remains compatible with `CP13-5` recoverable catch-up
5. boundedness proof
   - checkpoint remains about retention truth, not `CP13-7+`

Reject if:

1. a passing test still relies on log text rather than observable state for max-bytes behavior
2. retention correctness is inferred only from local flusher progress rather than replica-aware durable progress
3. delivery wording collapses bounded escalation into full rebuild lifecycle closure

##### Suggested first cut

1. restate the retention decision matrix explicitly against current code:
   - recoverable replica still needs WAL -> hold reclaim
   - recoverable replica exceeds timeout budget -> bounded escalation, release hold
   - recoverable replica exceeds max-bytes budget -> bounded escalation, release hold
2. convert the existing max-bytes `PASS*` into a real proof first
3. then confirm the already-green hold-back and timeout tests are truly primary proof rather than witness coverage
4. keep full `NeedsRebuild` lifecycle and rebuild execution out of this checkpoint

##### Assignment For `sw`

1. Goal
   - deliver bounded replica-aware WAL retention truth on the production replication path
2. Required outputs
   - one explicit retention contract summary
   - one focused code/test package centered on:
     - recoverable-replica WAL hold-back
     - timeout budget
     - max-bytes budget
     - compatibility with reconnect/catch-up
   - one delivery note explaining:
     - files updated in place
     - which baseline `PASS` / `PASS*` tests are promoted into real `CP13-6` evidence
     - what later checkpoints remain untouched
3. Hard rules
   - do not claim full `NeedsRebuild` lifecycle closure
   - do not broaden into rebuild execution/orchestration
   - do not accept max-bytes behavior based only on log text or comments

##### Assignment For `tester`

1. Goal
   - validate that `CP13-6` closes replica-aware WAL retention and nothing broader
2. Validate
   - recoverable replicas hold WAL as needed for catch-up
   - timeout and max-bytes budgets trigger observable bounded escalation
   - retention remains aligned with `CP13-5` catch-up truth
   - no-overclaim around `CP13-7+`
3. Reject if
   - max-bytes proof remains witness-only
   - reclaim can still discard catch-up-critical WAL
   - the delivery claims rebuild lifecycle closure

#### Short judgment

`CP13-6` is acceptable when:

1. WAL retention is explicitly tied to recoverable replica progress
2. recoverable replicas block reclaim of required WAL
3. timeout and max-bytes budgets trigger real bounded escalation
4. the checkpoint stays clearly separate from `CP13-7+`

---

### `CP13-6` Delivery Pack

Bounded contract:

1. `CP13-6` accepts replica-aware WAL retention only
2. it does not accept full rebuild fallback, rebuild execution, or rollout claims

What `sw` should deliver:

1. one focused contract review of retention inputs, reclaim hold-back, and bounded budget escalation
2. one bounded code/test package only where current code still fails the retention contract
3. one delivery note with:
   - changed files
   - proof shape
   - which baseline `PASS` / `PASS*` tests are promoted into real `CP13-6` evidence
   - no-overclaim statement

Recommended delivery shape:

1. contract:
   - define who holds WAL, when reclaim may proceed, and what timeout/max-bytes escalation means
2. code/tests:
   - keep updates local to retention/reclaim surfaces
   - make timeout/max-bytes effects directly observable in proofs
3. note:
   - distinguish primary proof from support evidence
   - explain why `CP13-7+` remains untouched

Review checklist:

1. is retention tied to recoverable replica progress?
2. does reclaim stay blocked while catch-up-critical WAL is still needed?
3. do timeout and max-bytes budgets have real observable effects?
4. does retention remain compatible with `CP13-5` catch-up?
5. is the checkpoint still bounded to retention truth?

---

### `CP13-7` Technical Pack

Date: 2026-04-03
Goal: make `NeedsRebuild` a real fail-closed fallback so unrecoverable replicas stop participating in normal replication, expose rebuild intent clearly, and re-enter only through bounded rebuild handoff

#### Layer 1: Semantic Core

##### Problem statement

`CP13-6` accepted replica-aware WAL retention and bounded escalation into `NeedsRebuild`.
That closes when the system decides retained-WAL recovery is no longer valid, but it does not close what the system must do once `NeedsRebuild` is reached.

The remaining rebuild-fallback cluster is explicit in current evidence:

1. `TestAdversarial_NeedsRebuildBlocksAllPaths` still fails and is the main open blocker
2. `TestReconnect_GapBeyondRetainedWal_NeedsRebuild` is still only bounded evidence because it proves failure on an unrecoverable gap, not the full `NeedsRebuild` lifecycle
3. existing rebuild tests already suggest strong support evidence for heartbeat visibility, rebuild completion, epoch aborts, and post-rebuild progress initialization

`CP13-7` therefore accepts only one bounded thing:

1. `NeedsRebuild` fallback and bounded rebuild handoff truth

It does not accept:

1. broad rollout or production-workload validation
2. new protocol discovery outside rebuild fallback
3. broad performance claims

##### State / contract

`CP13-7` must make these truths explicit:

1. unrecoverable replicas transition to `NeedsRebuild`, not indefinite `Degraded`
2. a `NeedsRebuild` shipper is fail-closed:
   - normal ship/barrier participation must not continue as if it were recoverable
3. rebuild handoff is explicit:
   - visible in heartbeat / state surfaces
   - start/abort/complete paths are bounded and epoch-safe
4. post-rebuild progress/state is initialized from checkpoint truth so the replica can re-enter from a bounded baseline rather than stale or zeroed progress
5. full real-workload proof remains `CP13-8`

##### Reject shapes

Reject before implementation or review if the checkpoint:

1. leaves `NeedsRebuild` as a label without blocking normal replication paths
2. proves only “SyncCache failed” without proving actual `NeedsRebuild` state ownership
3. mixes rebuild fallback with real-workload validation or broad rollout claims
4. leaves post-rebuild progress/state ambiguous or stale

#### Layer 2: Execution Core

##### Current gap `CP13-7` must close

1. unrecoverable gap detection exists in parts of the system, but the main adversarial fail-closed lifecycle is not yet closed
2. `TestReconnect_GapBeyondRetainedWal_NeedsRebuild` still needs to be promoted from bounded failure evidence to real rebuild-fallback proof
3. `NeedsRebuild` must be shown to block ship/barrier paths and then hand off cleanly into rebuild/re-entry

##### Suggested file targets

1. `weed/storage/blockvol/wal_shipper.go`
2. `weed/storage/blockvol/shipper_group.go`
3. `weed/storage/blockvol/rebuild.go`
4. `weed/storage/blockvol/sync_all_adversarial_test.go`
5. `weed/storage/blockvol/sync_all_protocol_test.go`
6. `weed/storage/blockvol/rebuild_v1_test.go`

##### Validation focus

Required proofs:

1. transition proof
   - unrecoverable gap transitions to `NeedsRebuild`
2. fail-closed proof
   - `NeedsRebuild` blocks normal ship/barrier participation
3. surface proof
   - heartbeat / status surfaces expose `NeedsRebuild` clearly enough for rebuild orchestration
4. rebuild-handoff proof
   - rebuild complete / abort behavior is bounded and epoch-safe
5. post-rebuild-progress proof
   - rebuilt replica starts from checkpoint-based progress truth
6. boundedness proof
   - checkpoint remains about rebuild fallback, not `CP13-8+`

Reject if:

1. a test proves only degraded failure without proving `NeedsRebuild`
2. rebuild completion is accepted without checking post-rebuild progress/state initialization
3. delivery wording implies real-workload launch readiness

##### Suggested first cut

1. close `TestAdversarial_NeedsRebuildBlocksAllPaths` first
2. promote `TestReconnect_GapBeyondRetainedWal_NeedsRebuild` from bounded failure witness to real transition proof
3. then classify existing rebuild tests into:
   - primary proof
   - support evidence
   - out of scope for `CP13-7`
4. keep `CP13-8` workload claims out of the checkpoint

##### Assignment For `sw`

1. Goal
   - deliver bounded rebuild fallback truth on the production replication path
2. Required outputs
   - one explicit `NeedsRebuild` fallback contract summary
   - one focused code/test package centered on:
     - unrecoverable-gap transition
     - fail-closed blocking semantics
     - rebuild start/abort/complete handoff
     - post-rebuild progress/state initialization
   - one delivery note explaining:
     - files updated in place
     - which remaining `FAIL` / `PASS*` evidence is now closed by `CP13-7`
     - which existing rebuild tests are promoted into real `CP13-7` evidence
     - what later checkpoints remain untouched
3. Hard rules
   - do not broaden into real-workload benchmarking/validation
   - do not claim generic rollout readiness
   - do not treat “rebuild exists” as sufficient without fail-closed lifecycle proof

##### Assignment For `tester`

1. Goal
   - validate that `CP13-7` closes rebuild fallback and nothing broader
2. Validate
   - unrecoverable replicas become `NeedsRebuild`
   - `NeedsRebuild` blocks normal replication paths
   - heartbeat/status surfaces expose rebuild intent
   - rebuild handoff is bounded and epoch-safe
   - post-rebuild progress/state is initialized correctly
   - no-overclaim around `CP13-8+`
3. Reject if
   - the main adversarial `NeedsRebuild` fail-closed test is still open
   - proof depends on workload/rollout claims outside scope
   - post-rebuild progress is still stale, zeroed, or implicit

#### Short judgment

`CP13-7` is acceptable when:

1. unrecoverable gaps transition cleanly to `NeedsRebuild`
2. `NeedsRebuild` is genuinely fail-closed for normal replication paths
3. rebuild handoff/re-entry is bounded and checkpoint-based
4. the checkpoint stays clearly separate from `CP13-8+`

---

### `CP13-7` Delivery Pack

Bounded contract:

1. `CP13-7` accepts rebuild fallback only
2. it does not accept workload validation, rollout claims, or broad performance positioning

What `sw` should deliver:

1. one focused contract review of `NeedsRebuild` transition, blocking semantics, and rebuild handoff
2. one bounded code/test package only where current code still fails the rebuild-fallback contract
3. one delivery note with:
   - changed files
   - proof shape
   - which remaining `FAIL` / `PASS*` tests are now closed
   - which existing rebuild tests are promoted into real `CP13-7` evidence
   - no-overclaim statement

Recommended delivery shape:

1. contract:
   - define when `NeedsRebuild` is entered, what it blocks, and how rebuild hands off back to a bounded re-entry state
2. code/tests:
   - keep updates local to rebuild fallback surfaces
   - make fail-closed blocking and post-rebuild progress directly observable in proofs
3. note:
   - distinguish primary proof from support evidence
   - explain why `CP13-8+` remains untouched

Review checklist:

1. do unrecoverable gaps really transition to `NeedsRebuild`?
2. does `NeedsRebuild` block normal ship/barrier paths?
3. is rebuild handoff bounded and epoch-safe?
4. is post-rebuild progress initialized from checkpoint truth?
5. is the checkpoint still bounded to rebuild fallback?
