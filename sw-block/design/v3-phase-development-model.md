# V3 Phase Development Model

Date: 2026-04-11 (methodology stable; **NOT current-state — see §0 below**)
Status: methodology reference; **for current state see [`v3-dev-roadmap.md`](./v3-dev-roadmap.md)**
Purpose: define how `V3` phases should be run so every phase has a clear target, a closed loop, and a visible path toward production scope

## §0 Document scope (added 2026-04-26)

**This doc is the methodology, NOT the current state.** Sections describe the closed-loop discipline, target/proof/closure rules, and suggested phase shapes. Some illustrative examples (especially §7 "Suggested V3 Phases" and §15 "Production Roadmap Layer") were drafted Apr 2026 and may not match current reality.

**For current state, go to:**

| Question | Doc |
|---|---|
| What's done, what's next, where are we? | [`v3-dev-roadmap.md`](./v3-dev-roadmap.md) |
| What does P15 promise (22 gates)? | [`v3-phase-15-mvp-scope-gates.md`](./v3-phase-15-mvp-scope-gates.md) |
| Why pre-declared topology + authority discipline? | [`v3-product-placement-authority-rationale.md`](./v3-product-placement-authority-rationale.md) |

**This doc remains canonical for**: (§1-§6) closed-loop discipline rules; (§8) anti-pattern closure mapping; (§9) phase-gates-must-stay-narrow rule; (§10-§14) repo bootstrap + naming conventions. These are methodology-stable.

---



## 1. Short Answer

Yes, `V3` should use phase-based development.

In fact, `V3` needs phase discipline even more than `V2`, because:

1. `V3` is trying to protect semantic cleanliness
2. it is likely to reuse execution muscles that do not fully align
3. it may later support multiple runtimes such as Go and Rust
4. it must not quietly accumulate workaround semantics while "moving fast"

So the point of phases is not bureaucracy.

The point is:

1. each phase must have one clear target
2. each phase must define what it will not do
3. each phase must define what counts as proof
4. each phase must close its loop before the next phase expands scope

## 2. What A Closed Loop Means In V3

For `V3`, a phase is not closed when code exists.

A phase is closed only when all four are true:

1. the semantic target is explicit
2. `sw` implemented only that target
3. `tester` validated the intended proof level
4. the result was recorded in a durable phase decision/progress artifact

That means the loop is:

```text
target
  -> implementation
  -> evidence
  -> review
  -> accepted closure
```

If one of these is missing, the phase is not closed.

## 3. Why V3 Needs Stronger Phase Rules Than V2

`V2` had to discover many truths while implementation was already moving.

`V3` has a different job:

1. preserve the semantic lessons already learned
2. prevent old route pollution from re-entering
3. convert design package into a portable semantic platform

That means `V3` phases should be stricter about:

1. semantic scope
2. non-goals
3. evidence gates
4. reuse boundaries

## 4. Phase Shape

Each `V3` phase should contain three documents:

1. `phase-xx.md`
2. `phase-xx-log.md`
3. `phase-xx-decisions.md`

This follows the earlier project process, but the content emphasis changes for `V3`.

### 4.1 `phase-xx.md`

Use for:

1. current phase target
2. current scope
3. explicit non-goals
4. accepted guardrails
5. handoff expectations for `sw` and `tester`

It should stay short.

### 4.2 `phase-xx-log.md`

Use for:

1. design evolution
2. review corrections
3. carry-forward items
4. reuse notes from `V2`
5. why a boundary moved or stayed fixed

It can be longer.

### 4.3 `phase-xx-decisions.md`

Use for:

1. durable decisions
2. gate decisions
3. accepted compromises
4. explicit "do not re-open without new evidence" items

This keeps `V3` from re-arguing the same architecture boundary every week.

## 5. Required Fields For Every V3 Phase

Every phase should explicitly answer these questions.

### 5.1 Target

What single thing is this phase trying to establish?

Good examples:

1. mini engine can execute deterministic conformance
2. adapter-backed route can drive one real recovery family path
3. selected `V2` scenarios calibrate correctly under `V3`

Bad examples:

1. improve architecture
2. make `V3` better
3. support more stuff

### 5.2 Scope

What is included in this phase?

### 5.3 Non-goals

What is explicitly excluded in this phase?

This is critical for `V3`, because otherwise:

1. storage extensions leak in early
2. scheduler policy leaks into semantic core
3. mismatched `V2` muscles get imported "temporarily"

### 5.4 Reuse boundary

What may be reused from `V2`, and in what form?

Examples:

1. reference only
2. wrapper allowed
3. copy allowed
4. must rewrite in native `V3` form

### 5.5 Required proof

What evidence level is required?

Examples:

1. schema validation
2. conformance runner
3. adapter-backed calibration
4. selected runner scenarios

### 5.6 Reject conditions

What makes this phase fail, even if code exists?

Examples:

1. duplicate semantic route appears
2. projection is reused as control input
3. terminal success has multiple authorities
4. adapter silently decides policy

### 5.7 Closure Checklist

Every phase should also answer three closure questions explicitly.

These are not optional review style points.

They are part of whether the phase is actually closed.

#### A. Semantic closure

Question:

Has the semantic route evolved far enough for this phase, with the required
constraints complete and no hidden anti-pattern dependence?

Must check:

1. the engine / adapter / accepted command boundary now covers the semantic target of this phase
2. stale, reorder, overlap, timeout, cancel, and handoff behavior are fail-closed at the proof level required by this phase
3. target meaning, lineage meaning, and terminal truth authority are still explicit and fixed
4. no new surface quietly turns progress, projection, transport state, or policy hints into semantic truth
5. relevant anti-patterns from `protocol-anti-patterns.md` are either:
   - already closed
   - explicitly bounded in this phase
   - or explicitly deferred to a named later phase

Short form:

**the semantic contract needed by this phase is complete enough, and it does not
quietly depend on timing luck or execution residue.**

#### B. Functional closure

Question:

Does the new function introduced in this phase, including any `V2` muscle
migration, work cleanly and honestly at the intended boundary?

Must check:

1. new behavior is inside the declared scope of the phase
2. any migrated `V2` execution muscle names what old semantic coupling was removed
3. code path, docs, scope statement, and test/evidence story all describe the same capability
4. integration behavior exists at the intended level, not only isolated local unit success
5. the claimed feature has enough evidence to prevent "implemented but not actually closed" drift

Short form:

**new function and migrated execution both land cleanly, and the evidence matches
the real product/runtime claim.**

#### C. Phase-responsibility closure

Question:

Is this work closed in the correct phase, rather than quietly borrowing authority
from a later phase or leaving a current-phase obligation unresolved?

Must check:

1. the work belongs to this phase's declared responsibility line
2. this phase does not silently pull policy, topology authority, or product meaning backward from a later phase
3. anything not closed here is explicitly assigned to a later named phase
4. if a task or PR spans more than one responsibility line, the split or the phase definition should be re-reviewed

Short form:

**the work is not only correct; it is closed at the correct layer and in the
correct phase.**

## 6. The V3 Phase Loop

Each phase should run through the same loop.

### Step 1: Phase definition

Owner:

1. manager / architect

Output:

1. target
2. scope
3. non-goals
4. gate

### Step 2: Expectation definition

Owner:

1. tester

Output:

1. must-pass expectations
2. failure-class checklist
3. required test level
4. reject conditions

### Step 3: Implementation

Owner:

1. sw

Output:

1. code
2. delivery template
3. trace hooks
4. reuse note

### Step 4: Technical review

Owner:

1. architect

Output:

1. correctness review
2. semantic-boundary review
3. fail-closed review

### Step 5: Evidence closure

Owner:

1. tester

Output:

1. runner/conformance evidence
2. calibration closure
3. reject-or-accept statement

### Step 6: Durable closure

Owner:

1. manager / architect

Output:

1. phase decision
2. carry-forward list
3. next-phase handoff

## 7. Suggested V3 Phases

The exact numbering can change, but the semantic order should not.

### Phase 0: Constitution Freeze

Target:

1. freeze the initial `V3` semantic package enough to begin implementation

Close when:

1. truth domains are accepted
2. mini engine unit is accepted
3. metadata boundary is accepted
4. growth plan and calibration plan exist

### Phase 1: Initial Core

Target:

1. executable deterministic mini engine

Close when:

1. schema loads
2. engine applies events deterministically
3. conformance examples run
4. stale rejection and projection derivation are reviewable

### Phase 2: Reference Runtime

Target:

1. a small replay/conformance runtime exists

Close when:

1. conformance runner works
2. traces are readable
3. semantic outputs are replayable

### Phase 3: First Adapter Route

Target:

1. one real adapter-backed route exists from runtime facts to session close

Close when:

1. no duplicate trigger path exists
2. no duplicate terminal path exists
3. adapter does not silently own policy

### Phase 4: Runnable Block Sparrow

Target:

1. one complete but narrow block slice can run end-to-end through the new core

Close when:

1. one basic block route can be started and exercised end-to-end
2. the route still uses one semantic decision path and one terminal-close path
3. the slice is narrow, but it is complete enough to act as the first public runnable check-in
4. phase output can honestly say "this repo runs a basic block slice"

### Phase 5: Bootstrap And Validation Stabilization

Target:

1. the runnable block sparrow becomes easy to start, inspect, and validate repeatedly

Close when:

1. the repo has a minimal bootstrap and read-only inspection surface for the runnable slice
2. the repo has a minimal test interface for validating the runnable slice
3. first-time users can start, inspect, and troubleshoot the slice without reading deep design docs
4. progress remains visible and honest in the standalone repo
5. no real operator CLI is introduced before a later single-node demo stage

### Phase 6: Calibration MVP

Target:

1. selected `V2` scenarios are calibrated through the runnable `V3` route

Close when:

1. chosen calibration scenarios pass
2. semantic route matches observed runtime route
3. tester closes the calibration evidence

### Phase 7: Persistent Single-Node Slice

Target:

1. admit one persistence-backed single-node block slice cleanly

Close when:

1. one persistence-backed single-node slice survives stop/restart with data still present
2. the persistence seam does not pollute the semantic core
3. the result stays honest about proving local persistence rather than broader durability

### Phase 8: Local Data Process

Target:

1. admit one coherent local data process around the accepted persistence seam

Close when:

1. local read/write/flush/checkpoint/recover responsibilities are explicit
2. bounded abrupt-termination proof exists for the accepted crash model
3. the local data process does not pollute the semantic core

### Phase 9: Data Sync Process

Target:

1. admit one coherent data-sync institution for catch-up and rebuild data movement

Close when:

1. data movement, barrier, and achieved-frontier feedback responsibilities are explicit
2. replication/rebuild data transfer no longer lives as scattered transport muscle
3. the data-sync process does not decide recovery class or terminal truth

This phase is where the remaining data-plane closure work belongs:

1. data-sync execution must be one named institution rather than scattered transport helpers
2. byte movement, barrier, achieved-frontier, and restart/retry behavior must be explicit
3. recovery traffic must stay lineage-bound and fail closed under stale or overlapping execution
4. this phase closes data movement correctness, not topology governance

Practical sequencing rule:

1. implementation of `Phase 9` and `Phase 10` may overlap
2. declared closure must still treat `Phase 9` as earlier, because `Phase 10`
   timeout/cancel/lifecycle claims depend on `Phase 9` wire correctness and
   lineage-safe feedback behavior

### Phase 10: Recovery Execution Process

Target:

1. admit one coherent recovery execution institution around engine-issued commands

Close when:

1. catch-up and rebuild execution lifecycles are explicit
2. engine-issued targets remain fixed through execution
3. the execution process does not reinterpret command intent or publish terminal semantic truth by itself

This phase is where the remaining execution-side handoff closure belongs:

1. command-to-execution-to-close lifecycle must be explicit for catch-up and rebuild
2. old execution must become semantically dead immediately after newer lineage exists
3. delayed callback, retry, timeout, cancellation, and overlap rules must be explicit and tested
4. this phase closes recovery execution semantics, not promotion or failover policy

### Phase 11: Single-Node Product Surface

Target:

1. make the accepted single-node route operable as a bounded product surface

Close when:

1. one operator-usable single-node workflow exists
2. observability and safe inspection are strong enough for repeatable local operation
3. the product surface still reads truth from accepted lower institutions rather than inventing a new authority path

This phase is where bounded single-node governance surfaces belong:

1. local operator visibility, inspection, and safe action surfaces become explicit
2. the repo should state clearly that current closure is recovery semantics on one node, not cluster topology governance
3. any "promote" or "failover" wording must remain out of scope unless backed by later replicated-phase evidence
4. `Phase 11` surface must not expose cluster-shaped APIs that are likely to require breaking meaning changes when later topology phases land

### Phase 12: Replicated Durable Slice

Target:

1. admit one honest replicated durable slice around the accepted route

Close when:

1. the replicated route supports bounded catch-up and rebuild with honest failover/rejoin behavior
2. durability and fencing claims are backed by explicit evidence
3. replicated execution still preserves the accepted semantic ownership split

This phase is the first phase that should close bounded failover behavior:

1. reassignment, failover, rejoin, and fencing behavior become explicit replicated claims here
2. the product may claim one bounded failover/rejoin contract only after explicit evidence exists here
3. this phase still consumes topology facts from above; it does not yet own broad topology arbitration policy

Non-goals that must stay explicit:

1. `Phase 12` may consume `epoch` from an authority, but it must not mint or
   govern `epoch` itself
2. `Phase 12` may use a trivial test/manual authority as a placeholder
3. `Phase 12` does not choose which node becomes the new primary
4. `Phase 12` does not decide what condition should trigger a failover

Practical bounded-contract checklist:

1. given `(old primary, new primary, epoch+1)`, rejoin must converge without stale execution pollution
2. acknowledged or rejected stale callbacks during epoch turnover must not contaminate current truth
3. bounded failover/rejoin claims must name what is accepted
4. bounded failover/rejoin claims must also name what remains outside this phase

### Phase 13: Production Hardening And Release Gate

Target:

1. close the mainline product hardening gate for the accepted replicated slice

Close when:

1. qualification, observability, and release criteria are explicit
2. the repo can state a bounded production-ready claim honestly
3. the mainline can hand off to later scale and ecosystem phases without reopening core authority boundaries

This phase is where bounded first-launch governance closure belongs:

1. bounded failover/publication/runbook criteria should be explicit
2. operational disturbance handling must be clear enough for a bounded production-ready statement
3. this is still not the phase for broad topology expansion or broad placement policy

### Phase 14: Scale And Topology Expansion

Target:

1. widen the accepted product into larger-scale and richer-topology deployment shapes

Close when:

1. placement, rebalance, and failover behavior remain correct under the accepted expanded topology set
2. scale/topology work reuses the accepted semantic and execution institutions rather than replacing them
3. the resulting topology claims are backed by explicit evidence

This phase is where topology governance and richer failover policy belong:

1. topology authority, placement, rebalance, and failover policy become explicit institutions here
2. eligibility, selection, and topology-wide convergence policy should be closed here rather than hidden inside recovery execution
3. only from this phase onward should the product claim richer topology behavior beyond the bounded replicated slice

Mainline product meaning:

1. `Phase 14` is not a side expansion line; it is the mainline stage where fuller topology/governance product closure should land
2. if the repo wants to aim at a real full product rather than a bounded replicated slice, that aim must pass through `Phase 14`

### Phase 15: Operations And Ecosystem Expansion

Target:

1. complete the mainline product loop through operations and ecosystem integration

Close when:

1. operator workflows, ecosystem entry points, and lifecycle operations are production-reviewable
2. the ecosystem surface still preserves accepted truth boundaries
3. the mainline product and operations story is complete enough to shift later work into expansion lines

This phase is where cluster-facing operator policy becomes consumable:

1. operator workflows for failover, recovery supervision, and lifecycle control become explicit product surfaces here
2. ecosystem entry points may expose topology/failover operations only after the lower topology-policy phases are already closed
3. this phase makes the accepted governance and topology behavior operable; it does not invent new truth

Mainline product meaning:

1. `Phase 15` is where the mainline product loop becomes consumable through operations and ecosystem surfaces
2. `Phase 14` + `Phase 15` together should be read as the intended full-product completion target for the mainline

## 8. Anti-Pattern Closure Mapping

`V3` phases should explicitly track which anti-patterns are:

1. already avoided in the semantic core
2. only partially closed
3. deferred until adapter/runtime phases

Reference:

1. `protocol-anti-patterns.md`

### 8.1 A1: Heartbeat Timing Defines Recovery Semantics

Status:

1. **not fully closed in Phase 1-2**

Reason:

1. the semantic core already decides from bounded facts rather than timer values
2. but the real closure requires a live adapter/runtime path where heartbeat, probe, and refresh can arrive in different timings

Closure phase:

1. primary closure target: **Phase 3**
2. calibration proof target: **Phase 6**

Required proof:

1. heartbeat timing changes latency only
2. it does not directly choose recovery class
3. selected reconnect/rejoin scenarios prove this through the adapter-backed route

### 8.2 A4: Event Ordering Determines Semantics

Status:

1. **partially closed in Phase 1-2**

Reason:

1. the mini engine already rejects stale epoch/endpoint/session and uses bounded facts for decisions
2. but full closure requires proof that runtime/adapter event ordering still converges to the same semantic result

Closure phase:

1. semantic-core partial closure: **Phase 1**
2. replay/runtime strengthening: **Phase 2**
3. adapter/runtime closure: **Phase 3**
4. calibration proof: **Phase 6**

Required proof:

1. same facts produce the same commands/projection regardless of arrival ordering
2. replay and adapter-backed scenario evidence agree

### 8.3 A7: Transport Mechanics Leak Into the Semantic Engine

Status:

1. **avoided in Phase 1-2 engine/runtime core**
2. **not yet fully proven at integration boundary**

Reason:

1. the semantic reducer and reference runtime are intentionally transport-free
2. but real closure requires attaching execution muscles without reintroducing transport state into semantic logic

Closure phase:

1. engine/runtime avoidance: **Phase 1-2**
2. integration closure: **Phase 3**
3. calibration proof: **Phase 6**

Required proof:

1. transport lifecycle stays in adapter/runtime
2. engine only receives normalized facts
3. no semantic rule depends on TCP/session plumbing details

### 8.4 Practical Rule

If a phase touches runtime integration and does not say how it preserves closure
against `A1`, `A4`, and `A7`, then the phase definition is incomplete.

## 9. Phase Gates Must Stay Narrow

A common failure is letting one phase carry too much ambition.

For `V3`, phase gates should be narrow enough that failure is diagnosable.

Bad phase gate:

1. "V3 MVP should support failover, rebuild, RF3, SmartWAL, and Rust readiness"

Good phase gate:

1. "one adapter-backed route handles short-gap and long-gap recovery with one terminal-close authority"

Narrow gates are important because they make closure real.

## 10. Repo Bootstrap After Phase 4

`Phase 4` should be treated as the first complete runnable sparrow, but a small
repo bootstrap step should immediately follow its closure.

Purpose:

1. turn the first runnable slice into a credible first public check-in
2. freeze public-facing paths early so git history is not dominated by later renames
3. make the repo operable by readers who did not follow the internal design history

Recommended immediate actions after `Phase 4` closes:

1. freeze the primary semantic path under `core/`
2. add a minimal runnable entry path
3. add minimal repo status and progress files
4. prepare the bootstrap and validation stabilization work for `Phase 5`

Practical rule:

1. `Phase 4` proves the sparrow can run
2. the immediate bootstrap step makes that sparrow visible and usable as the first stable repo shape

## 11. Testrunner Inside The Phase Model

The runner should be attached to phases explicitly.

### Phases 1-2

Use:

1. conformance runner

### Phases 3-4

Use:

1. calibration scenarios
2. selected `V2` scenario reuse

### Phases 6-8

Use:

1. broader SW system scenarios
2. chaos / soak / workload validation as appropriate

This keeps the runner aligned with phase goals instead of becoming a giant undifferentiated pass/fail bucket.

## 12. Progress Exposure In The Future V3 Repo

Each phase should be visible in the standalone repo through:

1. `docs/progress/current-status.md`
2. `docs/progress/phase-xx.md`
3. `docs/progress/phase-xx-log.md`
4. `docs/progress/phase-xx-decisions.md`
5. `docs/conformance/status.md`

For each phase, report:

1. target
2. current status
3. evidence level reached
4. open risks
5. next gate

This progress surface is still necessary even when a broader production roadmap
also exists.

## 13. Naming And Repo Structure Rule

For the future standalone repo, public-facing directory names should describe
system role, not internal generation history.

Recommended top-level implementation names:

1. `core/`
2. `runtime/`
3. `adapter/`
4. `conformance/`
5. `schema/`

Reason:

1. the standalone repo is the first public implementation, not a public "`V3` repo"
2. outside readers should see stable subsystem names rather than internal rewrite history
3. the semantic core is expected to grow into a fuller system rather than be replaced by another generation directory later

Current transition rule:

1. after the runnable-sparrow closeout, the preferred public semantic-center path is `core/`
2. any remaining `v3mini/` references should be treated as historical or transitional, not as the preferred long-term public package name
3. phase plans should refer to the semantic center as `core` when describing the intended standalone repo structure
4. documents may still mention "`v3mini`" when referring to the already-built Phase 01-03 code or historical implementation path

Practical reading rule:

1. `core` = the public-facing semantic center name
2. `v3mini` = the current internal implementation name for that same semantic center until the repo is renamed

## 14. Practical Rule

If a phase cannot be summarized in one sentence of the form:

**"This phase proves X, and it is closed when Y evidence exists."**

then the phase is probably too vague.

## 15. Production Roadmap Layer

The phase model above is the execution model.

It should not be the only planning view.

`V3` also needs a top-down product roadmap so the team can see how narrow phase
closures accumulate into a production-capable block system.

The correct planning shape is:

1. one broad production roadmap describing the major capability gates
2. one narrow execution-phase ladder describing the next concrete closure step
3. a clear mapping from the current phase to the broader roadmap position

### 15.1 Production Target

The production target is not:

1. a runnable demo
2. a narrow persistence slice
3. a calibration-only repo

The production target is:

1. a semantically correct block system
2. with real persistence and restart safety
3. with credible crash-safety boundaries
4. with operator-usable surfaces
5. with replicated recovery and fencing behavior that are production-reviewable
6. with observability, validation, and release-hardening strong enough for real deployment

### 15.2 Top-Down Roadmap

The broad roadmap should be read as a capability stack:

1. semantic core correctness
2. runnable adapter-backed block slice
3. calibration against selected `V2` reality
4. persistent single-node slice
5. local data process around the accepted persistence seam
6. data-sync process for catch-up and rebuild movement
7. recovery execution process around engine-issued commands
8. operator-usable single-node product surface
9. replicated durable slice with honest failover/fencing behavior
10. production hardening, qualification, and release gate
11. scale and topology expansion
12. operations and ecosystem expansion

Each layer should inherit the truth already closed below it.

No layer should be claimed early just because a lower layer demo exists.

### 15.3 Current Position

The current execution package should be read as:

1. Phase 04: runnable narrow slice
2. Phase 05: bootstrap and inspection stabilization
3. Phase 06: first-pass calibration
4. Phase 07: first persistence-backed single-node slice

This means the project is currently moving from:

1. "runnable and calibrated"

to:

2. "persisted on one node"

It is NOT yet at:

1. full crash-safety qualification
2. operator-ready product surface
3. replicated production rollout
4. release hardening / GA gate
5. scale/topology expansion
6. operations/ecosystem completion

Practical reading rule:

1. the current delivered closure is recovery semantics and its lower execution boundaries
2. topology authority, failover selection/promotion policy, and broader cluster governance are explicitly assigned to later phases rather than implied as already done

### 15.5 Responsibility Table

For scope checking, the later mainline should be read in this short form:

| Phase | One-line responsibility |
|---|---|
| `P9` | move bytes correctly under lineage |
| `P10` | execute recovery lifecycle correctly under start/cancel/complete/timeout |
| `P11` | ship bounded single-node product surface with no cluster-shape leak |
| `P12` | close bounded replicated handoff/rejoin mechanism as mechanism, not topology policy |
| `P13` | harden the bounded replicated slice on persistent storage for release criteria |
| `P14` | close topology authority and failover policy for who/when/where |
| `P15` | expose accepted governance through operator and ecosystem surfaces without creating new authority |

Practical rule:

1. if one task or PR cannot be assigned cleanly to one row, either the task is
   over-scoped or the phase boundary is still unclear

Delivery-weight note:

1. `P13-P15` should be read as a substantial finalization program, not light
   finishing work
2. the expected cost center shifts from semantic-engine invention to
   control-plane, frontend, and ecosystem integration
3. the mainline is expected to rely heavily on selected `V2` product-muscle
   porting rather than broad greenfield rewrite
4. use `v3-p13-p15-work-estimate.md` as the planning baseline for workload,
   sequencing, and expected new-code versus port mix

### 15.4 Planning Rule

Future phase definitions should always answer two questions:

1. what narrow loop does this phase close
2. which production-roadmap layer does that closure advance

If a phase closes a local loop but does not clearly move one roadmap layer
forward, the phase is probably too internal or too detached from product
direction.

## 16. Execution Institution Model

`V3` should not invent lower execution institutions independently from the
semantic model.

Instead, they should be derived from:

1. the model truth domains
2. the accepted event types
3. the accepted command boundaries
4. the visibility contract
5. the crash/recover contract

This means execution-layer interfaces are not just engineering convenience.

They are the controlled lower institutions that receive delegated authority
from the already-accepted `V3` semantic route.

### 16.1 Core Rule

Execution institutions may have:

1. internal data structures
2. asynchronous scheduling
3. batching, retry, and queueing
4. local progress tracking
5. narrow local decisions required to execute accepted commands

Execution institutions may NOT have:

1. authority to redefine semantic meaning
2. authority to reinterpret accepted commands
3. authority to publish terminal truth on their own
4. authority to widen or narrow semantic targets such as `targetLSN`
5. authority to turn local progress into system truth without returning through the accepted route

The short rule is:

**inside freedom is allowed; outside contract is fixed.**

### 16.2 Knowledge And Authority Split

The semantic route should continue to own:

1. recovery classification
2. command intent
3. target meaning
4. terminal truth
5. external visibility rules

Execution institutions may own only:

1. local execution state
2. local persistence state
3. transport/data-transfer state
4. progress and error detail
5. local recovery mechanics that do not redefine semantic meaning

### 16.3 Typical Execution Institutions

Examples of correct lower institutions:

1. `LogicalStorage`
2. `DataCommunicator`
3. `RecoveryExecutor`
4. `ProgressFeed` or equivalent trace/progress surface

Expected role boundaries:

1. `LogicalStorage` owns local persistence execution, local reopen, and local data recovery details
2. `DataCommunicator` owns data transfer, barrier execution, progress reporting, and transport-local retry behavior
3. `RecoveryExecutor` owns orchestration of already-decided commands by calling storage and communication institutions
4. trace/progress surfaces expose evidence but do not become semantic control input

### 16.3A One Possible Further Split

The list above is not the only acceptable cut.

One possible future split, if the system needs stronger named institutions, is:

1. `IdentityManager`
2. `DataManager`
3. `CommunicationManager`
4. `LBAMap` or `RecoveryIndex`
5. `RecoverProcess`

This is a possible partition, not a claim that all five already exist as
cleanly separated interfaces today.

Suggested responsibilities:

1. `IdentityManager` owns self identity, peer identity, assignment, epoch, endpoint version, and topology context
2. `DataManager` owns local read, local write, `Write -> lsn`, sync, flush, checkpoint, recover, and local durability boundaries
3. `CommunicationManager` owns replica connection state, ship/receive, barrier, backpressure, and remote progress
4. `LBAMap` or `RecoveryIndex` owns recovery-oriented mapping knowledge such as which `lsn` touched which `lba`
5. `RecoverProcess` owns orchestration of engine-issued recovery intent across the other institutions

Suggested prohibitions:

1. `IdentityManager` should not own bytes truth or semantic recovery class
2. `DataManager` should not own topology, connection policy, or shipping policy
3. `CommunicationManager` should not decide recovery class or terminal semantic success
4. `LBAMap` should not own identity, connection lifecycle, or semantic authority
5. `RecoverProcess` should not invent recovery meaning or rewrite engine-issued targets

One useful consequence of this split is:

1. read APIs can go directly through `DataManager`
2. writes can return `lsn` directly from `DataManager`
3. upper layers can maintain `LBAMap` knowledge for smarter recovery without pushing that planning logic down into local storage
4. `CommunicationManager` can consume `lsn`-ordered change facts explicitly rather than being hidden behind local write side effects

### 16.3B Manager Split Rule

The institutions above do not need to become perfectly synchronized at every
instant.

Local execution lag is acceptable.

Semantic lag is not.

Allowed:

1. `IdentityManager` may accept a newer assignment, epoch, or topology fact before older data-movement goroutines are physically gone
2. `CommunicationManager` may still hold an old connection, sender, or queue while cleanup drains
3. `DataManager` may still retain local state associated with an older replica path while invalidation is being processed

Not allowed:

1. an old shipper or old connection continuing to advance current semantic truth after `IdentityManager` has already made it stale
2. stale progress, ack, barrier, or "caught up" results being accepted into the live semantic route
3. cleanup timing differences turning into authority differences

The intended rule is:

1. execution lag is allowed
2. authority lag is not

Practical consequence:

1. once a newer assignment / epoch / endpoint version / session context exists, older execution state must become semantically dead immediately
2. older goroutines may still run for cleanup, but their outputs must be ignored or rejected at the acceptance boundary
3. every manager split should be reviewed by asking whether the split changes only cleanup timing, or whether it also changes who still gets to affect system truth

Short form:

**old process may still run, but it must already be semantically dead.**

### 16.3C Deliver-Lineage Rule

When a `V3` recovery command crosses from semantic truth into execution,
it must no longer be lineage-free.

The accepted deliver-side rule is:

1. the engine chooses recovery class and freezes `targetLSN`
2. the adapter or execution boundary binds that recovery work to a
   fresh `sessionID`
3. the execution route carries at least `sessionID + epoch +
   endpointVersion + targetLSN`
4. stale or superseded lineage must be rejected before it can mutate
   current semantic truth or current replica bytes

This keeps the semantic reducer small without allowing the runtime to
become ambiguous during handoff.

Short form:

**partial semantic command is acceptable; lineage-free execution is not.**

### 16.4 V2 Muscle Migration Rule

`V3` is not a blank-slate rewrite.

The working model is:

1. `V3` defines semantic truth, authority, and boundaries first
2. compatible `V2` execution muscles may then be migrated behind clean `V3` seams
3. migration is filtered by semantic compatibility, not by code reuse convenience alone

This means:

1. semantic modules from `V2` are usually reference-only or rewritten in native `V3` form
2. execution modules from `V2` may often be migrated as whole muscles after old semantic coupling is stripped
3. the phase claim may stay narrow even when the migrated execution muscle is substantial

The purpose is to avoid a renamed `V2` that still quietly inherits older truth
structures.

## 17. Interface Review Checklist

When `sw` proposes a new execution-layer interface or implementation, review it
with these questions first.

### 17.1 Knowledge Boundary

1. what facts does this interface know
2. are those facts local execution facts or semantic truth
3. does it hold any knowledge that should remain in the semantic route instead

### 17.2 Authority Boundary

1. what local decisions is this institution allowed to make
2. does it only execute an accepted command, or does it reinterpret the command
3. can it accidentally choose recovery class, target meaning, or terminal truth
4. does it add the required execution lineage before work leaves the semantic route

### 17.3 Visibility Boundary

1. what does it expose as trace, progress, or status
2. can any exposed local state be mistaken for semantic truth
3. does externally visible success still require the accepted route rather than local completion alone

### 17.4 Crash And Recover Boundary

1. what local state must survive restart
2. what local state may be discarded after crash
3. after restart, which facts must return through the accepted event/command route before they become system truth

### 17.5 Migration Boundary

1. is this a semantic module, adapter/glue module, or execution muscle from `V2`
2. if migrated from `V2`, what old semantic coupling was explicitly removed
3. is the migration saving throwaway work without re-importing old truth ownership

### 17.6 Reject Signals

Reject the interface or implementation if any of these appear:

1. it changes the meaning of an accepted command
2. it silently widens or narrows `targetLSN`
3. it treats local progress as terminal semantic success
4. it converts trace/projection output into control truth
5. it carries hidden `V2` policy that the `V3` route did not explicitly accept
6. it allows stale callback, stale probe result, or stale mutation traffic to pass without lineage rejection

## 18. Summary

Yes, `V3` should use phases.

But the important rule is:

1. each phase has one semantic target
2. each phase has explicit non-goals
3. each phase has a clear proof gate
4. each phase is not closed until `sw` + `tester` + review all complete the loop
5. those narrow phase closures should be explicitly mapped to a broader production roadmap
6. lower execution institutions should be derived from `V3` semantic boundaries and reviewed for knowledge/authority correctness
7. compatible `V2` execution muscles may be migrated, but only after semantic filtering and boundary cleanup

That is the safest way to grow `V3` without turning it into another open-ended architecture drift.

## 19. Canonical P14B To Production Plan

This section is the current canonical reading for the remaining mainline work
from late `P14` to production.

It exists because the earlier coarse reading of `P14` and `P15` was too small
on internal control-plane closure and too optimistic about how much of the
production gap could be postponed to operator-facing phases.

Short form:

1. `P14B` must close the internal control-plane truth loop
2. `P14A` must verify each new mixed route created by that closure
3. `P15` must expose and productize the already-closed internal loop
4. multi-master HA is not silently included in this bounded mainline plan

### 19.1 Stable Anchors That Must Not Drift

The following documents remain the stable review anchors while `P14B` and `P15`
move quickly:

1. `v3-protocol-truths.md`
2. `v3-protocol-claim-and-evidence.md`
3. `v3-semantic-constraint-checklist.md`
4. the active phase package and `14A` sidecar

Planning may move faster than before.
These anchors must not.

Practical rule:

1. do not widen engine truth just because control-plane work grows
2. do not import old `V2` policy ownership with reused code
3. do not let heartbeat, transport mood, or projection convenience become semantic authority
4. do not claim broader product closure than the currently proved route

### 19.2 Global Target

The remaining mainline target is not just "more topology" or "more operator
surface".

The target is:

1. one bounded single-active-master deployment shape
2. one bounded topology/control-plane truth loop
3. one bounded host-consumable block product loop

The control-plane truth loop that must become real is:

`heartbeat / observation`
-> `inventory merge and freshness`
-> `stable ClusterSnapshot synthesis`
-> `policy/controller decision`
-> `assignment publication`
-> `adapter / engine convergence`
-> `observed confirmation`
-> `restart recovery of current truth`

If this loop is not closed, `P15` surfaces have nothing stable to expose.

### 19.3 Phase Split From Here

The remaining mainline should be read as:

1. `P14B`: internal control-plane closure
2. `P14A`: verification sidecar over new `P14B` mixed routes
3. `P15`: external control surface, operator/product surface, and frontend/productization

That means:

1. `P14B` is still part of `P14`, not a separate product phase
2. `P15` must not absorb missing internal truth closure
3. `14A` remains proof and regression pressure, not institution ownership

### 19.4 P14B Overall Responsibility

`P14B` is closed only when the bounded accepted topology set has:

1. real observation ingestion
2. real snapshot synthesis
3. real durable authority source
4. real convergence and confirmation rules
5. real restart-safe current-truth recovery
6. proof through the real adapter/engine route, not only publisher-local or recording-consumer proof

Bounded deployment assumption for this plan:

1. one active master / control-plane owner
2. multiple volumes
3. per volume, three bounded replica slots on distinct servers
4. one current primary and two bounded candidates
5. no multi-master leader election or distributed authority ownership in this plan

### 19.5 P14B-1 Observation Institution

Target:

1. turn heartbeat and inventory into a stable `ClusterSnapshot` producer rather than a test input

Must close:

1. heartbeat ingestion
2. freshness / expiry rules
3. partial and conflicting observation handling
4. bounded unsupported evidence for incomplete or inconsistent topology input
5. stable `ClusterSnapshot` synthesis for the accepted topology set

Engine work:

1. none by default
2. engine remains a consumer of already-published identity truth
3. no new engine truth or projection field unless a later proof shows a real missing read-only output

Primary new-code areas:

1. `core/authority/` for normalized cluster snapshot contract and observation institution boundary
2. `weed/server/` for heartbeat collector and raw master-side wiring
3. `weed/storage/blockvol/v2bridge/` only for bounded source-format adaptation when needed

Port now from `V2` (mechanism only):

1. `weed/server/block_heartbeat_loop.go`
2. `weed/storage/blockvol/block_heartbeat.go`
3. `weed/storage/blockvol/block_heartbeat_proto.go`
4. selected source-format adaptation ideas from `weed/storage/blockvol/v2bridge/control.go`

Do not port directly:

1. any heartbeat-to-policy shortcut that directly decides recovery/failover from timing
2. any volume-local authority mutation path
3. any old projection/status reuse as control truth

Required proof:

1. incomplete inventory becomes unsupported evidence, not failover input
2. stale observation cannot mint fresh authority
3. one bad or unsupported volume does not block unrelated healthy volume progress

### 19.6 P14B-2 Durable Authority Institution

Target:

1. make the current authority line durable and restart-recoverable under the bounded single-owner deployment

Must close:

1. durable source for current per-volume authority line
2. restart recovery for controller / publisher current truth
3. explicit bounded single-owner rule
4. epoch / endpointVersion continuity after restart
5. bounded recovery when observed state lags durable authority

Engine work:

1. none by default
2. no semantic ownership moves into engine
3. only read-only evidence additions are acceptable, and only if later proof forces them

Primary new-code areas:

1. `core/authority/` for durable registry / current-line persistence / replay into controller state
2. `weed/server/` for master-side hosting and lifecycle
3. possibly `weed/storage/blockvol/` or adjacent persistence helpers only as storage muscle, not authority owner

Port now from `V2` (mechanism only):

1. `weed/server/master_block_registry.go`
2. `weed/server/master_block_assignment_queue.go`
3. lifecycle/wiring patterns from `weed/server/volume_server_block.go`

Do not port directly:

1. old master or volume code that mutates assignment truth from local convenience state
2. promote/demote ownership hidden inside volume-local paths
3. any old registry state treated as semantic authority without explicit filtering

Required proof:

1. restart does not lose current authority line
2. stale pre-restart state cannot revive newer authority
3. bounded single-owner truth remains intact without multi-master assumptions

### 19.7 P14B-3 Convergence Institution

Target:

1. make `publish-until-observed` a real bounded convergence loop instead of a local planning idea

Must close:

1. desired-state pending / observed / superseded rules
2. retry / suppression / backoff / dedupe rules
3. explicit confirmation source for assignment and endpoint moves
4. stale observation handling that does not thrash authority
5. bounded authority transition honesty while new identity is converging

Engine work:

1. preserve the existing ack-gated fence and publication contract
2. do not add a generic controller loop to adapter/runtime
3. only tighten bounded-fate handling if a real mixed-route hole appears under `14A` review

Primary new-code areas:

1. `core/authority/` for convergence state, confirmation rules, and bounded desired-state lifecycle
2. `weed/server/` for observation inputs and hosting
3. `core/adapter/` only if a new bounded-fate or stale-rejection proof forces a narrow integration fix

Port now from `V2` (mechanism only):

1. selected planning/failover plumbing from `weed/server/master_block_plan.go`
2. selected selection/evidence mechanics from `weed/server/master_block_failover.go`
3. selected evidence patterns from `weed/server/master_block_evidence.go`

Do not port directly:

1. old failover trigger meaning as-is
2. old policy ownership hidden in master convenience logic
3. any route where transport/heartbeat timing directly becomes failover truth

Required proof:

1. published desired state remains until observed or superseded
2. stale observation cannot churn the current line
3. authority transition does not overclaim healthy publication
4. failover and rebalance remain bounded and diagnosable under the accepted topology set

### 19.8 P14B-4 Full P14 Close

Target:

1. close one bounded topology/governance product shape on the full accepted topology set

Must close:

1. multi-volume topology authority under the accepted three-slot pattern
2. failover and rebalance through real adapter/engine convergence
3. restart/catch-up/fence/publication behavior across the full bounded route
4. one final bounded supported-topology statement
5. one explicit unsupported list

Engine work:

1. no new policy ownership
2. no broad semantic rewrite
3. only bounded fixes that `14A` proves are necessary on the mixed routes

Primary proof expectation:

1. controller-driven route must reach real `VolumeReplicaAdapter`, not only recording consumers
2. controller-driven failover/rebalance must remain honest at the publication surface during transition
3. crash/restart mixed-route proofs must exist for the bounded deployment

### 19.9 P14A Role During P14B

`14A` remains the verification sidecar.
It should reopen only when a new `P14B` workstream creates real mixed-route pressure.

Expected `14A` reopen themes by workstream:

1. after Observation Institution:
   - stale / delayed heartbeat
   - partial inventory
   - conflicting observation
   - unsupported evidence honesty
2. after Durable Authority Institution:
   - stale authority after restart
   - old truth revival
   - bounded single-owner correctness
3. after Convergence Institution:
   - publish-but-not-observed loops
   - timeout / suppress / dedupe correctness
   - transition publication honesty
   - new silent liveness holes
4. before Full P14 Close:
   - mixed-route sequence pass across observation, authority, convergence, demotion, and failover overlap

`14A` must not:

1. invent new product institutions
2. widen engine truth just because testing is hard
3. claim global engine stability

### 19.10 P15 Responsibility After P14B

`P15` starts only after the internal control-plane loop is closed enough to expose.

`P15` owns:

1. external control APIs
2. operator-facing diagnostics and explanation surfaces
3. runbook and lifecycle productization
4. frontend/export protocols such as `iSCSI`, `NVMe/TCP`, and `CSI`
5. operator packaging and ecosystem integration

`P15` does NOT own:

1. heartbeat truth formation
2. durable current authority truth
3. convergence semantics
4. the internal policy/controller truth loop itself

### 19.11 V2 Port Matrix For The Mainline Plan

#### Port now into `P14B`

These are the highest-value muscles for the reopened `P14` mainline:

1. `weed/server/block_heartbeat_loop.go`
2. `weed/storage/blockvol/block_heartbeat.go`
3. `weed/storage/blockvol/block_heartbeat_proto.go`
4. `weed/server/master_block_registry.go`
5. `weed/server/master_block_assignment_queue.go`
6. selected mechanism-shaped parts of `weed/server/master_block_plan.go`
7. selected mechanism-shaped parts of `weed/server/master_block_failover.go`
8. selected evidence patterns from `weed/server/master_block_evidence.go`
9. bounded master/volume hosting patterns from `weed/server/volume_server_block.go`

#### Port later into `P15`

1. `weed/storage/blockvol/iscsi/`
2. `weed/storage/blockvol/nvme/`
3. `weed/storage/blockvol/csi/`
4. `weed/storage/blockvol/operator/`
5. `weed/storage/blockvol/monitoring/`
6. larger `testrunner` and scenario-product muscles

#### Reference only or do-not-port directly

1. `weed/storage/blockvol/promotion.go`
2. old `HandleAssignment` / `promote` / `demote` ownership paths
3. old engine/orchestrator/registry semantic owners as current truth owners
4. any `V2` route that turns heartbeat timing, local status, or transport convenience directly into authority
5. any code that would make volume-local runtime mutate assignment truth on its own

### 19.12 Practical Implementation Order

Use this order unless new evidence forces a re-cut:

1. `P14B-1` Observation Institution
2. `14A` targeted reopen on observation routes
3. `P14B-2` Durable Authority Institution
4. `14A` targeted reopen on restart and durable-truth routes
5. `P14B-3` Convergence Institution
6. `14A` targeted reopen on convergence and publication-honesty routes
7. `P14B-4` Full P14 close package
8. `P15` external/operator/frontend productization

This is intentionally a big-step plan.
It is not a license to blur the engine.

The speed rule is:

1. move fast by porting `V2` muscles aggressively
2. keep engine truth, authority ownership, and semantic constraints stable
3. prefer one big bounded institution per step over many plumbing-only micro-steps

### 19.13 One-Sentence Summary

From here to production, the mainline should be read as:

**`P14B` closes the internal control-plane truth loop using semantic-filtered `V2` muscles, `14A` verifies the new mixed routes, and `P15` exposes the already-closed loop through operator and ecosystem surfaces.**
