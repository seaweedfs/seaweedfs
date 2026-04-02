# Phase 4.5 Reason

Date: 2026-03-27
Status: proposal for dev manager decision
Purpose: explain why a narrow V2 fine-tuning step should follow the main Phase 04 slice, without reopening the core ownership/fencing direction

## 1. Why This Note Exists

`Phase 04` has already produced strong progress on the first standalone V2 slice:

- per-replica sender identity
- one active recovery session per replica per epoch
- endpoint / epoch invalidation
- sender-owned execution APIs
- explicit recovery outcome branching
- minimal historical-data prototype

This is good progress and should continue.

However, recent review and discussion show that the next risk is no longer:

- ownership ambiguity
- stale completion acceptance
- scattered local recovery authority

The next risk is different:

- `CatchUp` may become too broad, too long-lived, and too resource-heavy
- simulator proof is still weaker than desired on crash-consistency and recoverability boundaries
- the project may accidentally carry V1.5-style "keep trying to catch up" assumptions into V2 engine work

So this note proposes:

- **do not interrupt the main Phase 04 work**
- **do not reopen core V2 ownership/fencing architecture**
- **add a narrow fine-tuning step immediately after Phase 04 main closure**

This note is for the dev manager to decide implementation sequencing.

## 2. Current Basis

This proposal is grounded in the following current documents:

- `sw-block/.private/phase/phase-04.md`
- `sw-block/docs/archive/design/v2-prototype-roadmap-and-gates.md`
- `sw-block/design/v2-acceptance-criteria.md`
- `sw-block/design/v2-detailed-algorithm.zh.md`

In particular:

- `phase-04.md` shows that Phase 04 is correctly centered on sender/session ownership and recovery execution authority
- `docs/archive/design/v2-prototype-roadmap-and-gates.md` shows that design proof is high, but data/recovery proof and prototype end-to-end proof are still low
- `v2-acceptance-criteria.md` already requires stronger proof for:
  - `A5` non-convergent catch-up escalation
  - `A6` explicit recoverability boundary
  - `A7` historical correctness
  - `A8` durability-mode correctness
- `v2-detailed-algorithm.zh.md` Section 17 now argues for a direction tightening:
  - keep the V2 core
  - narrow `CatchUp`
  - elevate `Rebuild`
  - defer higher-complexity expansion

## 3. Main Judgment

### 3.1 What should NOT change

The following V2 core should remain stable:

- `CommittedLSN` as the external safe boundary
- durable progress as sync truth
- one sender per replica
- one active recovery session per replica per epoch
- stale epoch / stale endpoint / stale session fencing
- explicit `ZeroGap / CatchUp / NeedsRebuild`

This is the architecture that most clearly separates V2 from V1.5.

### 3.2 What SHOULD be fine-tuned

The following should be tightened before engine planning:

1. `CatchUp` should be narrowed to a short-gap, bounded, budgeted path
2. `Rebuild` should be treated as a formal primary recovery path, not only a fallback embarrassment
3. `recover -> keepup` handoff should be made more explicit
4. simulator should prove recoverability and crash-consistency more directly

## 4. Algorithm Thinking Behind The Fine-Tune

This section summarizes the reasoning already captured in:

- `sw-block/design/v2-detailed-algorithm.zh.md`

Especially Section 17:

- `V2` is still the right direction
- but V2 should be tightened from:
  - "make WAL recovery increasingly smart"
  - to:
  - "make block truth boundaries hard, keep `CatchUp` cheap and bounded, and use formal `Rebuild` when recovery becomes too complex"

### 4.1 First-principles view

From block first principles, the hardest truths are:

1. when `write` becomes real
2. what `flush/fsync ACK` truly promises
3. whether acknowledged boundaries survive failover
4. how replicas rejoin without corrupting lineage

These are more fundamental than:

- volume product shape
- control-plane surface
- recovery cleverness for its own sake

So the project should optimize for:

- clearer truth boundaries
- not for maximal catch-up cleverness

### 4.2 Mayastor-style product insight

The useful first-principles lesson from Mayastor-like product thinking is:

- not every lagging replica is worth indefinite low-cost chase
- `Rebuild` can be a formal product path, not a shameful fallback
- block products benefit from explicit lifecycle objects and formal rebuild flow

This does NOT replace the V2 core concerns:

- `flush ACK` truth
- committed-prefix failover safety
- stale authority fencing

But it does suggest a correction:

- do not let `CatchUp` become an over-smart general answer to all recovery

### 4.3 Proposed V2 fine-tuned interpretation

The fine-tuned interpretation of V2 should be:

- `CatchUp` is for short-gap, clearly recoverable, bounded recovery
- `Rebuild` is for long-gap, high-cost, unstable, or non-convergent recovery
- recovery session is a bounded contract, not a long-running rescue thread
- `> H0` live WAL must not silently turn one recovery session into an endless chase

## 5. Specific Fine-Tune Adjustments

### 5.1 Narrow `CatchUp`

`CatchUp` should explicitly require:

- short outage
- bounded target `H0`
- clear recoverability
- bounded reservation
- bounded time
- bounded resource cost
- bounded convergence expectation

`CatchUp` should explicitly stop when:

- target drifts too long without convergence
- replay progress stalls
- recoverability proof is lost
- retention cost becomes unreasonable
- session budget expires

### 5.2 Elevate `Rebuild`

`Rebuild` should be treated as a first-class path when:

- lag is too large
- catch-up does not converge
- recoverability is no longer stable
- complexity of continued catch-up exceeds its product value

The intended model becomes:

- short gap -> `CatchUp`
- long gap / unstable / non-convergent -> `Rebuild`

This should be interpreted more strictly than a simple routing rule:

- `CatchUp` is not a general recovery framework
- `CatchUp` is a relaxed form of `KeepUp`
- it should stay limited to short-gap, bounded, clearly recoverable WAL replay
- it only makes sense while the replica's current base is still trustworthy enough to continue from

By contrast:

- `Rebuild` is the more general recovery framework
- it restores the replica from a trusted base toward a frozen target boundary
- `full rebuild` and `partial rebuild` are not different protocols; they are different base/transfer choices under the same rebuild contract

So the intended product shape is:

- use `CatchUp` when replay debt is small and clearly cheaper than rebuild
- use `Rebuild` when correctness, boundedness, or product simplicity would otherwise be compromised

And the correctness anchor for both `full` and `partial` rebuild should remain explicit:

- freeze `TargetLSN`
- pin the snapshot/base used for recovery
- only then optimize transfer volume using `snapshot + tail`, `bitmap`, or similar mechanisms

### 5.3 Clarify `recover -> keepup` handoff

Phase 04 already aims to prove a clean handoff between normal sender and recovery session.

The fine-tune should make the next step more explicit:

- one recovery session only owns `(R, H0]`
- session completion releases recovery debt
- replica should not silently stay in "quasi-recovery"
- re-entry to `KeepUp` / `InSync` should remain explicit, ideally with `PromotionHold` or equivalent stabilization logic

### 5.4 Keep Smart WAL deferred

No fine-tune should broaden Smart WAL scope at this point.

Reason:

- Smart WAL multiplies recoverability, GC, payload-availability, and reservation complexity
- the current priority is to harden the simpler V2 replication contract first

So the rule remains:

- no Smart WAL expansion beyond what minimal proof work might later require

## 6. Simulation Strengthening Requirements

This is the highest-value part of the fine-tune.

Current simulator strength is already good on:

- epoch fencing
- stale traffic rejection
- promotion candidate rules
- ownership / session invalidation
- basic `CatchUp / NeedsRebuild` classification

Current simulator weakness is still significant on:

- crash-consistency around extent / checkpoint / replay boundaries
- `ACK` boundary versus recoverable boundary
- `CatchUp` liveness / convergence

### 6.1 Required new modeling direction

The simulator should stop collapsing these states together:

- received but not durable
- WAL durable but not yet fully materialized
- extent-visible but not yet checkpoint-safe
- checkpoint-safe base image
- restart-recoverable read state

Suggested explicit storage-state split:

- `ReceivedLSN`
- `WALDurableLSN`
- `ExtentAppliedLSN`
- `CheckpointLSN`
- `RecoverableLSNAfterRestart`

### 6.2 Required new invariants

The simulator should explicitly check at least:

1. `AckedFlushLSN <= RecoverableLSNAfterRestart`
2. visible state must have recoverable backing
3. `CatchUp` cannot remain non-convergent indefinitely
4. promotion candidate must still possess recoverable committed prefix

### 6.3 Required new scenario classes

Priority scenarios to add:

1. `ExtentAheadOfCheckpoint_CrashRestart_ReadBoundary`
2. `AckedFlush_MustBeRecoverableAfterCrash`
3. `UnackedVisibleExtent_MustNotSurviveAsCommittedTruth`
4. `CatchUpChasingMovingHead_EscalatesOrConverges`
5. `CheckpointGCBreaksRecoveryProof`

### 6.4 Required simulator style upgrade

The simulator should move beyond only hand-authored examples and also support:

- dangerous-state predicates
- adversarial random exploration guided by those predicates

Examples:

- `acked_flush_lost`
- `extent_exposes_unrecoverable_state`
- `catchup_livelock`
- `rebuild_required_but_not_escalated`

## 7. Relationship To Acceptance Criteria

This fine-tune is not a separate architecture line.

It is mainly intended to make the project satisfy the existing acceptance set more convincingly:

- `A5` explicit escalation from non-convergent catch-up
- `A6` recoverability boundary as a real rule, not hopeful policy
- `A7` historical correctness against snapshot + tail rebuild
- `A8` strict durability mode semantics

So this fine-tune is a strengthening of the current V2 proof path, not a new branch.

## 8. Recommended Sequencing

### Option A: pause Phase 04 and reopen design now

Not recommended.

Why:

- Phase 04 has strong momentum
- its core ownership/fencing work is correct
- pausing it now would blur scope and waste recent closure

### Option B: finish Phase 04, then add a narrow `4.5`

Recommended.

Why:

- Phase 04 can finish its intended ownership / orchestration / minimal-history closure
- `4.5` can then tighten recovery strategy without destabilizing the slice
- the project avoids carrying "too-smart catch-up" assumptions into later engine planning

Recommended sequence:

1. finish Phase 04 main closure
2. immediately start `Phase 4.5`
3. use `4.5` to tighten:
   - bounded `CatchUp`
   - formal `Rebuild`
   - crash-consistency and recoverability simulator proof
4. then re-evaluate Gate 4 / Gate 5

## 9. Scope Of A Possible Phase 4.5

If the dev manager chooses to implement a `4.5` step, its scope should be:

### In scope

- tighten algorithm wording and boundaries from `v2-detailed-algorithm.zh.md`
- formalize bounded `CatchUp`
- formalize `Rebuild` as first-class path
- strengthen simulator state model and invariants
- add targeted crash-consistency and liveness scenarios
- improve prototype traceability against `A5-A8`

### Out of scope

- Smart WAL expansion
- real storage engine redesign
- V1 production integration
- frontend/wire protocol
- performance optimization as primary goal

## 10. Decision Requested From Dev Manager

Please decide:

1. whether `Phase 04` should continue to normal closure without interruption
2. whether a narrow `Phase 4.5` should immediately follow
3. whether the simulator strengthening work should be treated as mandatory for Gate 4 / Gate 5 credibility

Recommended decision:

- **Yes**: finish `Phase 04`
- **Yes**: add `Phase 4.5` as a bounded fine-tuning step
- **Yes**: treat crash-consistency / recoverability / liveness simulator strengthening as required, not optional

## 11. Bottom Line

The project does not need a new direction.

It needs:

- a slightly tighter interpretation of V2
- a stronger recoverability/crash-consistency simulator
- a clearer willingness to use formal `Rebuild` instead of over-extending `CatchUp`

So the practical recommendation is:

- **keep the V2 core**
- **finish Phase 04**
- **add a narrow Phase 4.5**
- **strengthen simulator proof before engine planning**
