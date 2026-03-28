# V2 Open Questions

Date: 2026-03-27

## Purpose

This document records what is still algorithmically open in V2.

These are not bugs.

They are design questions that should be closed deliberately before or during implementation slicing.

## 1. Recovery Session Ownership

Open question:

- what is the exact ownership model for one active recovery session per replica?

Need to decide:

- session identity fields
- supersede vs reject vs join behavior
- how epoch/session invalidates old recovery work

Why it matters:

- V1.5 needed local reconnect serialization
- V2 should make this a protocol rule

## 2. Promotion Threshold Strictness

Open question:

- must a promotion candidate always have `FlushedLSN >= CommittedLSN`, or is there any narrower safe exception?

Current prototype:

- uses committed-prefix sufficiency as the safety gate

Why it matters:

- determines how strict real failover behavior should be

## 3. Recovery Reservation Shape

Open question:

- what exactly is reserved during catch-up?

Need to decide:

- WAL range only?
- payload pins?
- snapshot pin?
- expiry semantics?

Why it matters:

- recoverability must be explicit, not hopeful

## 4. Smart WAL Payload Classes

Open question:

- which payload classes are allowed in V2 first?

Current model has:

- `WALInline`
- `ExtentReferenced`

Need to decide:

- whether first real implementation includes both
- whether `ExtentReferenced` requires pinned snapshot/versioned extent only

## 5. Smart WAL Garbage Collection Boundary

Open question:

- when can a referenced payload stop being recoverable?

Need to decide:

- GC interaction
- timeout interaction
- recovery session pinning

Why it matters:

- this is the line between catch-up and rebuild

## 6. Exact Orchestrator Scope

Open question:

- how much of the final V2 control logic belongs in:
  - local node state
  - coordinator
  - transport/session manager

Why it matters:

- avoid V1-style scattered state ownership

## 7. First Real Implementation Slice

Open question:

- what is the first production slice of V2?

Candidates:

1. per-replica sender/session ownership
2. explicit recovery-session management
3. catch-up/rebuild decision plumbing

Recommended default:

- per-replica sender/session ownership

## 8. Steady-State Overhead Budget

Open question:

- what overhead is acceptable in the normal healthy case?

Need to decide:

- metadata checks on hot path
- extra state bookkeeping
- what stays off the hot path

Why it matters:

- V2 should be structurally better without becoming needlessly heavy

## 9. Smart WAL First-Phase Goal

Open question:

- is the first Smart WAL goal:
  - lower recovery cost
  - lower steady-state WAL volume
  - or just proof of historical correctness model?

Recommended answer:

- first prove correctness model, then optimize

## 10. End Condition For Simulator Work

Open question:

- when do we stop adding simulator depth and start implementation?

Suggested answer:

- once acceptance criteria are satisfied
- and the first implementation slice is clear
- and remaining simulator additions are no longer changing core protocol decisions
