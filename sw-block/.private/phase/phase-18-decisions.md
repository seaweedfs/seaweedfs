# Phase 18 Decisions

Date: 2026-04-05
Status: active

## D1: Phase 18 Uses M1-M5 As The Main Spine

Decision:

1. `Phase 18` will be the main control phase for the next kernel/runtime climb
2. the five major milestones (`M1-M5`) are the primary structure inside it
3. each major milestone should normally close in `2-3` implementation steps

Why:

1. we are no longer in pure exploration mode
2. the kernel boundary is now stable enough to support larger development slices
3. milestone-level review is more efficient than micro-slice review

Implication:

1. later work should be grouped into larger reviewable packages
2. helper-level or naming-level pauses should be minimized unless they affect
   architecture

## D2: Preserve Current In-Process Runtime As The Reference Slice

Decision:

1. the current in-process RF2 failover runtime remains the reference slice while
   `M1` introduces the transport/session seam

Why:

1. it already proves the current authority split in executable form
2. it gives a stable baseline for transport-backed migration
3. it reduces the risk of confusing transport mechanics with ownership

Implication:

1. `M1` should introduce adapter seams first
2. the existing in-process path should remain valid until the transport-backed
   slice closes

## D3: Make Adapter-Backed Targets The Primary Failover Contract

Decision:

1. the primary failover contract is now `FailoverTarget`
2. `FailoverTarget` is split into:
   - `FailoverEvidenceAdapter`
   - `FailoverTakeoverAdapter`
3. the old all-in-one `FailoverParticipant` remains only as a compatibility
   wrapper

Why:

1. failover-time query traffic and takeover execution are different boundary
   types
2. the transport seam should be explicit before any real remote adapter is added
3. the runtime/driver/session should depend on adapters, not on concrete
   `*Node` coupling

Implication:

1. future remote work should implement adapter contracts rather than widening
   direct node ownership
2. current in-process tests and runtime remain valid through the in-process
   adapter implementation

## D4: `M1` Closes On Failover-Time Evidence Transport, Not Remote Takeover

Decision:

1. `M1` is considered complete when `PromotionQuery` and `ReplicaSummary`
   traffic cross an explicit transport/session adapter seam
2. `M1` does not require remote takeover execution
3. takeover remains primary-local in this milestone

Why:

1. the `M1` goal is to remove direct failover-time evidence coupling from the
   orchestration path
2. the selected primary should remain the owner of reconstruction and activation
   gating
3. forcing remote takeover too early would risk mixing transport mechanics with
   ownership changes

Implication:

1. the first transport-backed slice is:
   - transport/session-backed evidence
   - primary-local takeover
2. later transport work may widen execution transport, but only without changing
   the authority split

## D5: `M2` Closes On A Bounded Summary-Driven Active Loop 2 Runtime

Decision:

1. `M2` is considered complete when Loop 2 becomes runtime-owned outside
   failover-only logic through a bounded active observation/controller slice
2. `M2` does not require full shipper task execution or rebuild choreography

Why:

1. the main gap after `M1` is not more transport syntax; it is that Loop 2
   should exist as an active runtime owner
2. bounded replica summaries already carry enough information to derive a first
   runtime-owned `keepup` / `catching_up` / `needs_rebuild` slice
3. this allows the runtime to become continuously meaningful without pretending
   the full replication executor is already migrated

Implication:

1. the first active Loop 2 runtime is summary-driven
2. later work can deepen it into real continuous keepup/catchup/rebuild
   choreography without changing the ownership rule

## D6: `M3` Closes On One Bounded Continuity Statement, Not Broad RF2 Proof

Decision:

1. `M3` is considered complete when one runtime-owned continuity path exists:
   write -> active Loop 2 observation -> failover -> readback verification
2. `M3` requires both:
   - a healthy path
   - a gated fail-closed path
3. `M3` does not imply broad RF2 product continuity proof

Why:

1. after `M1` and `M2`, the next meaningful closure is to compose failover and
   active Loop 2 into one bounded continuity statement
2. this proves the runtime is not only structurally correct, but already able to
   carry one real end-to-end continuity story
3. keeping the claim bounded avoids overreading the current in-process runtime as
   a complete RF2 product path

Implication:

1. later work can attach RF2-facing product/runtime surfaces on top of a real
   continuity-bearing runtime slice
2. `M4` should attach one bounded surface without widening the continuity claim
