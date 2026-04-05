# Phase 19 Decisions

Date: 2026-04-05
Status: complete

## D1: Keep Real Transport Ahead Of Auto Trigger

Decision:

1. `M6` must land before `M7`
2. live transport-backed runtime queries come before continuous Loop 2 service
   and automatic failover trigger

Why:

1. the next main risk is hidden assumptions in live integration
2. auto-trigger is easier to overread if the runtime path is still partially
   synthetic
3. the existing evidence seam is already explicit and is the safest next live
   integration point

Implication:

1. `M7` should build on the real transport path from `M6`
2. if ordering changes later, the reason must be written explicitly

## D2: Keep Frontend And CSI Downstream Of Real Runtime Proof

Decision:

1. frontend, CSI, and operator surface work stay downstream of live transport and
   continuous runtime ownership
2. `M8-M10` must attach to runtime-owned truth rather than recreating control
   ownership in adapters

Why:

1. the current RF2 surface projection pattern is already correct
2. product-facing integrations should reuse projected/runtime-owned truth instead
   of defining parallel truth
3. attaching frontends too early risks hiding runtime gaps behind working local
   adapters

Implication:

1. one real frontend may attach in `M8`
2. CSI and operator surfaces should wait until the working path is already real

## D3: Keep The First Working Path Bounded To RF2

Decision:

1. `Phase 19` is bounded to one working RF2 block path
2. `RF>2` remains outside the phase boundary

Why:

1. the main goal is to turn the proven RF2 kernel slice into one real serving
   path
2. widening replication factor now would mix product expansion with live-path
   closure

Implication:

1. each milestone should keep the claim bounded to RF2
2. any broader productization work belongs to later phases

## D4: `Phase 19` Closes On One Bounded Working Path, Not Broad Launch

Decision:

1. `Phase 19` is considered complete when one real bounded RF2 block path
   exists with:
   - live transport-backed evidence traffic
   - continuous Loop 2 observation
   - bounded auto failover
   - runtime-managed frontend rebinding
   - bounded repair/catch-up wrapper
   - one end-to-end client handoff proof
   - CSI/operator adapters over runtime-owned truth
2. `Phase 19` does not require broad launch approval or broad deployment matrix
   proof

Why:

1. after `Phase 18`, the main objective is to prove a real working path rather
   than continue only with structural/runtime proof
2. the correct next closure is one bounded user-serving path, not broad rollout
   language
3. keeping the claim bounded preserves the same ownership discipline as
   `Phase 18`

Implication:

1. later phases should focus on multi-process and pilot-ready closure rather than
   redefining the kernel/runtime split again
2. `Phase 19` completion should be read as a working bounded path, not a launch
   decision
