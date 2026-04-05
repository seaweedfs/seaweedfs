# V2 RF2 Runtime Bounded Envelope

Date: 2026-04-05
Status: draft
Purpose: freeze the bounded productionization envelope around the current
`Phase 19` working RF2 block path without overclaiming broad product readiness

## Reading Rule

This document defines the strongest bounded envelope currently justified by the
delivered `Phase 19` path.

It does NOT mean:

1. broad launch approval
2. working block product approval
3. support for broad frontend or transport matrices
4. that remaining runtime/product gaps are minor polish

It means only:

1. the current `masterv2 + volumev2 + purev2` RF2 runtime slice has a named,
   reviewable productionization boundary
2. the current support statement, exclusions, and blockers are explicit
3. later pilot or rollout work must stay inside this envelope or explicitly widen
   it with new evidence

## Envelope Basis

This envelope is anchored on the delivered `Phase 19` milestones:

1. `M6`: one live loopback HTTP transport now exists behind the evidence seam
2. `M7`: one background Loop 2 service and one bounded auto-failover service now
   exist
3. `M8`: one runtime-managed iSCSI export path and one bounded replica repair
   wrapper now exist
4. `M9`: one end-to-end RF2 handoff proof now exists with continued I/O on the
   new primary
5. `M10`: one bounded operator surface and one bounded CSI runtime backend
   adapter now exist

The envelope is therefore about one bounded working RF2 block path, not broad
product readiness.

## Supported Envelope

The current bounded support statement is:

1. one bounded working RF2 block path now exists with:
   - `masterv2` identity/promotion authority
   - `volumev2` failover, takeover, active Loop 2 service, continuity, repair,
     frontend rebinding, and projected RF2 surface ownership
   - `purev2` execution adapter reuse
2. one bounded live transport path now carries failover-time evidence and replica
   summaries
3. one bounded real client handoff path now exists:
   - write through runtime-managed iSCSI export
   - bounded repair/catch-up on the runtime path
   - lose primary
   - auto fail over
   - reconnect to the new primary
   - continue I/O
4. one bounded outward RF2 surface exists as projection only:
   - `RF2VolumeSurface`
5. one bounded operator/CSI adapter layer exists on top of runtime-owned truth

## Explicit Exclusions

The following are OUTSIDE this bounded envelope:

1. broad multi-process or multi-host deployment approval
2. broad transport/frontend matrix approval
3. full rebuild orchestration beyond the current bounded repair/catch-up wrapper
4. broad CSI lifecycle parity beyond the current bounded runtime backend adapter
5. broad operator/API/metrics coverage beyond the current bounded HTTP surface
6. broad launch or external customer support statements

## Current Blockers

The main blockers between this envelope and a working RF2 block product are:

1. the current path is still bounded to the current runtime harness rather than
   broad multi-process approval
2. bounded repair/catch-up is not yet broad rebuild lifecycle closure
3. CSI rebinding is still a bounded runtime backend adapter, not full lifecycle
   parity
4. the operator surface is still a bounded HTTP view, not a full operational
   platform surface

## Allowed Validation Shape

The allowed validation shape inside this envelope is:

1. internal engineering validation only
2. bounded lab/runtime exercise only
3. explicit artifact-driven interpretation only

The following are NOT allowed interpretations:

1. "the system is now production ready"
2. "the system now supports real automatic failover"
3. "the system now supports broad product traffic and rollout"

## Evidence Anchors

Read this envelope together with:

1. `sw-block/.private/phase/phase-19.md`
2. `sw-block/design/v2-kernel-closure-review.md`
3. `sw-block/design/v2-protocol-claim-and-evidence.md`
4. `sw-block/runtime/volumev2/runtime_manager.go`
5. `sw-block/runtime/volumev2/continuity_runtime.go`
6. `sw-block/runtime/volumev2/rf2_surface.go`
7. `sw-block/runtime/volumev2/loop2_service.go`
8. `sw-block/runtime/volumev2/frontend_runtime.go`
9. `sw-block/runtime/volumev2/operator_surface.go`
10. `sw-block/runtime/volumev2/poc_test.go`
11. `weed/storage/blockvol/csi/v2_runtime_backend.go`

## Envelope Output

The correct current reading of this envelope is:

1. runtime-bearing RF2 kernel slice: yes
2. bounded working RF2 block path: yes
3. bounded productionization artifact set: yes
4. pilot-ready broad product path: no
