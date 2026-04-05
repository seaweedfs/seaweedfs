# V2 Kernel Closure Review

Date: 2026-04-05
Status: active

## Question

The goal is not to prove whether iSCSI itself can be implemented. The reusable
`blockvol` + frontend code already shows that.

The real question is whether the current kernel split can grow into a product:

1. Is the brain owned by V2 semantics?
2. Is the control plane owned by V2 messages and convergence?
3. Is the data plane attached as an execution/backend service instead of a truth
   owner?

## Current Answer

The current `masterv2 + volumev2 + purev2` shape is viable as a product kernel
because ownership is split in the right direction.

### Brain

Owner:

- `sw-block/engine/replication/`

What it owns:

1. semantic state
2. event ingestion
3. command intent
4. outward projection

What it must not own:

1. backend I/O
2. transport lifecycle
3. frontend serving details

### Control Plane

Owner:

- `sw-block/runtime/masterv2/`
- `sw-block/runtime/volumev2/control_session.go`
- `sw-block/runtime/volumev2/orchestrator.go`

What it owns:

1. desired declaration
2. heartbeat observation
3. assignment emission
4. assignment apply loop
5. convergence/idempotence

What it must not own:

1. WAL/extent execution
2. frontend protocol implementation

### Data Plane

Owner:

- `sw-block/runtime/purev2/`
- `sw-block/runtime/volumev2/frontend.go`
- reused `weed/storage/blockvol/*`

What it owns:

1. create/open
2. read/write/flush
3. restart durability
4. frontend export such as iSCSI

What it must not own:

1. role truth
2. publication truth
3. assignment policy

## Closure Proofs

Two small closure proofs are enough for the current stage.

### 1. Control-plane closure

Scenario:

1. `masterv2` declares one RF1 primary
2. `volumev2` heartbeats with no local role yet
3. `masterv2` emits an assignment
4. `volumev2` applies it through the V2 path
5. a later heartbeat converges to quiet state
6. if desired state changes, assignment is reissued once and converges again

Why it matters:

- this proves the new head is not piggybacking on `weed/server` loops

### 2. Data-plane closure

Scenario:

1. `volumev2` exports a named volume through iSCSI
2. a client logs in and issues SCSI write/read
3. data is verified through the frontend and local backend view

Why it matters:

- this proves the kernel can host a real frontend while keeping truth ownership
  outside the frontend/backend code

## Product Meaning

If these two closures stay true while features expand, then the architecture can
scale toward:

1. RF1 productized single-node block service
2. RF2/RF3 replication as additional control/data workflows
3. failover and rebuild without moving semantic truth back into backend code
4. CSI on top of a clearer runtime contract

Another way to state the same result:

1. `masterv2` behaves like an external identity authority
2. each `volumev2` instance behaves like a per-volume micro-cluster shell
3. the selected primary inside that shell owns data-control truth and recovery
   choreography

## Current Milestone

The current milestone is:

1. explicit `masterv2` promotion authorization
2. explicit `volumev2` takeover preparation and activation gating
3. stepwise `FailoverSession` orchestration with observable stages
4. one in-process failover driver seam over registered participants
5. one runtime-owned `volumev2` manager that executes failover through a single
   runtime entry point and retains latest failover snapshot/result

What this milestone proves:

1. failover is no longer only an implicit concept hidden in mixed runtime code
2. authorization, reconstruction, activation, and runtime ownership are now
   separate code seams
3. the new kernel already has a usable in-process RF2 failover runtime slice

What it does not yet prove:

1. real transport-backed replica communication
2. background membership and health management beyond explicit failover calls
3. end-to-end replicated data continuity through a real primary handoff

## Next Major Milestone

The next major milestone should be:

`transport-backed RF2 failover runtime`

This means one level above the current in-process runtime slice:

1. `FailoverSession` no longer talks only to in-memory participants
2. promotion evidence and bounded replica summaries cross a real transport seam
3. runtime-owned failover still preserves stage/error/result observability
4. the runtime keeps the separation:
   - `masterv2` authorizes
   - the selected primary reconstructs and gates activation
   - the transport/runtime only carries requests, responses, and results

### Target Shape

Code should look roughly like:

1. `masterv2`: heartbeat, promotion evidence arbitration, assignment authority
2. `volumev2`: node shell, takeover seams, failover session, runtime/manager
3. `purev2`: execution adapter and local boundary observation
4. transport/session adapters: promotion query and replica-summary exchange
5. `engine/replication`: semantic brain and future active data-control automata

### Exit Criteria

The milestone should be considered complete when all are true:

1. one runtime-level API can execute failover without tests manually stitching
   participants and sessions together
2. one transport-backed runtime-level API can execute failover without
   in-process participant shortcuts
3. runtime-level observability exposes current/last failover stage, selected
   node, assignment, and bounded truth across the transport seam
4. one healthy component-style failover test passes through the runtime entry
   point
5. one gated/degraded component-style failover test stops through the same
   runtime entry point
6. no new recovery-planner responsibility leaks back into `masterv2`

### Non-Goals

This milestone should not try to prove:

1. broad product RF2 readiness
2. full catch-up or rebuild choreography
3. CSI or frontend productionization on top of RF2
4. broad transport matrix approval

It should prove only that the new kernel can carry the same authority model
through a real transport boundary without collapsing the ownership split.

## Main Risk

The main risk is not iSCSI or local I/O. The main risk is semantic leakage:

1. adding more backend-state shortcuts into control decisions
2. letting frontend/backend code redefine publication truth
3. rebuilding `weed/server` ownership inside `volumev2`
4. letting `masterv2` grow from identity authority into a centralized recovery
   planner

As long as those three are resisted, the kernel can keep expanding cleanly.
