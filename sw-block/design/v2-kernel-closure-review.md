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

What it may normalize but must not own semantically:

1. raw `syncAck` / timeout / callback observations may be normalized into
   primary-side sync fact kinds such as:
   - `sync_quorum_acked`
   - `sync_quorum_timed_out`
   - `sync_replay_required`
   - `sync_rebuild_required`
   - `sync_replay_failed`
2. this normalization is only a control-plane envelope for "what fact arrived"
3. it must not become a second decision authority or a second session planner

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

1. live transport-backed failover-time evidence now crosses one real loopback
   HTTP path
2. one continuous Loop 2 service and one bounded auto-failover service now exist
3. one runtime-managed frontend path and one bounded repair/catch-up wrapper now
   exist
4. one end-to-end RF2 handoff proof now exists with continued I/O on the new
   primary
5. one bounded operator surface and one bounded CSI runtime backend adapter now
   sit on top of runtime-owned truth

What this milestone proves:

1. the new kernel can now carry RF2 failover, active replication observation,
   bounded continuity, real serving, and one outward/operator/CSI surface stack
   without collapsing authority ownership
2. runtime/product-facing state can now be attached as compressed projection
   rather than by inventing a new truth owner
3. the `masterv2` identity boundary and primary-led data-control boundary still
   remain intact
4. one bounded working RF2 block path now exists

What it does not yet prove:

1. broad RF2 product approval across deployment and frontend/operator matrices
2. full rebuild lifecycle choreography beyond the bounded repair wrapper
3. multi-process / multi-host proof for the current working path
4. pilot-ready or launch-ready working block behavior

## Next Major Milestone

The next major milestone should be:

`multi-process and pilot-ready RF2 validation`

This means one level above the current runtime/product surface slice:

1. prove the current working path outside the current bounded runtime harness
2. widen the working path into multi-process or multi-host validation
3. harden rebuild lifecycle and operator/CSI behavior on that wider path
4. attach pilot/preflight/containment evidence on top of the widened path

### Target Shape

Code should look roughly like:

1. `masterv2`: identity authority only
2. `volumev2`: runtime-owned failover, active service Loop 2, repair, continuity,
   and projected RF2 surfaces
3. `purev2`: execution adapter and local boundary observation
4. transport/session adapters: live participant communication beyond the current
   bounded harness
5. product-facing layer: bounded frontend/operator/CSI attachment with pilot
   artifacts

### Exit Criteria

The milestone should be considered complete when all are true:

1. the current bounded working path is proven outside the current harness
2. pilot/preflight/containment evidence exists for that wider path
3. no new product/runtime surface silently widens into broad launch approval

### Non-Goals

This milestone should not try to prove:

1. broad product launch approval
2. broad matrix approval across all transport/frontend combinations
3. broad `RF>2` product closure
4. reopening the kernel authority split

It should prove only that the current working RF2 block path can widen toward
pilot-ready validation without collapsing the ownership model.

## Main Risk

The main risk is not iSCSI or local I/O. The main risk is semantic leakage:

1. adding more backend-state shortcuts into control decisions
2. letting frontend/backend code redefine publication truth
3. rebuilding `weed/server` ownership inside `volumev2`
4. letting `masterv2` grow from identity authority into a centralized recovery
   planner
5. letting normalized sync facts drift into a hidden second protocol vocabulary
   with no explicit review surface

As long as those risks are resisted, the kernel can keep expanding cleanly.
