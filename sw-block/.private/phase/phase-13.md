# Phase 13

Date: 2026-04-02
Status: active
Purpose: carry one explicit engineering gap beyond accepted `Phase 12` hardening into a bounded implementation phase so `RF=2 sync_all` becomes a correct, test-backed replicated durability mode under real reconnect, catch-up, retention, and rebuild conditions

## Why This Phase Exists

`Phase 09` accepted chosen-path execution closure.
`Phase 10` accepted bounded control-plane closure.
`Phase 11` accepted bounded product-surface rebinding.
`Phase 12` accepted bounded hardening, diagnosability, and first-launch envelope evidence.

What still remains is not broad protocol discovery.
It is one concrete engineering problem:

1. `sync_all` still needs a cleaner replicated-durability contract under cross-machine reconnect and replica recovery reality
2. that contract must be expressed in code and tests so later feature work can reuse it rather than reopen replication semantics repeatedly

## Phase Goal

Turn `RF=2 sync_all` from a bounded chosen-path mode with accepted launch-hardening evidence into a correct, reusable replicated-durability model for reconnect, catch-up, retention, and rebuild on real workloads.

Execution note:

1. use `phase-13-log.md` as the technical pack for:
   - checkpoint breakdown
   - acceptance objects
   - reject shapes
   - assignment text for `sw` and `tester`
2. prefer test-first baseline plus checkpointed implementation
3. keep the goal narrow: replication correctness first, not broad optimization or new transport work

## Scope

### In scope

1. canonical replica address truth
2. authoritative per-replica durable-progress tracking
3. reconnect handshake and WAL catch-up
4. replica-aware WAL retention / truncation
5. rebuild fallback when catch-up is impossible
6. real ext4 / PostgreSQL validation on real block devices for cross-machine `sync_all`
7. mode normalization work that depends directly on the corrected replication model

### Out of scope

1. broad new protocol discovery outside the replication path
2. new transport projects such as `SPDK`, `io_uring`, or striped-layout redesign
3. generic benchmark positioning beyond correctness-backed validation
4. unrelated control-plane or product-surface expansion
5. reopening accepted `Phase 09` / `Phase 10` / `Phase 11` / `Phase 12` semantics unless this phase exposes a real bug

## Phase 13 Items

### `CP13-1`: Test-First Baseline

Goal:

- freeze a failing/passing baseline that exposes the current replication gaps before protocol work begins

Acceptance object:

1. the focused sync-replication gap tests exist
2. they are run on current code before major implementation work
3. the fail/pass split is captured explicitly so later checkpoint claims are grounded

Status:

- accepted

Carry-forward:

1. the baseline report is frozen in `phase-13-cp1-baseline.md`
2. no protocol code was changed in `CP13-1`
3. `CP13-2` and later checkpoints must treat the baseline as the starting truth, not redefine it after implementation

### `CP13-2`: Canonical Replica Addressing

Goal:

- make replica endpoint truth canonical and routable so cross-machine replication never depends on wildcard listener strings, incomplete `:port` forms, or other non-authoritative address leakage

Acceptance object:

1. `CP13-2` accepts canonical replica address truth for the replication path
2. it does not accept durable-progress truth, reconnect protocol, WAL retention, or rebuild fallback by implication
3. it does not accept broad networking redesign beyond endpoint canonicalization

Execution steps:

1. Step 1: address truth contract freeze
   - define the canonical replica endpoint form for replication surfaces as routable `host:port`
   - define which forms are invalid for exported/registered truth:
     - bare `:port`
     - wildcard listener strings such as `[::]:port`
     - accidental loopback when cross-machine routing is intended
2. Step 2: implementation hardening
   - canonicalize replica listener addresses at the source where receiver/registration surfaces expose them
   - keep authoritative endpoint truth aligned across local listener state, registration/heartbeat publication, and any registry copies
3. Step 3: proof package
   - prove canonical `host:port` truth is emitted under wildcard-bind cases
   - prove no wildcard or incomplete address string leaks into exported replication truth
   - prove no-overclaim around reconnect, retention, or rebuild semantics

Required scope:

1. replica receiver endpoint truth
2. registration / heartbeat / registry path carrying replica endpoints
3. one focused wildcard-bind proof plus bounded cross-machine truth checks
4. explicit distinction between address canonicalization and later reconnect protocol work

Must prove:

1. cross-machine replica addresses exported for replication are canonical routable `host:port`
2. wildcard bind strings do not escape into replication truth
3. local canonicalization does not silently rewrite intentionally loopback-only cases into incorrect external truth
4. acceptance wording stays bounded to endpoint truth rather than later replication recovery semantics

Reuse discipline:

1. `weed/storage/blockvol/replica_receiver.go`, `replica_meta.go`, and nearby address helpers may be updated in place as the primary endpoint-truth surface
2. `weed/server/master_block_registry.go` and heartbeat/registration paths may be updated in place only if needed to keep authoritative endpoint truth aligned
3. focused unit/protocol tests should carry the main proof burden; component tests are support-only unless they prove an otherwise unreachable leak
4. no checkpoint work may silently introduce reconnect protocol, retention policy, or rebuild logic

Verification mechanism:

1. one focused wildcard-bind canonicalization proof
2. explicit checks that exported/registered replica endpoints are routable `host:port`
3. no-overclaim review so `CP13-2` does not absorb `CP13-3+`

Hard indicators:

1. one accepted canonical-endpoint proof:
   - wildcard-bind listener state resolves to canonical exported `host:port`
2. one accepted no-leak proof:
   - bare `:port` / wildcard listener strings no longer escape into replication truth
3. one accepted boundedness proof:
   - `CP13-2` claims endpoint truth only, not reconnect or durability semantics

Reject if:

1. the checkpoint fixes only one test string shape but leaves other exported endpoint paths unchanged
2. canonicalization happens only in tests rather than at the production truth surface
3. the checkpoint quietly broadens into reconnect, retention, or rebuild protocol work

Status:

- accepted

Carry-forward:

1. `localServerID` remains stable control identity and may be opaque
2. `advertisedHost` is now the transport-facing canonicalization input for wildcard-bind replica endpoints
3. `CP13-3` and later checkpoints must not reopen identity-vs-transport separation unless a new concrete bug is exposed

### `CP13-3`: Durable Progress Truth

Goal:

- make durable replication progress explicit and authoritative so sync correctness is grounded in replica flushed durability rather than sender-side send progress or loosely inferred health

Acceptance object:

1. `CP13-3` accepts durable progress truth for the replication path
2. it does not accept reconnect/catch-up protocol, retention policy, rebuild fallback, or broader state-machine closure by implication
3. it does not accept generic “tests pass” reasoning without an explicit durable-progress contract review

Execution steps:

1. Step 1: durable-progress contract freeze
   - define `replicaFlushedLSN` as replica-side WAL durability confirmed at barrier time
   - define sender-side shipped/sent progress as diagnostic only, not authority for sync correctness
   - define what barrier responses must expose as explicit durable progress truth
2. Step 2: implementation hardening or proof confirmation
   - update the durable-progress path only where current code fails to meet the contract
   - if current code already satisfies the contract, keep changes minimal and make the proof package explicit instead of broadening scope
3. Step 3: proof package
   - prove barrier success is grounded in replica flushed durability
   - prove flushed progress is monotonic within epoch and not updated on mere receive
   - prove no-overclaim around `CP13-4+`

Required scope:

1. replica receiver durable-progress state
2. barrier request/response path
3. sender/group tracking of replica durable progress
4. explicit separation between durable-progress truth and later reconnect / retention semantics

Must prove:

1. `replicaFlushedLSN` means replica durability, not sender transmission progress
2. barrier responses expose durable progress explicitly enough for sync correctness decisions
3. sender-side progress such as shipped/sent LSN is diagnostic only and cannot authorize sync success
4. acceptance wording stays bounded to durable-progress truth rather than broader recovery/state-machine closure

Reuse discipline:

1. `weed/storage/blockvol/replica_apply.go`, `wal_shipper.go`, `dist_group_commit.go`, and related protocol message code may be updated in place as the primary durable-progress surfaces
2. focused unit/protocol tests should carry the main proof burden
3. `weed/server/*` should remain reference only unless durable-progress truth requires an exposed wiring change
4. no checkpoint work may silently introduce reconnect protocol, retention policy, rebuild policy, or broader transport redesign

Verification mechanism:

1. one focused proof set around barrier/flushed progress truth
2. explicit checks that receive progress alone does not advance durable authority
3. no-overclaim review so `CP13-3` does not absorb `CP13-4+`

Hard indicators:

1. one accepted barrier-truth proof:
   - barrier success is tied to replica flushed durability
2. one accepted monotonicity proof:
   - `replicaFlushedLSN` is monotonic within epoch
3. one accepted no-false-authority proof:
   - sender-side shipped/sent progress is diagnostic only
4. one accepted boundedness proof:
   - `CP13-3` claims durable-progress truth only

Reject if:

1. the checkpoint treats passing baseline tests as automatic closure without reviewing the durable-progress contract
2. durable-progress truth is still mixed with sender-side transmission progress
3. the checkpoint quietly broadens into reconnect, retention, rebuild, or general replication redesign

Status:

- accepted

Carry-forward:

1. `replicaFlushedLSN` is now the authoritative durable-progress variable for `sync_all`
2. legacy `BarrierOK` responses without `FlushedLSN` are rejected and cannot count as durable authority
3. `CP13-4` and later checkpoints must treat sender-side send progress as diagnostic only, not as sync-correctness authority

### `CP13-4`: Replica State Machine / Barrier Eligibility

Goal:

- make replica state and barrier eligibility explicit so only `InSync` replicas can satisfy sync durability while non-eligible states fail closed instead of drifting into accidental success

Acceptance object:

1. `CP13-4` accepts the replica state machine and barrier-eligibility contract
2. it does not accept reconnect/catch-up protocol, retention policy, rebuild fallback, or broader rollout claims by implication
3. it does not accept vague “state seems fine” reasoning without an explicit eligibility contract

Execution steps:

1. Step 1: state contract freeze
   - define the bounded state set used by the replication path:
     - `Disconnected`
     - `Connecting`
     - `CatchingUp`
     - `InSync`
     - `Degraded`
     - `NeedsRebuild`
   - define barrier eligibility:
     - only `InSync` replicas count toward sync durability
     - non-eligible states must pre-reject or fail closed
2. Step 2: implementation hardening or proof confirmation
   - update the state/eligibility path only where current code fails the contract
   - if current code already satisfies much of the contract, keep code changes minimal and make the proof package explicit
3. Step 3: proof package
   - prove barrier rejects replicas not eligible for sync durability
   - prove degraded or catching-up replicas do not silently count toward `sync_all`
   - prove no-overclaim around `CP13-5+`

Required scope:

1. replica shipper state transitions and eligibility checks
2. barrier admission path
3. `sync_all` failure semantics when replicas are non-eligible
4. explicit separation between state eligibility and later reconnect/rebuild protocol work

Must prove:

1. only `InSync` replicas count toward sync durability
2. `Disconnected`, `Connecting`, `CatchingUp`, `Degraded`, and `NeedsRebuild` do not silently satisfy barrier eligibility
3. degraded/non-eligible replicas fail closed for `sync_all` rather than producing false durability success
4. acceptance wording stays bounded to state/eligibility truth rather than reconnect, retention, or rebuild closure

Reuse discipline:

1. `weed/storage/blockvol/wal_shipper.go`, `dist_group_commit.go`, `shipper_group.go`, and nearby replication coordination code may be updated in place as the primary state/eligibility surfaces
2. focused unit/protocol/adversarial tests should carry the main proof burden
3. `weed/server/*` should remain reference only unless state eligibility requires a surfaced wiring correction
4. no checkpoint work may silently introduce reconnect handshake, retention policy, rebuild flow, or broader transport redesign

Verification mechanism:

1. one focused proof set around replica state and barrier eligibility
2. explicit checks that non-`InSync` states cannot satisfy `sync_all`
3. no-overclaim review so `CP13-4` does not absorb `CP13-5+`

Hard indicators:

1. one accepted eligibility proof:
   - only `InSync` replicas count toward sync durability
2. one accepted fail-closed proof:
   - non-eligible replicas cause bounded failure rather than false success
3. one accepted state-boundary proof:
   - barrier rejects or excludes disallowed states explicitly
4. one accepted boundedness proof:
   - `CP13-4` claims state/eligibility truth only

Reject if:

1. the checkpoint treats passing baseline tests as automatic closure without restating the state/eligibility contract
2. non-eligible replica states can still satisfy sync durability
3. the checkpoint quietly broadens into reconnect, retention, rebuild, or general replication redesign

Status:

- accepted

Carry-forward:

1. the replica state set and barrier-eligibility contract are now explicit
2. only `InSync` may satisfy sync durability; `Disconnected`/`Degraded` may invoke `Barrier()` only as bounded recovery entry paths
3. `CP13-5` and later checkpoints must preserve this eligibility boundary rather than reopening it implicitly

### `CP13-5`: Reconnect Handshake + WAL Catch-up

Goal:

- make reconnect after replica disturbance explicit and correct so a replica with known durable progress can resume from retained WAL, catch up, and re-enter `InSync` without false bootstrap success or barrier hangs

Acceptance object:

1. `CP13-5` accepts the reconnect handshake and WAL catch-up contract for recoverable gaps on the replication path
2. it does not accept replica-aware WAL retention policy, full rebuild fallback lifecycle, or broader rollout claims by implication
3. it does not accept vague “reconnect seems to work” reasoning without an explicit resume/catch-up contract

Execution steps:

1. Step 1: reconnect contract freeze
   - define when a replica must use bootstrap versus reconnect:
     - fresh replica with no prior durable progress may bootstrap
     - replica with prior flushed progress must reconnect via explicit resume truth
   - define reconnect decision outcomes:
     - already caught up
     - recoverable gap within retained WAL
     - unrecoverable gap that must fail closed and defer full rebuild handling to `CP13-7`
2. Step 2: implementation hardening
   - update the reconnect path only where current code still fails the resume/catch-up contract
   - ensure catch-up replays retained WAL before barrier success is allowed
   - ensure repeated disconnect/reconnect cycles remain bounded and do not silently fall back to unsafe bootstrap
3. Step 3: proof package
   - prove degraded replicas with prior durable progress use handshake/reconnect rather than bootstrap
   - prove retained-WAL catch-up completes and re-enters `InSync` on recoverable gaps
   - prove reconnect fails closed on unrecoverable or incomplete recovery cases
   - prove no-overclaim around `CP13-6+`

Required scope:

1. `wal_shipper` reconnect discriminator and resume handshake
2. retained-WAL catch-up replay path
3. repeated disconnect/reconnect recovery behavior
4. bounded failure semantics for gaps that cannot be recovered within this checkpoint
5. explicit separation between reconnect/catch-up closure and later retention/rebuild policy work

Must prove:

1. fresh shippers bootstrap, but previously-synced shippers reconnect using resume truth
2. barrier success after disturbance is allowed only after reconnect/catch-up has re-established `InSync`
3. repeated disconnect/reconnect cycles do not strand the replica in false degraded recovery
4. recoverable gaps replay retained WAL correctly without overwriting newer replica data
5. acceptance wording stays bounded to reconnect/catch-up truth rather than retention or rebuild closure

Reuse discipline:

1. `weed/storage/blockvol/wal_shipper.go`, reconnect/catch-up helpers, and nearby replication protocol code may be updated in place as the primary reconnect surface
2. focused protocol/adversarial tests should carry the main proof burden; component tests are support-only unless a protocol gap is otherwise unreachable
3. `weed/server/*` should remain reference only unless reconnect correctness requires surfaced wiring changes
4. no checkpoint work may silently broaden into retention policy, rebuild orchestration, or performance tuning

Verification mechanism:

1. one focused proof set around reconnect discriminator, catch-up replay, and post-reconnect barrier behavior
2. explicit checks for repeated disconnect/reconnect recovery
3. explicit checks that recoverable gaps replay retained WAL before sync success
4. no-overclaim review so `CP13-5` does not absorb `CP13-6+`

Hard indicators:

1. one accepted reconnect-discriminator proof:
   - prior durable progress uses handshake/reconnect rather than bootstrap
2. one accepted catch-up proof:
   - recoverable retained-WAL gap replays and returns to `InSync`
3. one accepted repeated-recovery proof:
   - multiple disconnect/reconnect cycles recover without hanging or drifting
4. one accepted fail-closed proof:
   - reconnect does not falsely succeed when recovery is incomplete or impossible within retained WAL
5. one accepted boundedness proof:
   - `CP13-5` claims reconnect/catch-up truth only

Reject if:

1. a previously-synced replica can still skip resume truth and succeed via unsafe bootstrap
2. barrier success can occur before reconnect/catch-up has restored `InSync`
3. repeated reconnect cycles still hang, strand, or silently degrade correctness
4. the checkpoint quietly broadens into retention, explicit `NeedsRebuild` lifecycle closure, rebuild execution, or general replication redesign

Status:

- accepted

Carry-forward:

1. replacement shippers now preserve prior durable-progress intent across `SetReplicaAddrs`
2. previously-synced replicas must reconnect through resume truth and retained-WAL catch-up rather than unsafe bootstrap
3. `CP13-6` and later checkpoints must preserve the reconnect/catch-up contract rather than weakening it through reclaim or rebuild shortcuts

### `CP13-6`: Replica-Aware WAL Retention

Goal:

- make WAL retention explicit and replica-aware so reclaim is gated by recoverable replica progress and bounded retention budgets rather than silently discarding catch-up-critical WAL

Acceptance object:

1. `CP13-6` accepts replica-aware WAL retention and retention-budget truth on the replication path
2. it does not accept full rebuild fallback lifecycle, rebuild execution, or broader rollout claims by implication
3. it does not accept vague “reclaim seems safe” reasoning without an explicit retention contract

Execution steps:

1. Step 1: retention contract freeze
   - define which replica progress is authoritative for WAL retention:
     - only replicas with prior durable progress and still recoverable state may hold WAL
   - define bounded retention outcomes:
     - reclaim blocked while a recoverable replica still needs retained WAL
     - timeout / max-bytes budgets may escalate boundedly and release the WAL hold
     - full rebuild handling after escalation remains `CP13-7`
2. Step 2: implementation hardening
   - update the retention path only where current code still fails the bounded retention contract
   - ensure retention decisions use replica-aware progress rather than primary-local heuristics alone
   - ensure budget-triggered escalation is explicit and fail-closed rather than silent reclaim
3. Step 3: proof package
   - prove recoverable replicas block reclaim of needed WAL
   - prove timeout / max-bytes budgets trigger bounded escalation instead of indefinite WAL growth
   - prove retention remains aligned with `CP13-5` reconnect/catch-up truth
   - prove no-overclaim around `CP13-7+`

Required scope:

1. WAL retention/reclaim gates
2. shipper-group retention inputs derived from recoverable replica progress
3. bounded timeout / max-bytes escalation behavior
4. explicit separation between retention truth and full rebuild lifecycle closure

Must prove:

1. reclaim does not drop WAL still required by a recoverable replica
2. retention inputs come from replica-aware durable progress, not sender-side guesses
3. timeout / max-bytes budgets trigger bounded escalation when WAL cannot be held indefinitely
4. acceptance wording stays bounded to retention truth rather than full rebuild closure

Reuse discipline:

1. `weed/storage/blockvol` WAL-retention, flusher, shipper-group, and adjacent replication coordination code may be updated in place as the primary retention surface
2. focused unit/protocol tests should carry the main proof burden; component tests are support-only unless a retention gap is otherwise unreachable
3. `weed/server/*` should remain reference only unless retention truth requires surfaced reporting changes
4. no checkpoint work may silently broaden into rebuild execution, broad control-plane redesign, or performance tuning

Verification mechanism:

1. one focused proof set around retention hold, reclaim gating, and budget-triggered escalation
2. explicit checks that max-bytes and timeout paths are real production behaviors, not just comments/logs
3. explicit checks that retention stays compatible with `CP13-5` recoverable catch-up
4. no-overclaim review so `CP13-6` does not absorb `CP13-7+`

Hard indicators:

1. one accepted hold-back proof:
   - recoverable replicas block reclaim of required WAL
2. one accepted timeout-budget proof:
   - timeout can escalate a stalled recoverable replica into bounded fail-closed behavior
3. one accepted max-bytes-budget proof:
   - max-bytes pressure triggers explicit bounded escalation rather than silent reclaim or TODO-only behavior
4. one accepted boundedness proof:
   - `CP13-6` claims retention truth only

Reject if:

1. reclaim can still silently discard WAL needed for a recoverable replica
2. max-bytes behavior is still only log text / placeholder behavior without real state effect
3. the checkpoint quietly broadens into full `NeedsRebuild` lifecycle closure, rebuild execution, or general replication redesign

Status:

- accepted

Carry-forward:

1. retention inputs and bounded retention budgets are now replica-aware
2. timeout and max-bytes escalation can move a stalled recoverable replica into `NeedsRebuild`
3. `CP13-7` must turn that escalation into a real fail-closed rebuild lifecycle rather than leaving `NeedsRebuild` as a partially-signaled state

### `CP13-7`: Rebuild Fallback

Goal:

- make `NeedsRebuild` a real fail-closed recovery state so unrecoverable replicas stop participating in normal replication paths, surface rebuild intent clearly, and re-enter the replication contract only through bounded rebuild handoff

Acceptance object:

1. `CP13-7` accepts the `NeedsRebuild` fallback and bounded rebuild handoff lifecycle on the replication path
2. it does not accept broad rollout claims or real-workload validation by implication
3. it does not accept vague “rebuild eventually works” reasoning without an explicit fail-closed lifecycle contract

Execution steps:

1. Step 1: rebuild-fallback contract freeze
   - define what `NeedsRebuild` means:
     - unrecoverable via retained WAL catch-up
     - excluded from normal ship/barrier success
     - visible to rebuild orchestration and observability surfaces
   - define lifecycle boundaries:
     - detection/escalation into `NeedsRebuild`
     - fail-closed behavior while in `NeedsRebuild`
     - bounded rebuild handoff and post-rebuild re-entry
2. Step 2: implementation hardening
   - update the rebuild-fallback path only where current code still leaves `NeedsRebuild` partial, leaky, or inconsistent
   - ensure ship/barrier paths block correctly while `NeedsRebuild`
   - ensure successful rebuild resets progress/state in a way compatible with later re-entry
3. Step 3: proof package
   - prove unrecoverable gaps transition to `NeedsRebuild`
   - prove `NeedsRebuild` blocks normal replication participation
   - prove rebuild handoff can re-establish a bounded healthy starting point
   - prove no-overclaim around `CP13-8+`

Required scope:

1. `NeedsRebuild` detection and state ownership on the primary shipper side
2. fail-closed behavior for ship/barrier and related replication paths while `NeedsRebuild`
3. rebuild start/abort/complete handoff boundaries
4. post-rebuild progress/state initialization needed for safe re-entry
5. explicit separation between rebuild fallback closure and later real-workload validation

Must prove:

1. unrecoverable gaps do not remain merely degraded; they transition to `NeedsRebuild`
2. a shipper in `NeedsRebuild` cannot silently participate in ship/barrier success
3. rebuild completion restores a bounded re-entry point without faking immediate `InSync`
4. acceptance wording stays bounded to rebuild fallback truth rather than `CP13-8` rollout/workload claims

Reuse discipline:

1. `weed/storage/blockvol` rebuild, shipper-group, wal-shipper, and adjacent replication coordination code may be updated in place as the primary rebuild-fallback surface
2. focused unit/protocol/adversarial tests should carry the main proof burden; component tests are support-only unless a rebuild gap is otherwise unreachable
3. `weed/server/*` should remain reference only unless rebuild fallback requires surfaced status/reporting changes
4. no checkpoint work may silently broaden into real-workload benchmarking, performance tuning, or new protocol discovery

Verification mechanism:

1. one focused proof set around `NeedsRebuild` transition, blocking semantics, and rebuild re-entry
2. explicit checks that `NeedsRebuild` blocks normal replication paths rather than merely logging/marking degraded
3. explicit checks that post-rebuild progress initializes from bounded truth such as checkpoint state
4. no-overclaim review so `CP13-7` does not absorb `CP13-8+`

Hard indicators:

1. one accepted transition proof:
   - unrecoverable retained-WAL gap transitions to `NeedsRebuild`
2. one accepted fail-closed proof:
   - `NeedsRebuild` blocks ship/barrier participation
3. one accepted rebuild-handoff proof:
   - rebuild start/complete path restores a bounded re-entry state
4. one accepted post-rebuild-progress proof:
   - replica progress after rebuild is initialized from checkpoint truth, not stale/zeroed state
5. one accepted boundedness proof:
   - `CP13-7` claims rebuild fallback only

Reject if:

1. an unrecoverable gap can still linger in `Degraded` without escalating to `NeedsRebuild`
2. a `NeedsRebuild` shipper can still satisfy normal ship/barrier paths
3. rebuild completion jumps directly to misleading healthy semantics without bounded re-entry proof
4. the checkpoint quietly broadens into `CP13-8` real-workload validation or general replication redesign

Status:

- accepted

Carry-forward:

1. `NeedsRebuild` is now a real fail-closed fallback state
2. rebuild handoff and post-rebuild progress are bounded by checkpoint truth rather than implicit recovery assumptions
3. `CP13-8` must validate the accepted replication contract on named real workloads without reopening protocol semantics or quietly broadening into mode policy work

### `CP13-8`: Real-Workload Validation

Goal:

- validate the accepted `RF=2 sync_all` replication contract on one bounded set of real workloads so the engineering proof is no longer only protocol/unit-level but also demonstrated on named real block-device consumers

Acceptance object:

1. `CP13-8` accepts one bounded real-workload validation package for the accepted `RF=2 sync_all` path
2. it does not accept broad rollout claims, broad benchmark positioning, or mode normalization by implication
3. it does not accept vague “worked in a manual run” reasoning without named workloads, bounded envelope, and replayable evidence

Execution steps:

1. Step 1: workload envelope freeze
   - name one bounded validation matrix:
     - workload(s)
     - topology
     - transport/frontend
     - filesystem/application surface
     - disturbance shapes included and excluded
   - recommended first-cut surfaces:
     - real filesystem behavior such as `ext4`
     - one database/application surface such as `PostgreSQL`
2. Step 2: harness and evidence hardening
   - wire the workload run through real block-device consumers on the accepted path
   - keep the environment reproducible and bounded enough that failures are attributable
   - collect evidence at the same semantic layer as accepted prior checkpoints
3. Step 3: proof package
   - prove the named real workloads complete correctly on the accepted path
   - prove disturbance/failover behavior is bounded inside the named envelope if included
   - prove no-overclaim around `CP13-9+`

Required scope:

1. one bounded workload matrix on the accepted `RF=2 sync_all` path
2. real block-device consumer validation (not only protocol/unit tests)
3. bounded disturbance cases only if explicitly named in the envelope
4. explicit separation between real-workload proof and later mode normalization / rollout claims

Must prove:

1. the accepted replication contract survives contact with named real workloads
2. evidence is tied to a bounded environment and workload envelope, not generic “production ready” rhetoric
3. failures, if any, are attributable to explicit workload-envelope gaps rather than ambiguous harness drift
4. acceptance wording stays bounded to real-workload validation rather than `CP13-9` policy/mode closure

Reuse discipline:

1. prefer existing `testrunner`, bounded component scenarios, and real-device harnesses where possible
2. update `weed/storage/blockvol/*` only when the real workload exposes a concrete bug in accepted semantics
3. `weed/server/*` should remain reference only unless workload validation exposes a surfaced control/runtime issue
4. no checkpoint work may silently broaden into generic benchmark marketing, launch approval, or mode policy redesign

Verification mechanism:

1. one named workload matrix with explicit environment description
2. replayable runs or artifacts for the chosen workload package
3. explicit pass/fail conditions tied back to accepted `CP13-1..7` semantics
4. no-overclaim review so `CP13-8` does not absorb `CP13-9+`

Hard indicators:

1. one accepted filesystem proof:
   - a named real filesystem workload completes correctly on the accepted path
2. one accepted application proof:
   - a named real application/database workload completes correctly on the accepted path
3. one accepted envelope proof:
   - the validation matrix is explicit about topology, frontend, workload, and exclusions
4. one accepted boundedness proof:
   - `CP13-8` claims real-workload validation only

Reject if:

1. the checkpoint relies on ad hoc manual runs with no bounded envelope
2. a claimed real-workload proof is actually only a synthetic benchmark or unit test
3. delivery wording quietly broadens into mode normalization, launch approval, or general production-readiness claims

Status:

- active

### `CP13-8A`: Assignment-to-Publication Closure

Goal:

- close the control/runtime/publication contradiction exposed by `CP13-8` so the system no longer treats allocation or assignment presence as equivalent to replica publication readiness

Acceptance object:

1. `CP13-8A` accepts one bounded closure slice for assignment-to-publication truth on the accepted `RF=2 sync_all` path
2. it does not accept broad mode normalization, launch approval, or backend replacement by implication
3. it does not accept sleep-based or timing-based fixes that leave readiness semantics implicit

Execution steps:

1. Step 1: unify assignment lifecycle
   - ensure assignment delivery flows through one authoritative path from role apply to receiver/shipper wiring to readiness bookkeeping
   - remove semantic split between store-only role application and service-level replication/publication setup
2. Step 2: name readiness and publication truth
   - define explicit readiness states for the chosen path
   - ensure heartbeat / lookup / tester surfaces distinguish:
     - allocated
     - role applied
     - receiver ready
     - publish healthy
3. Step 3: bounded rerun
   - rerun the bounded `CP13-8` workload package after closure lands
   - determine whether the remaining contradiction is backend data visibility, adapter timing/publication, or a true core-rule gap

Required scope:

1. assignment-to-publication closure only
2. chosen path only: `RF=2 sync_all`
3. existing master / volume-server heartbeat path only
4. `blockvol` remains the execution backend

Must prove:

1. assignment delivered does not by itself imply receiver ready or publish healthy
2. replica publication requires explicit readiness closure rather than allocation completion or precomputed port presence
3. master lookup / REST / tester health checks consume the same bounded readiness truth
4. `CP13-8A` remains about closure, not mode normalization or backend redesign

Reuse discipline:

1. prefer `weed/server/*` and bridge-layer updates first because this is a surfaced control/runtime issue
2. update `weed/storage/blockvol/*` only if closure work exposes a concrete backend bug rather than a publication-path contradiction
3. keep `CP13-1..7` semantics fixed unless the closure work exposes a live contradiction
4. no checkpoint work may silently broaden into `CP13-9` mode policy or broad rollout claims

Verification mechanism:

1. one focused proof set around assignment lifecycle closure and readiness/publication gating
2. explicit tests that heartbeat / lookup / tester surfaces do not publish a replica before readiness closes
3. bounded `CP13-8` rerun or equivalent evidence showing the contradiction moves from mixed-state ambiguity to an attributable remaining cause
4. no-overclaim review so `CP13-8A` does not absorb `CP13-9`

Hard indicators:

1. one accepted lifecycle proof:
   - assignment processing uses one authoritative path from role apply through runtime wiring
2. one accepted readiness proof:
   - replica-ready is explicit and not inferred from mere existence/allocation
3. one accepted publication proof:
   - lookup / heartbeat / tester gates do not publish a replica before readiness closure
4. one accepted boundedness proof:
   - `CP13-8A` claims closure only and leaves broader mode policy untouched

Reject if:

1. assignment still reaches different semantic outcomes depending on whether it flows through heartbeat/store-only or service-level processing
2. a replica can still be surfaced as healthy/ready before receiver/session readiness closes
3. the slice relies on delays or ad hoc retries rather than explicit readiness semantics
4. delivery wording broadens into `CP13-9` mode normalization, launch approval, or generic backend replacement

Status:

- active

### Later checkpoints inside `Phase 13`

1. `CP13-9`: mode normalization (only after `CP13-8A` closes the assignment/publication contradiction)

## Reuse Discipline

1. `weed/storage/blockvol/*` is the primary implementation surface and may be updated in place
2. focused unit/component/adversarial tests should carry the main proof burden
3. real-node / real-device validation belongs in testrunner or bounded component scenarios, not chat prose
4. `weed/server/*` may be updated only when replication correctness requires registry / assignment / heartbeat truth to change
5. no checkpoint may silently broaden into performance-optimization or broad rollout work

## Expected Outcome

If `Phase 13` succeeds:

1. reconnect / catch-up / rebuild semantics become explicit and test-backed
2. `sync_all` correctness no longer depends on partial or implicit sender-state assumptions
3. later feature work can reuse a clearer replication contract instead of re-deriving durability semantics each time
