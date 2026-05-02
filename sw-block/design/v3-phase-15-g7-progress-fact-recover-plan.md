# V3 Phase 15 / G7 — Progress Fact + Constant Recover Plan

**Status**: draft plan for staged implementation.

**Scope**: `seaweed_block` v3 recovery/replication path. Design text lives here in `seaweedfs/sw-block/design/`.

**Goal**: make recover a normal feeder/coordinator workflow, not a one-off targetLSN job. The system should use one progress fact model, one recover decision owner, and one monotonic WAL egress owner per peer.

---

## 0. Current State And Gap

The project already has coordinator layers; the issue is that their facts are still split.

| Layer | Current owner | What exists today | Gap |
|-------|---------------|-------------------|-----|
| Semantic coordinator | `core/engine` via `adapter.VolumeReplicaAdapter` | `ProbeResult` normalizes into `R/S/H`; engine decides `none / catch_up / rebuild / unknown`. | No unified progress fact type; decisions are tied to probe/session events directly. |
| Peer/probe host | `core/replication.ReplicationVolume` | Maintains peer set and degraded-peer probe loop; `G5_5C` covers restart → probe → auto catch-up. | Auto rebuild for no-trusted-R / R<S needs explicit component coverage. |
| Session/pin coordinator | `core/recovery.PeerShipCoordinator` | Owns active session phase, route-local-write, barrier witness, and pin floor. | Durable ack updates pin, but is not yet a first-class engine progress fact. |
| Feeder/executor | `core/transport` + `core/recovery` | Owns WAL/BASE movement and monotonic egress path. | Should remain execution-only; must not decide recovery class or membership. |

Immediate conclusion: **do not invent a new top-level coordinator**. Instead, add a shared progress fact seam so existing coordinators consume the same facts with explicit authority rules.

---

## 1. Target State

### 1.1 Roles

| Role | Owns | Must not own |
|------|------|--------------|
| **Probe / ack producer** | Measures facts: replica durable frontier, retention window, identity, liveness. | Recovery class, membership, WAL recycle release. |
| **Coordinator / engine** | Interprets facts and decides `healthy`, `lagging`, `catch_up`, `rebuild`, `degraded`, serving eligibility. | Byte movement / WAL cursor mutation. |
| **Feeder / shipper** | Executes ordered feeding: BASE and WAL frames, monotonic cursor, durable ack reporting. | Whether a replica is recovered, in quorum, or must rebuild. |
| **Storage / WAL retention** | Retains or releases WAL according to durable ack driven pin floor. | Probe-based release or health decisions. |

Short rule: **feeder owns ordering; coordinator owns meaning**.

### 1.2 Unified fact model

All layers should eventually translate their observations into one conceptual fact:

```text
ReplicaProgressFact {
  replica_id
  epoch
  endpoint_version
  observed_at

  replica_R       // durable frontier observed on replica
  replica_S       // replica retained tail, if known
  replica_H       // replica local head, if known

  primary_S       // primary retained WAL tail
  primary_H       // primary current head

  source          // probe | durable_ack | session_close | startup
  confidence      // live_wire | cached | missing
}
```

Rules:

1. `durable_ack` can advance WAL release / pin floor.
2. `probe` can classify recovery need, but cannot by itself release WAL.
3. `session_close` can close a bounded feeding attempt, but cannot by itself admit membership/quorum.
4. `targetLSN` is not part of this fact except as legacy wire/frontier-hint text.

### 1.3 Recover as normal feeding

Recover is a coordinator-selected feeder mode:

| State | Fact shape | Coordinator action | Feeder action |
|-------|------------|--------------------|---------------|
| `Unknown/NewPeer` | no trusted `replica_R` | choose rebuild | BASE from `BasePinLSN`, WAL after pin |
| `CatchUpCapable` | `primary_S <= replica_R+1 <= primary_H` | choose catch-up | WAL from `replica_R+1` |
| `NeedsRebuild` | `replica_R < primary_S`, identity mismatch, or missing base truth | choose rebuild | full/base cover + WAL |
| `LaggingButProgressing` | lag exists but ack/probe advances over window | keep feeding | no rebuild escalation |
| `Unreachable` | probe/ship timeout | mark degraded/unknown; policy decides serving | no self-started rebuild by feeder |
| `LiveTail` | lag within policy window for N observations | eligible for healthy/sync participation | normal WAL feed |

Lag is normal. Recover starts when coordinator policy says the peer cannot catch up by ordinary feeding or needs a base cover.

---

## 2. TargetLSN Standard

`targetLSN` / `TargetLSN` has one allowed meaning during this migration:

> legacy wire/frontier-hint/band text for compatibility and diagnostics.

It must not be:

1. a receiver completion predicate;
2. a coordinator "recovered" predicate;
3. a lane discriminator;
4. a WAL release predicate;
5. a membership/quorum predicate.

Allowed uses:

1. encode/decode compatibility in old lineage structs;
2. diagnostics named `frontierHint` / `compatBand`;
3. tests explicitly marked as legacy band oracle;
4. bridge paths that are documented as legacy and not product closure.

---

## 3. TDD Strategy

### 3.1 Red/green waves

| Wave | Test level | Red condition | Green condition |
|------|------------|---------------|-----------------|
| **W1: fact classifier** | unit | same R/S/H from probe vs ack classifies differently | one pure classifier produces same decision from one fact type |
| **W2: targetLSN inert** | unit/component | changing `TargetLSN` changes lane/close/release | changing `TargetLSN` changes only diagnostics/wire echo |
| **W3: durable ack pin** | unit/e2e | pin advances to applied but unsynced LSN | pin advances only to durable ack |
| **W4: auto recover** | component | replica restart does not recover without manual call | probe loop emits catch-up/rebuild and converges |
| **W5: lag policy** | component | temporary ack lag triggers rebuild immediately | lagging-but-progressing remains feeding; stalled/out-of-window escalates |
| **W6: retention safety** | component/integration | WAL recycles past active replica need | recycle floor respects min durable ack across active sessions |

### 3.2 Existing anchors

Already landed in `seaweed_block`:

| Commit / area | Coverage |
|---------------|----------|
| `32b5e65 recovery: ack base batches at durable frontier` | W3: receiver ack is durable (`min(WALApplied, store.Sync())`) |
| `de8f7a0 recovery: retain writes during registered pre-ready session` | sender start-window route ownership |
| single feed ingress / cursor-advance commits | feeder monotonic single-ingress direction |
| `core/replication/component/g5_5c_restart_catchup_test.go` | W4: restart + probe loop triggers automatic catch-up |

### 3.3 New tests to add next

1. `TestProgressFact_ClassifiesCatchupRebuildNoop`
   - Pure table test.
   - Inputs: `R/H/S` shapes.
   - Expected: no-op, catch-up, rebuild, unknown.

2. `TestProgressFact_SourceDoesNotChangeClassification`
   - Same numeric fact from `probe` and `durable_ack`.
   - Expected same recovery class.
   - Only release eligibility differs.

3. `TestCoordinator_DurableAckCanReleaseProbeCannot`
   - Probe reports high `R`; durable ack stays low.
   - Expected: recovery class may improve, but WAL pin/release does not advance.

4. `TestCoordinator_LaggingButProgressing_DoesNotRebuild`
   - Durable ack sequence: `9,10,9,11,12` near threshold.
   - Expected: keep feeding, no rebuild.

5. `TestCoordinator_StalledLag_EscalatesWithinRetentionToCatchup`
   - Ack flatlines while `primary_H` advances and `R >= S`.
   - Expected: catch-up/recovery command, not full rebuild.

6. `TestCoordinator_RBelowPrimaryTail_EscalatesToRebuild`
   - `R < primary_S`.
   - Expected: rebuild.

7. `TestAutoRecover_NewReplicaNoTrustedR_FullRebuild`
   - Replica comes online with no trusted durable frontier.
   - Expected: coordinator selects rebuild with `BasePinLSN`.

---

## 4. Implementation Steps

### Step A — Introduce progress fact as a test-first seam

Add a small package or internal type, preferably under `core/replication` or `core/recovery`:

```go
type ProgressSource string
const (
  ProgressFromProbe ProgressSource = "probe"
  ProgressFromDurableAck ProgressSource = "durable_ack"
  ProgressFromSessionClose ProgressSource = "session_close"
)

type ReplicaProgressFact struct { ... }
type RecoveryClass string // none | lagging | catch_up | rebuild | unknown
func ClassifyProgress(f ReplicaProgressFact) RecoveryClass
func CanAdvanceRecyclePin(f ReplicaProgressFact) bool
```

TDD first: add W1/W2 tests before wiring production.

### Step B — Translate existing facts, do not change behavior yet

Adapters:

1. `adapter.ProbeResult` -> `ReplicaProgressFact{source: probe}`.
2. `BaseBatchAck` / coordinator `SetPinFloor` path -> `ReplicaProgressFact{source: durable_ack}`.
3. Session close / barrier achieved -> observation fact only, not health fact.

Tests should assert translation only. No coordinator behavior change in this step.

### Step C — Move coordinator decisions to the fact classifier

Engine/coordinator should consume `ReplicaProgressFact` rather than ad hoc R/S/H fields.

Required invariants:

1. same fact numbers produce same recovery class regardless of source;
2. source controls authority: only durable ack can release pin;
3. membership/healthy requires coordinator policy, not session close alone.

### Step D — Add lag policy

Define policy inputs:

```text
max_sync_lag_lsn
max_sync_lag_duration
min_progress_delta
probe_timeout
ack_timeout
```

Lagging-but-progressing is not failure. Stalled lag or missing probe becomes degraded/unknown, then catch-up/rebuild depending on `R/S/H`.

### Step E — Retire targetLSN influence

Run targeted grep and reduce remaining uses to:

1. wire compatibility;
2. old tests labeled `BandOracle`;
3. diagnostic aliases.

Any production predicate using `TargetLSN` for close/release/lane/health fails W2.

### Step F — Component / integration closure

Add one end-to-end component test:

```text
primary writes
replica offline
primary writes more
replica online
probe loop observes R/S/H
coordinator chooses catch-up or rebuild
feeder converges
durable ack advances pin
byte-equal convergence
```

Existing `G5_5C` covers catch-up. Add a rebuild variant for new/no-trusted-R or `R < S`.

---

## 5. Acceptance Criteria

1. `targetLSN` no longer affects system behavior outside explicit legacy compatibility.
2. One classifier owns recover class decisions from unified progress facts.
3. Feeder never starts recover by itself; it reports progress/failure only.
4. Durable ack is the only path to advance recycle/release pin.
5. Probe loop can bring an online replica back via catch-up or rebuild without manual operator call.
6. Existing hardware #2/#5/#6 remains green.
7. New component tests cover:
   - auto catch-up,
   - auto rebuild,
   - lagging-but-progressing no rebuild,
   - stalled/out-of-window escalation,
   - probe cannot release WAL.

---

## 6. Non-goals

1. Do not delete wire `TargetLSN` in this phase.
2. Do not merge physical TCP lanes in this phase.
3. Do not make feeder a membership authority.
4. Do not require a single Go object if a single serialized state machine is already provable.
5. Do not block every write waiting for recover; use durable ack/probe windows.

---

## 7. Recommended Next Commit Order

1. Pure unit tests + classifier type (`ReplicaProgressFact`).
2. ProbeResult/BaseBatchAck translation tests.
3. Coordinator consumes classifier for catch-up/rebuild/no-op.
4. Lag policy tests and implementation.
5. Auto rebuild component test (`R < S` or no trusted R).
6. TargetLSN grep cleanup and strict test.

This keeps each commit reviewable and prevents another broad recover rewrite without red/green control.
