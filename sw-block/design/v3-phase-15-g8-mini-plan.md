# V3 Phase 15 — G8 Failover Data Continuity Mini-Plan

**Date**: 2026-05-02
**Status**: CLOSED 2026-05-02; code evidence pushed on `p15-g8/failover-data-continuity`
**Gate**: G8 — Failover Data Continuity
**Predecessor**: G7 CLOSED 2026-05-02, canonical evidence `seaweed_block@d09fcc6`
**Code repo**: `seaweed_block`
**Docs repo**: `seaweedfs/sw-block/design`
**§1.A architect ratification**: 2026-05-02. First G8 close uses RF=2 subprocess L2 with real `cmd/blockmaster` + 2x `cmd/blockvolume` daemons and explicit iSCSI reconnect as sufficient evidence. m01/M02 cross-node confirmation is forward-carry, not first-close blocking.

---

## 0. Product Sentence

After the primary for a replicated volume fails, the system can move service to an eligible replica without losing previously acknowledged data, and the old primary cannot later acknowledge stale reads or writes against the old authority line.

G8 is the first P15 gate that turns recovery correctness into availability semantics.

---

## 1. Scope

### 1.1 In scope

G8 covers one narrow but product-critical path:

1. A replicated volume has a primary and at least one eligible replica.
2. A client writes known data and receives success.
3. The current primary process is killed or made unreachable.
4. V3 authority publishes a new primary assignment.
5. The client reconnects or reattaches to the new primary.
6. The new primary reads the exact acknowledged data.
7. The old primary, if it returns, cannot corrupt future state and cannot serve stale success under the old line.

### 1.2 Required evidence shape

G8 success must include data verification. Authority movement alone is insufficient.

Required proof:

```text
acknowledged write before failure
  -> primary failure
  -> reassignment / new primary
  -> reconnect or reattach
  -> byte-equal read from new primary
  -> old primary stale path rejects
```

### 1.3 Out of scope

| Item | Disposition |
|---|---|
| Multi-master HA / distributed authority store | Out of P15 unless separately ratified. |
| Rack/AZ-aware placement | G20/P16; not G8. |
| Full chaos matrix | G22 final validation. G8 binds a minimal failure-continuity gate. |
| Performance SLO / failover time SLO | G21. G8 records timing, but no SLO claim. |
| CSI Kubernetes failover under workload | Dogfood checkpoint / G15a+G17 after G9A; G8 may provide the lower data-continuity primitive. |
| Snapshot/resize behavior across failover | G10/G11/G15b. |
| Strict RF=2 quorum/full-ack write contract under replica lag | Deferred to G8-followup/G9A policy. G8 proves healthy-path data continuity, not that every primary ACK is blocked on a replica durable ACK. |

### 1.4 ACK profile vs recovery profile

G8 keeps these two concepts separate:

1. **ACK profile** decides when the frontend write/sync returns success.
2. **Recovery profile** decides what happens when a replica falls behind, disappears, or misses progress.

`best-effort` means the frontend ACK path does not require a full remote replica durable ACK for every write/sync. It does **not** mean the system ignores lag. If progress facts show the replica is behind, stalled, or below the retained WAL window, the coordinator/feeder path must still catch up or rebuild that replica.

The stricter future profile is a quorum/full-ack mode: frontend success is withheld until the configured replica durability condition is met. That is the profile needed before claiming RF=2 zero acknowledged-write loss under arbitrary replica lag or failure.

For RF=2 in quorum/full-ack mode, a replica that has entered recovery cannot count as the synchronous ACK peer. While that condition holds, the primary has only three valid product behaviors:

1. **Fail or block new writes/syncs** until the replica leaves recovery and becomes sync-ack eligible again.
2. **Explicitly degrade the volume to best-effort / single-replica availability** through a named policy transition that is visible to operators and clients.
3. **Fence or make the volume read-only/unavailable** if policy requires no RPO exposure.

It must not silently ACK writes as "full sync" while the only secondary is in recovery. Recovery traffic may continue feeding the replica, but recovery progress is not a substitute for the synchronous ACK contract.

---

## 2. Starting Point

### 2.1 What G7 gives us

G7 closed:

1. empty replica join -> rebuild -> byte-equal;
2. concurrent writes during rebuild -> byte-equal;
3. stale WAL -> rebuild -> byte-equal;
4. practical single-ingress WAL feeder;
5. `targetLSN` removed as terminal completion truth;
6. progress fact and flow-control diagnostics seams.

G8 should not reopen those contracts unless a failing G8 test proves a missing handoff.

### 2.2 Current likely gap

The likely G8 gap is not raw rebuild transport. It is the integration boundary:

1. authority reassignment to new primary;
2. frontend readiness on the new primary;
3. stale frontend fail-closed on the old primary;
4. client reconnect / retry semantics;
5. ensuring the chosen new primary has the acknowledged data before service success.

---

## 3. Architecture Bindings

### 3.1 Truth domains

| Domain | G8 owner |
|---|---|
| Authority / assignment | master publisher remains single source of authority; no volume-local role minting. |
| Data correctness | primary/replica storage + replication/recovery proof. |
| Frontend stale fencing | frontend backend/projection must reject old line after EV/epoch moves. |
| Client reconnect | protocol-specific behavior may differ; G8 binds at least one declared path. |

### 3.2 Non-negotiable rules

1. Do not port V2 promote/demote or heartbeat-as-authority.
2. Do not declare failover success from assignment movement alone.
3. Do not let stale primary reads return success.
4. Do not let stale primary writes ACK success.
5. Do not make a recovered replica a serving primary solely because a rebuild session completed; assignment must come from master/publisher.

---

## 4. Scenario Set

### G8-0 — Authority failover eligibility + publisher epoch

**Purpose**: prove the control-plane precondition for failover before data-plane claims enter the picture.

**Shape**:

```text
current authority is r1@epoch=N
r1 is no longer acceptable
only ReadyForPrimary+Reachable+Eligible candidates may be selected
publisher mints r2@epoch=N+1 via IntentReassign
```

**Level**: authority unit/component.

**Pass**: controller emits only a bounded `IntentReassign`; publisher remains the sole epoch minter.

**Fail**: high evidence but not-ready candidate is selected, no-candidate case leaves a desired mint, or local code mints authority.

---

### G8-A — Component authority move + stale old primary fence

**Purpose**: prove stale read/write fail-closed at the backend/projection boundary after authority moves.

**Shape**:

```text
old primary backend writes data
authority moves epoch/EV to new primary
old backend write -> ErrStalePrimary / protocol failure
old backend read -> ErrStalePrimary / protocol failure
new backend can read/write under new line
```

**Level**: component.

**Expected first red test**: stale read path returns data success or stale write path succeeds after move.

---

### G8-B — Multi-process primary kill, new primary reads acknowledged data

**Purpose**: the core G8 pass gate.

**Component prelude**: `G8-B0` proves the data-continuity precondition before subprocess work: after acknowledged primary writes converge to a candidate, that candidate's local durable bytes are byte-equal. It also includes the negative oracle that authority movement alone is not G8 success.

**Shape**:

```text
start blockmaster + two blockvolume processes
write known data to primary
confirm replica has acknowledged/durable data or recovery can close
kill primary process
wait for master to publish new primary
reattach/reconnect client
read from new primary
assert byte-equal
```

**Level**: L2/L3 process/hardware.

**Pass**: byte-equal read from new primary after old primary failure.

**Fail**: only observes assignment moved, or read is not checked.

---

### G8-C — Old primary returns stale and is fenced

**Purpose**: prevent split-brain-ish success after old primary restarts or old frontend remains attached.

**Shape**:

```text
after G8-B reassignment
old primary process returns or stale backend remains reachable
old path read -> fail-closed
old path write -> fail-closed
new primary remains authoritative
```

**Level**: component first, then process/hardware if feasible.

**Important non-claim**: `Healthy=false` on the returned old primary proves frontend fail-closed, not that the process has become a ready supporting replica. A returned old primary should eventually flow through:

```text
Observed -> FrontendClosed/Superseded -> ReplicaCandidate -> Syncing/Rebuilding -> ReplicaReady
```

G8-C binds the first two states. Claiming `ReplicaCandidate` or `ReplicaReady` requires additional progress/peer-set evidence and may be pulled into a follow-up slice only if explicitly ratified.

---

### G8-D — Primary kill during active rebuild

**Purpose**: consume the G7 non-claim: primary-kill mid-rebuild was explicitly deferred to G8.

**Shape**:

```text
start rebuild session
kill primary before rebuild completes
system either:
  A. continues from new primary and converges, or
  B. fails cleanly with explicit unsupported evidence
```

**Initial disposition**: OUT for first G8 close by default. This becomes `G8b` only if architect explicitly pulls it in after G8-0/A/B/C are green. If deferred, G8 close must state the exact consequence and owner.

---

### G8-E — Client reconnect semantics

**Purpose**: bind how clients find the new primary.

**Allowed paths**:

1. explicit reconnect / reattach through test harness;
2. iSCSI/NVMe reconnect if available;
3. frontend-harness reconnect if OS initiator path is not yet stable.

**Rule**: G8 must name the selected path. It cannot leave "client reconnects somehow" implicit.

---

## 5. V2 Port / Reuse Plan

| V2 asset | Disposition |
|---|---|
| `ha-io-continuity.yaml` | PORT scenario shape / oracle. |
| `ha-failover.yaml` | PORT scenario steps where they do not rely on V2 authority. |
| `ha-full-lifecycle.yaml` | Read for future G9/G15 dogfood; likely too broad for first G8. |
| HA component tests under `weed/storage/blockvol/test/` | PORT tests that assert data continuity and stale fencing; REBIND authority setup to V3 publisher. |
| V2 promote/demote RPC | PERMANENT SKIP. Violates V3 authority model. |
| heartbeat-as-authority | PERMANENT SKIP. |

G8 should port scenario oracles aggressively, not V2 authority semantics.

---

## 6. TDD Plan

### 6.1 Red tests before production changes

0. `TestG8_0_TopologyController_FailoverRequiresReadyEligibleReachableCandidate`
   - Authority-only eligibility and publisher-minted epoch precondition (`G8-0`).

1. `TestG8A_SupersedeFact_FencesOldPrimaryDurableBackend`
   - Component-level stale read/write.

2. `TestG8B0_NewPrimaryCandidate_ReadsAcknowledgedWritesAfterConvergence`
   - Component-level data continuity after convergence; prelude to process-level G8-B.

3. `TestG8_FailoverCannotCloseOnAuthorityMoveOnly`
   - Negative test: assignment movement without data oracle is not G8 success.

4. `TestG8_ProcessKillPrimary_NewPrimaryByteEqual`
   - Multi-process or hardware scenario.

5. `TestG8_OldPrimaryReturn_StalePathRejected`
   - Component first; hardware follow-up if feasible.

### 6.2 Test gates

| Layer | Required for first G8 close? |
|---|---|
| Unit / component stale fence | Required. |
| Component data continuity | Required. |
| Multi-process primary kill -> new primary byte-equal | Required. |
| OS initiator reconnect | Preferred; may be provisional if explicitly non-claimed and G15a/G17 dogfood owns it. |
| Primary kill mid-rebuild | Recommended; may be split into G8b if too broad. |

---

## 7. Implementation Audit Checklist

Before production code, sw performs read-only audit:

1. Where does master decide new primary after failure?
2. What evidence marks old primary unreachable?
3. How does `cmd/blockvolume` update frontend projection after new assignment?
4. Can the old primary still serve an already-open iSCSI/NVMe/backend path?
5. Does the candidate new primary have durable acknowledged bytes before serving?
6. What code path proves client reconnect / reattach?
7. Which V2 scenario files provide the closest oracle?

Audit output must classify:

```text
PROCEED-verify-only
PROCEED-minor-patch
HALT-scope-evolution
```

### 7.1 Current audit snapshot (2026-05-02)

| Area | Current code fact | Verdict |
|---|---|---|
| Authority failover decision | `TopologyController.decideVolume` emits `IntentReassign` when current candidate is unacceptable and another candidate is `Reachable && ReadyForPrimary && Eligible && !Withdrawn`. | `PROCEED-verify-only` |
| Epoch minting | `Publisher.apply(IntentReassign)` advances per-volume epoch from max prior volume epoch; local volume/host code does not mint. | `PROCEED-verify-only` |
| Assignment fan-out | `master.SubscribeAssignments` is volume-scoped and fans in all replica slots, allowing old primary to observe a newer cross-replica line. | `PROCEED-verify-only` |
| Old-primary fence | `Host.recordOtherLine -> IsSuperseded -> AdapterProjectionView` turns local Healthy into frontend `Healthy=false`; durable backend maps that to `ErrStalePrimary`. | `PROCEED-verify-only` |
| Candidate data continuity | Process-level G8-B now writes through r1 iSCSI, kills r1, waits r2 failover, reconnects to r2 iSCSI, and reads byte-equal acknowledged data. | `PROCEED-verify-only` |
| Client reconnect / reattach | First G8 close uses explicit test-harness iSCSI reconnect to the new primary target. This is not a claim of transparent OS initiator reconnect. | `PROCEED-verify-only` |
| Primary kill during active rebuild | Crosses G7 recovery + G8 failover; not required for first close by default. | `HALT-scope-evolution` for first G8; split to `G8b` |

Expected first-close default: `PROCEED-minor-patch`, because G8 needs process integration wiring/tests rather than new recovery primitives.

---

## 8. Acceptance Criteria

G8 closes only when all first-close criteria are true:

1. G8-A component stale read/write fence is GREEN.
2. G8-B multi-process or hardware primary-kill scenario is GREEN.
3. The scenario verifies byte-equal acknowledged data on the new primary.
4. The old primary stale path is fail-closed in at least component coverage.
5. G8 close report names the client reconnect / reattach path used.
6. G8 close report names all non-claims, especially OS initiator behavior if not fully exercised.
7. No V2 authority-minting semantics are ported.

---

## 9. Non-Claims For First G8 Close

Unless explicitly expanded during §1.A ratification, first G8 close does not claim:

1. multi-master control-plane HA;
2. RF>=3 quorum/min-pin policy;
3. rack/AZ placement;
4. transparent OS initiator reconnect for all protocols;
5. primary crash during every possible recovery sub-phase;
6. performance/failover SLO;
7. Kubernetes workload failover under CSI.

These are forward-carry unless pulled into G8 by architect before code.

---

## 10. Initial §1.A Questions For Architect Ratification

| Question | Default recommendation |
|---|---|
| Q1: topology | RF=2 subprocess L2 with real product daemons and real loopback iSCSI is sufficient for first G8 close. m01/M02 cross-node confirmation is forward-carry; RF>=3 later. |
| Q2: frontend path | Use backend/harness reconnect for first G8; OS initiator reconnect preferred if available but not required for first data-continuity close. |
| Q3: primary-kill mid-rebuild | OUT for first G8 close by default; split to G8b unless architect explicitly pulls it in. |
| Q4: timing | Record dispatch/reassignment/reconnect time; no SLO. |
| Q5: old primary return | Component required; hardware preferred if harness can restart old primary cleanly. |

Architect decision can override these defaults before sw starts code.

---

## 11. Sequence Followed

1. sw read current failover / publisher / projection / frontend stale-fence code.
2. sw wrote the G8 audit table against §7.1.
3. sw landed `G8-0` / `G8-A` / `G8-B0` component tests before process-level close evidence.
4. G8-B process evidence landed through real product daemons and explicit iSCSI reconnect; see §12.
5. Architect §1.A ratification locked the first-close topology and non-claims on 2026-05-02.
6. Remaining hardware and wider failure-matrix work moves through §12.6 forward-carry, not hidden first-close scope.

---

## 12. Close-Ready Evidence Snapshot

### 12.1 Code evidence

Code branch: `seaweed_block:p15-g8/failover-data-continuity`

| Commit | Evidence |
|---|---|
| `292db26` | G8-0/A/B0 component tests: authority failover eligibility, old-primary stale fence, component data-continuity prelude, and authority-move-only negative oracle. |
| `b01bb9e` | G8-B process role movement: r1 primary kill -> r2 becomes `Healthy=true` at `r2@epoch>=2` through real `cmd/blockmaster` / `cmd/blockvolume` subprocesses. |
| `945942d` | G8-C process old-primary return: r1 restarts after r2 failover and remains frontend non-Healthy while r2 stays primary. |
| `6669cf5` | G8-B process data continuity: r1 iSCSI acknowledged write -> kill r1 -> r2 iSCSI reconnect -> byte-equal read from new primary. |

### 12.1.1 Hardware-fidelity choice

First G8 close deliberately uses subprocess L2 rather than m01/M02 cross-node hardware:

```text
real cmd/blockmaster process
real cmd/blockvolume r1 + r2 processes
real walstore-backed durable roots
real iSCSI target endpoints
explicit iSCSI reconnect from r1 to r2
byte-equal SCSI READ(10) after primary kill
```

This is sufficient for the first data-continuity close because G8's claim is authority-to-frontend failover correctness, not network partition timing, transparent initiator reconnect, or multi-host deployment behavior. m01/M02 confirmation remains forward-carry for the next hardware-fidelity pass.

### 12.2 Selected reconnect path

First G8 close uses explicit iSCSI reconnect in the Go subprocess harness:

```text
dial r1 iSCSI -> WRITE(10) acknowledged
kill r1
wait r2 assignment / Healthy
dial r2 iSCSI -> READ(10) byte-equal
```

This proves the data-plane continuity primitive through a real frontend target in-process with the product daemons. It does not claim transparent kernel initiator reconnect or multipath behavior.

### 12.3 Tests run by sw

```powershell
go test ./cmd/blockvolume -run "TestG8|TestG54_BinaryWiring" -count=1
go test ./cmd/blockvolume ./core/authority ./core/host/volume ./core/frontend/durable ./core/replication/component -count=1
```

Both passed on 2026-05-02.

### 12.4 First G8 close claim

G8 can claim:

```text
V3 can move primary service after a primary process kill in an RF=2 product-daemon setup.
The new primary can read the tested acknowledged data written through the healthy replication path before failure.
The old primary, if it returns, remains frontend fail-closed and does not regain authority.
```

### 12.5 First G8 non-claims

G8 does not claim:

1. transparent OS/kernel initiator reconnect;
2. CSI/Kubernetes failover under workload;
3. primary kill during active rebuild (`G8b`);
4. returned old primary has become `ReplicaReady`;
5. RF>=3 quorum/min-pin policy;
6. rack/AZ placement;
7. multi-master control-plane HA;
8. failover time SLO.
9. strict RF=2 full-ack/quorum write semantics when the replica is lagging, down, or unable to durably ACK.

### 12.6 Forward-carry

| Item | Owner |
|---|---|
| Returned old primary -> `ReplicaCandidate` -> syncing/rebuild -> `ReplicaReady` | G8-followup / G9A reintegration policy |
| Primary kill during active rebuild | G8b |
| Transparent OS initiator reconnect / multipath behavior | G15a/G17 dogfood or dedicated frontend gate |
| m01/M02 cross-node G8 confirmation | G8-followup / hardware-fidelity pass |
| Control-plane placement intent and plan/result split | G9/G9A |
| State vocabulary in diagnostics (`frontend_ready`, `authority_role`, `replication_role`, progress) | G17-lite |
| ACK profile policy: `best-effort` vs quorum/full-ack, and user-visible mode naming | G8-followup / G9A |

---

## 13. Close

### 13.1 Close decision

G8 is closed on 2026-05-02.

Architect single-sign basis:

1. §1.A ratification accepts RF=2 subprocess L2 with real product daemons and real loopback iSCSI as first-close evidence.
2. QA verified the close packet after governance fixes and signed the evidence on 2026-05-02.
3. Code evidence is pinned to `seaweed_block:p15-g8/failover-data-continuity` through `6669cf5`.
4. The close claim is limited to authority-to-frontend failover data continuity under the tested healthy replication path.

### 13.2 Evidence accepted

Accepted evidence:

1. `292db26` — authority failover eligibility, stale-fence component coverage, data-continuity prelude, and authority-move-only negative oracle.
2. `b01bb9e` — process primary kill causes r2 to become `Healthy=true` at a newer epoch.
3. `945942d` — returned old primary remains frontend fail-closed.
4. `6669cf5` — real iSCSI `WRITE(10)` ACK on r1, kill r1, explicit reconnect to r2, real iSCSI `READ(10)` byte-equal.

Accepted test commands:

```powershell
go test ./cmd/blockvolume -run "TestG8|TestG54_BinaryWiring" -count=1
go test ./cmd/blockvolume ./core/authority ./core/host/volume ./core/frontend/durable ./core/replication/component -count=1
```

### 13.3 Final G8 claim

G8 proves:

```text
In an RF=2 product-daemon setup, V3 can move primary service after killing the current primary.
The new primary can read the tested data acknowledged through the old primary's healthy replication path.
The old primary, if restarted, remains frontend fail-closed and does not regain authority.
```

### 13.4 Final non-claims

G8 does not claim:

1. transparent kernel initiator reconnect or multipath behavior;
2. m01/M02 cross-node network-failure evidence;
3. primary kill during active rebuild;
4. returned old primary has become `ReplicaReady`;
5. strict RF=2 full-ack/quorum semantics under lag or recovery;
6. RF>=3 quorum/min-pin policy;
7. rack/AZ placement;
8. multi-master control-plane HA;
9. failover time SLO;
10. CSI/Kubernetes workload failover.

### 13.5 Forward owner

Next owner is G8-followup/G9A:

1. ACK profile policy: best-effort vs full-ack/quorum.
2. Returned replica reintegration: `Observed -> ReplicaCandidate -> Syncing/Rebuilding -> ReplicaReady`.
3. m01/M02 cross-node confirmation.
4. Primary-kill-mid-rebuild split to G8b.
