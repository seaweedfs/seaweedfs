# Phase 20 Product Acceptance Checklist

Date: 2026-04-06
Status: active

## Reading

The system is architecture-complete but execution-incomplete. The V2
engine thinks like a product. The data plane still behaves partly like
a prototype. The gap is not features — it is end-to-end contract closure
from engine through write/flush/catch-up/barrier to data serve.

## Acceptance Matrix

### 1. Write / Durability Contract

| Item | Current state | Required for product | Blocks T6/T7? | Best test level |
|------|--------------|---------------------|---------------|-----------------|
| WriteLBA() guarantee is explicit | implicit (WAL append + maybe ship) | must document: write-back only, not durable | yes | contract doc + unit |
| SyncCache() = durability boundary | partially (barrier in group commit) | must be the single durability commit point | yes | component |
| sync_all observable truth | barrier in distributed sync path | barrier success = all replicas durable before return | yes | component |
| FUA / fdatasync boundary | exists in flusher | must be explicit in contract: which fsync is the commit fence | no | unit |
| WriteLBA returns before replication | yes (fire-and-forget ship) | must document: write != replicated, sync = replicated | yes | contract doc |

### 2. Fresh Replica Bootstrap

| Item | Current state | Required for product | Blocks T6/T7? | Best test level |
|------|--------------|---------------------|---------------|-----------------|
| Fresh replica entry condition | SetReplicaAddrs creates shipper | must have explicit session before live tail | yes | component |
| Catch-up target frozen | engine has FrozenTargetLSN | host must freeze target before live streaming | yes | component |
| Live stream start condition | phase gate blocks during active session | must verify: gate clears only after catch-up complete | yes | component |
| LSN gap prevention | phase gate prevents live tail | must also move backlog (bounded WAL catch-up) | yes | component |
| Timeout → replan or rebuild | not implemented | must classify: retry catch-up vs escalate to rebuild | yes | component |
| Retention loss → rebuild | not implemented | if WAL recycled during catch-up, must escalate | yes | component |

### 3. Host Observation Completeness

| Item | Current state | Required for product | Blocks T6/T7? | Best test level |
|------|--------------|---------------------|---------------|-----------------|
| ShipperConfiguredObserved | implemented | — | no | — |
| ShipperConnectedObserved | implemented | — | no | — |
| CatchUpCompleted | implemented | — | no | — |
| NeedsRebuildObserved | implemented | — | no | — |
| Replay progress observation | not centralized | must feed back as bounded progress events | no | component |
| Target reached observation | not implemented | must emit when catch-up reaches frozen target | yes | component |
| Timeout classification | not implemented | must distinguish "retry" from "must rebuild" | yes | component |
| Retention loss detection | exists in shipper (NeedsRebuild) | must feed back through protocol observation seam | no | component |
| Transport contact vs session complete | partially separated | must be strictly distinct in observation path | yes | component |

### 4. Serving / Publish / Durability Alignment

| Item | Current state | Required for product | Blocks T6/T7? | Best test level |
|------|--------------|---------------------|---------------|-----------------|
| publish_healthy requires | role + shipper + connected + durable>0 | must also require: no active recovery session, barrier confirmed | yes | component |
| Frontend serving requires | activation gate (T4) | must be derived from one protocol contract, not mixed signals | partially | integration |
| Replica durability observable | MinReplicaFlushedLSN on shipper group | must be the same boundary that publish_healthy reads | no | unit |
| bootstrap_pending → publish_healthy | requires ShipperConnected + barrier | must require: catch-up complete + barrier durable + no recovery active | yes | component |

### 5. Snapshot / Rebuild Convergence

| Item | Current state | Required for product | Blocks T6/T7? | Best test level |
|------|--------------|---------------------|---------------|-----------------|
| Rebuild entry condition | engine has StartRebuild | must match session contract (not ad-hoc) | no | component |
| Rebuild completion → host convergence | engine has RebuildCommitted | host must clear recovery state and re-enter normal protocol | no | integration |
| WAL catch-up → snapshot escalation | not implemented | must have explicit timeout/retention boundary | no | component |
| Snapshot/build under same protocol model | separate paths | should converge into same session kind dispatch | no | design doc |

### 6. Adapter Consistency

| Item | Current state | Required for product | Blocks T6/T7? | Best test level |
|------|--------------|---------------------|---------------|-----------------|
| RF=2 single-replica ServerID | missing in some paths | must carry stable identity in all adapter paths | yes | component |
| ReplicaID derivation | path/serverID convention | must be consistent across v2bridge, blockcmd, registry | no | unit |
| Assignment conversion edge cases | mostly covered | must handle: empty ReplicaAddrs, missing ServerID, role mismatch | no | unit |

### 7. Test Contract Alignment

| Item | Current state | Required for product | Blocks T6/T7? | Best test level |
|------|--------------|---------------------|---------------|-----------------|
| Tests treat WriteLBA as commit | some do | must not — WriteLBA is write-back only | yes | audit |
| Tests treat SyncCache as barrier | most do | correct — preserve this | no | — |
| Bootstrap tests cover bounded catch-up | partial (phase gate only) | must cover: freeze → catch-up → gate clear → live | yes | component |
| Contract matrix test | missing | one test per contract boundary: write / ship / flush / barrier / catch-up / publish | yes | component |

## Priority Order

### Must close before Stage 1 hardware validation

1. **Fresh bootstrap bounded catch-up** — the biggest execution gap.
   Move backlog under the phase gate contract. Freeze target, stream
   WAL, clear gate on completion.

2. **Write/durability contract doc** — make WriteLBA vs SyncCache
   guarantees explicit so tests and callers align.

3. **RF=2 adapter ServerID** — fix the identity leak so assignments
   carry complete replica identity.

4. **Test contract audit** — find and fix tests that treat WriteLBA
   as a commit boundary.

### Should close before Stage 2 hardware validation

5. **Timeout / retention-loss / escalation** — runtime failures feed
   back as explicit protocol observations.

6. **Target reached observation** — catch-up completion emits a
   distinct observation event.

7. **publish_healthy alignment** — derive from one protocol contract
   (catch-up complete + barrier durable + no recovery active).

### Can close after T6/T7

8. **Snapshot/build convergence** — fold into same protocol model.

9. **Replay progress observation** — bounded progress events for
   operator visibility during long catch-ups.

10. **Full contract matrix test** — one test per boundary.

## The One-Sentence Gap

The engine already knows the protocol. The execution body does not yet
fully obey it. Closing that gap is the difference between
"architecture-complete" and "product-complete."
