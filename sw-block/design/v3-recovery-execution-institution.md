# Recovery Execution Institution

The recovery-execution institution is the bounded layer that turns
engine-issued recovery commands into real runtime lifecycle facts.
It owns session preparation, real execution start, cancellation,
timeout wiring, and terminal close callbacks. It does not own
recovery classification, failover decisions, topology authority,
or operator-facing publication.

This doc names what the institution owns, what it does not, and
what is explicitly carried forward to later phases.

## Files

| File | Role |
|---|---|
| `core/adapter/adapter.go` | Command preparation, lifecycle callback ingress, single route from engine commands to runtime execution |
| `core/adapter/executor.go` | `CommandExecutor` contract, including `SetOnSessionStart` and `SetOnSessionClose` |
| `core/adapter/normalize.go` | Runtime lifecycle facts to engine events: `SessionPrepared`, `SessionStarted`, `SessionClosed*` |
| `core/engine/apply.go` | Semantic lifecycle reducer for prepare / start / fail / complete / invalidate |
| `core/transport/executor.go` | Runtime session registry, conn ownership, invalidation, start/close signaling |
| `core/transport/catchup_sender.go` | Catch-up execution path; emits start only after real execution path begins |
| `core/transport/rebuild_sender.go` | Rebuild execution path; same lifecycle rule as catch-up |

## What the Institution Owns

1. **Command admission into execution** — engine-issued `StartCatchUp`,
   `StartRebuild`, and `InvalidateSession` become runtime actions
   through one adapter route.
2. **Prepared versus running split** — `SessionPrepared` is emitted
   when the adapter binds lineage and queues runtime work; `SessionStarted`
   is emitted only when the runtime actually begins execution.
3. **Bounded start timeout** — a prepared session that never reaches
   real execution is failed by the adapter's bounded start-timeout
   watchdog instead of remaining indefinitely in `starting`.
4. **Lifecycle callbacks** — the executor reports real start and
   terminal close through `SetOnSessionStart` and `SetOnSessionClose`.
5. **Session invalidation** — `InvalidateSession` removes the active
   session, closes its conn, and prevents stale delayed callbacks from
   becoming current truth.
6. **Fail-closed start / close behavior** — start failure, timeout,
   or invalidation must not fabricate `running` or `healthy`.
7. **Runtime-local ownership only** — the executor may own conn state,
   deadlines, and cancellation wiring, but not recovery policy.

## What the Institution Does NOT Own

The following belong to later phases. The recovery-execution institution
must not silently absorb them:

| Concern | Owner | Why not here |
|---|---|---|
| Choosing catch-up vs rebuild | engine | semantic policy, not execution lifecycle |
| Widening `targetLSN` or rewriting recovery meaning | engine | execution must honor the frozen command contract |
| Cross-session retry policy and backoff strategy | later P10 / P12 work | broader lifecycle policy than the current bounded institution |
| Live WAL streaming during rebuild | P10 / P12 | needs richer lifecycle orchestration than the current single-run route |
| Multi-replica coordination | P12 | replicated contract, not one replica's runtime lifecycle |
| Promotion / failover selection | P14 | topology authority |
| Epoch minting / endpoint authority | P14 | topology authority produced above the executor |
| Operator-facing health / repair controls | P15 | governance surface |

## Fail-Closed Contract

The current recovery-execution institution proves these boundaries:

| Scenario | Property proven |
|---|---|
| Session prepared but not yet started | projection stays `starting`; no fabricated `running` without a real start callback |
| Prepared session exceeds bounded start timeout | adapter emits `SessionClosedFailed(start_timeout)`; prepared does not hang forever |
| Immediate executor start error | adapter converts the failure into `SessionClosedFailed`; lifecycle fails closed without a `started` event |
| Delayed start after timeout | same-session `SessionStarted` is ignored once the session has already failed |
| Delayed success close after timeout | same-session `SessionClosedCompleted` is ignored; late success cannot revive a failed session |
| Delayed stale start callback after newer assignment | old `SessionStarted` is rejected; it cannot advance the new session |
| Delayed stale close callback after newer assignment | old `SessionClosed*` is rejected; it cannot make the new session healthy or failed |
| Transport dial failure | executor emits failed close but no `SessionStarted`; command issuance alone does not count as running |
| Session invalidation mid-run | invalidated session does not emit terminal close; runtime cancellation is semantically dead |

The key truth split is:

- `SessionPrepared` means the engine accepted one bounded recovery contract.
- `SessionStarted` means runtime execution actually began.
- `SessionClosedCompleted` / `SessionClosedFailed` are the only terminal lifecycle truth.

## Watchdog Guard Note

One non-blocking implementation note is important for future maintainers:

1. the adapter watchdog's read of engine state before firing is advisory, not authoritative
2. after that pre-check, engine state may still move before the watchdog calls back into the adapter
3. the final authoritative guard remains the engine's phase checks in `applySessionFailed`

This dual layer is intentional:

1. the watchdog should suppress obviously stale timeout fires where it can
2. the engine must remain the last semantic guard against late timeout/start/close races

Do not simplify this into one convenience check unless the replacement preserves
both properties explicitly.

## Reading Order

For someone new to this institution:

1. `core/adapter/executor.go` — lifecycle contract surface
2. `core/adapter/adapter.go` — command preparation and callback ingress
3. `core/engine/apply.go` — semantic lifecycle reducer
4. `core/transport/executor.go` — session registry and invalidation
5. `core/transport/catchup_sender.go` and
   `core/transport/rebuild_sender.go` — where real execution start is admitted
6. `core/adapter/adapter_test.go` and
   `core/transport/transport_test.go` — lifecycle proof set

## Carry-Forward

Items the recovery-execution institution leaves explicitly to later phases:

- **Later P10 work** — per-session timeout classification, retry/backoff policy,
  richer execution-phase observability, and live-WAL-during-rebuild orchestration.
- **P12 (bounded replicated failover contract)** — rejoin contract,
  multi-replica lifecycle coordination, and bounded takeover guarantees.
- **P14 (topology / failover policy)** — epoch authority, promotion,
  failover selection, and replica placement truth.
- **P15 (operator-facing governance surface)** — operator controls for
  retire / repair / rebalance and their publication semantics.
