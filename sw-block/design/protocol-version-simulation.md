# Protocol Version Simulation

Date: 2026-03-26
Status: design proposal
Purpose: define how the simulator should model WAL V1, WAL V1.5 (Phase 13), and WAL V2 on the same scenario set

## Why This Exists

The simulator is more valuable if the same scenario can answer:

1. how WAL V1 behaves
2. how WAL V1.5 behaves
3. how WAL V2 should behave

That turns the simulator into:
- a regression tool for V1/V1.5
- a justification tool for V2
- a comparison framework across protocol generations

## Principle

Do not fork three separate simulators.

Instead:
- keep one simulator core
- add protocol-version behavior modes
- run the same named scenario under different modes

## Proposed Versions

### `ProtocolV1`

Intent:
- represent pre-Phase-13 behavior

Behavior shape:
- WAL is streamed optimistically
- lagging replica is degraded/excluded quickly
- no real short-gap catch-up contract
- no retention-backed recovery window
- replica usually falls toward rebuild rather than incremental recovery

What scenarios should expose:
- short outage still causes unnecessary degrade/rebuild
- transient jitter may be over-penalized
- poor graceful rejoin story

### `ProtocolV15`

Intent:
- represent Phase-13 WAL V1.5 behavior

Behavior shape:
- reconnect handshake exists
- WAL catch-up exists
- primary may retain WAL longer for lagging replica
- recovery still depends heavily on address stability and control-plane timing
- catch-up may still tail-chase or stall operationally

What scenarios should expose:
- transient disconnects may recover
- restart with new receiver address may still fail practical recovery
- tail-chasing / retention pressure remain structural risks

### `ProtocolV2`

Intent:
- represent the target design

Behavior shape:
- explicit recovery reservation
- explicit catch-up vs rebuild boundary
- lineage-first promotion
- version-correct recovery sources
- explicit abort/rebuild path on non-convergence or lost recoverability

What scenarios should show:
- short gap recovers cleanly
- impossible catch-up fails cleanly
- rebuild is explicit, not accidental

## Behavior Axes To Toggle

The simulator does not need completely different code paths.
It needs protocol-version-sensitive policy on these axes:

### 1. Lagging replica treatment

`V1`:
- degrade quickly
- no meaningful WAL catch-up window

`V1.5`:
- allow WAL catch-up while history remains available

`V2`:
- allow catch-up only with explicit recoverability / reservation

### 2. WAL retention / recoverability

`V1`:
- little or no retention for lagging-replica recovery

`V1.5`:
- retention-based recovery window
- but no strong reservation contract

`V2`:
- recoverability check plus reservation

### 3. Restart / address stability

`V1`:
- generally poor rejoin path

`V1.5`:
- reconnect may work only if replica address is stable

`V2`:
- address/identity assumptions should be explicit in the model

### 4. Tail-chasing behavior

`V1`:
- usually degrades rather than catches up

`V1.5`:
- catch-up may be attempted but may never converge

`V2`:
- non-convergence should explicitly abort/escalate

### 5. Promotion policy

`V1`:
- weaker lineage reasoning

`V1.5`:
- improved epoch/LSN handling

`V2`:
- lineage-first promotion is a first-class rule

## Recommended Simulator API

Add a version enum, for example:

```go
type ProtocolVersion string

const (
    ProtocolV1  ProtocolVersion = "v1"
    ProtocolV15 ProtocolVersion = "v1_5"
    ProtocolV2  ProtocolVersion = "v2"
)
```

Attach it to the simulator or cluster:

```go
type Cluster struct {
    Protocol ProtocolVersion
    ...
}
```

## Policy Hooks

Rather than branching everywhere, centralize the differences in a few hooks:

1. `CanAttemptCatchup(...)`
2. `CatchupConvergencePolicy(...)`
3. `RecoverabilityPolicy(...)`
4. `RestartRejoinPolicy(...)`
5. `PromotionPolicy(...)`

That keeps the simulator readable.

## Example Scenario Comparisons

### Scenario: brief disconnect

`V1`:
- likely degrade / no efficient catch-up

`V1.5`:
- catch-up may succeed if address/history remain stable

`V2`:
- explicit recoverability + reservation
- catch-up only if the missing window is still recoverable
- otherwise explicit rebuild

### Scenario: replica restart with new receiver port

`V1`:
- poor recovery path

`V1.5`:
- background reconnect fails if it retries stale address

`V2`:
- identity/address model must make this explicit
- direct reconnect is not assumed
- use explicit reassignment plus catch-up if recoverable, otherwise rebuild cleanly

### Scenario: primary writes faster than catch-up

`V1`:
- replica degrades

`V1.5`:
- may tail-chase indefinitely or pin WAL too long

`V2`:
- explicit non-convergence detection -> abort / rebuild

## What To Measure

For each scenario, compare:

1. does committed data remain safe?
2. does uncommitted data stay out of committed lineage?
3. does recovery complete or stall?
4. does protocol choose catch-up or rebuild?
5. is the outcome explicit or accidental?

## Immediate Next Step

Start with a minimal versioned policy layer:

1. add `ProtocolVersion`
2. implement one or two version-sensitive hooks:
   - `CanAttemptCatchup`
   - `CatchupConvergencePolicy`
3. run existing scenarios under:
   - `ProtocolV1`
   - `ProtocolV15`
   - `ProtocolV2`

That is enough to begin proving:
- V1 breaks
- V1.5 improves but still strains
- V2 handles the same scenario more cleanly

## Bottom Line

The same scenario set should become a comparison harness across protocol generations.

That is one of the strongest uses of the simulator:
- not only "does V2 work?"
- but "why is V2 better than V1 and V1.5?"
