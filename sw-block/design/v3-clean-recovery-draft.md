# V3 Clean Recovery Draft

Date: 2026-04-09
Status: draft

## 1. Why V3

`V2` has already proven several important things:

1. the primary/replica recovery model is workable
2. rebuild and catch-up can be driven from bounded facts
3. master and primary roles can be separated more clearly than in the original `V1` logic
4. execution muscles in `weed/` can increasingly be treated as runtime adapters rather than semantic authority

But `V2` also exposed a class of problems that should not be fixed forever by local patches:

1. heartbeat timing affects recovery behavior too much
2. transport failures are too easily over-interpreted as semantic failures
3. event ordering still changes recovery paths in some cases
4. completion/failure authority can split across ack arrival, executor unwind, and projection updates
5. engine changes can drift toward workaround states and temporary semantics

This draft proposes a `V3` direction that is **not** a feature expansion.

`V3` is a semantic cleanup effort:

1. reduce time-polluted semantics
2. minimize engine authority to the true problem space
3. keep execution in runtime/backend muscles
4. establish a strict audit rule for future engine changes

## 2. Core Thesis

`V3` should be built around one statement:

**Timers trigger observation. Facts determine semantics.**

Expanded:

1. timeouts, heartbeats, retry loops, and poll intervals may trigger a reconcile
2. they must not by themselves define recovery truth
3. recovery truth must come from bounded facts such as `epoch`, `endpointVersion`, `sessionID`, `R`, `S`, `H`, `DurableLSN`, and `AckKind`

Where:

1. `R` = replica achieved/applied boundary
2. `S` = primary retained start boundary
3. `H` = primary head/target boundary

## 3. Scope Of The Engine

The engine should be allowed to grow only when the problem space itself requires it.

The question is not whether the engine is 500 lines or 2500 lines.
The question is whether the engine still only solves semantic problems.

### 3.1 Engine owns semantic truth

Engine-owned truth domains:

1. identity truth
2. reachability truth
3. recovery truth
4. session validity truth
5. durability/publication truth
6. outward projection

### 3.2 Engine does not own execution details

The following must stay outside the semantic engine:

1. TCP connection management
2. retry sleep durations
3. heartbeat polling cadence
4. goroutine scheduling and drain details
5. block install mechanics
6. WAL streaming mechanics
7. storage backend quirks
8. log ordering and transport timing workarounds

## 4. Truth Domains

`V3` should explicitly separate several truth domains that are too easy to mix.

### 4.1 Identity Truth

Authority: `master`

Contains:

1. who is primary
2. which replica slots exist for the volume
3. which `replicaID` occupies each slot
4. current endpoint for each replica
5. `endpointVersion`
6. `epoch`

Meaning:

1. who belongs to the roster
2. where they are expected to be reached
3. which version of membership is authoritative

### 4.2 Reachability Truth

Authority: `primary`

Contains:

1. reachable / unreachable / probing
2. last successful contact
3. last probe result
4. last endpoint attempted

Meaning:

1. can the primary currently talk to this replica transport
2. should it keep probing

It does **not** determine recovery type by itself.

### 4.3 Recovery Truth

Authority: `primary`

Contains:

1. replica boundary `R`
2. primary retained start `S`
3. primary head or target `H`
4. recovery decision:
   1. no recovery
   2. catch-up
   3. rebuild
   4. undetermined

Meaning:

1. which semantic recovery path is required
2. why that path was chosen

### 4.4 Session / Execution Truth

Authority: recovery executor/session owner

Contains:

1. active `sessionID`
2. session kind
3. target boundary
4. achieved boundary
5. running / failed / completed

Meaning:

1. which recovery contract is active
2. whether that contract actually closed successfully

### 4.5 Projection

Authority: derived only

Contains operator-facing state such as:

1. `publish_healthy`
2. `degraded`
3. `needs_rebuild`
4. `recovery_in_progress`

Projection is not semantic authority.
It may report truth, but it must never create truth.

## 5. Role Split

### 5.1 Master

Master is responsible for:

1. liveness of volume servers at topology level
2. leader failover and primary assignment
3. replica roster membership
4. replica endpoint refresh when a VS joins or changes address

Master is not responsible for:

1. deciding catch-up vs rebuild
2. executing recovery sessions
3. interpreting data-plane progress

### 5.2 Primary

Primary is responsible for:

1. transport connectivity to replicas
2. collecting recovery facts
3. deciding keep-up / catch-up / rebuild
4. executing recovery
5. re-integrating a replica into the active data path

Primary is not responsible for:

1. inventing identity truth
2. deciding the replica roster independently of master

## 6. Minimal Principles

These principles should be treated as normative.

### P1. Timers trigger, facts decide

1. heartbeat timeout may trigger roster reconsideration
2. transport timeout may trigger a probe
3. ack timeout may trigger a failure or retry path
4. but none of these timers define recovery truth directly

### P2. Single authority per truth domain

1. identity truth belongs to `master`
2. recovery truth belongs to `primary`
3. terminal session success/failure belongs to the session/executor close path
4. durability truth belongs to barrier/quorum proof

### P3. Projection is derived only

1. `healthy`, `degraded`, and `needs_rebuild` are reports
2. they must not be used as hidden control inputs

### P4. Recovery choice must be deterministic

For the same bounded facts, the system must choose the same recovery path regardless of event order.

Examples:

1. if `R >= H`, do not recover
2. if `R >= S` and `R < H`, catch-up
3. if `R < S`, rebuild

### P5. Monotonic facts win

1. old `epoch` must never override new `epoch`
2. old `sessionID` must never override new `sessionID`
3. old `endpointVersion` must never override new endpoint truth
4. old completion/failure signals must never roll back newer semantic truth

### P6. Transport failure is not semantic overreach

1. ship/barrier/probe failure may mark reachability loss
2. it must not by itself decide rebuild
3. rebuild requires bounded recovery facts

### P7. Execution is a muscle

1. streaming blocks
2. replaying WAL
3. applying blocks
4. wiring sessions

These are implementation muscles, not semantic authority.

### P8. Engine size is not the target

1. the engine may grow if the problem space requires it
2. a larger engine is acceptable if it remains semantically clean
3. a smaller engine is not better if it hides complexity in ad-hoc workarounds

## 7. Anti-Patterns

The following patterns are explicitly rejected in `V3`.

### A1. Heartbeat timing defines recovery semantics

Examples:

1. missed heartbeat means rebuild is now required
2. reconnect heartbeat timing decides whether recovery occurs at all

Rejected because:

1. heartbeat is identity observation latency, not data truth

### A2. Barrier failure directly defines rebuild

Examples:

1. one failed barrier implies `needs_rebuild`
2. one transport error implies semantic replacement

Rejected because:

1. transport loss and recovery type are different truth domains

### A3. Ack arrival defines terminal success

Examples:

1. `SessionAckCompleted` immediately becomes final success even if executor unwind is not complete

Rejected because:

1. observed completion is not the same as fully closed recovery authority

### A4. Event ordering determines semantics

Examples:

1. roster refresh arrives before probe => one path
2. probe arrives before roster refresh => another path

Rejected because:

1. same facts must produce same decision

### A5. Projection drives control truth

Examples:

1. outward mode remains healthy so recovery is deferred
2. `needs_rebuild` projection doubles as internal transport state

Rejected because:

1. projection must remain derived only

### A6. Timing-specific workaround states accumulate in the engine

Examples:

1. one-off states created to survive a single race
2. special enums that exist only because one timer arrived before another

Rejected because:

1. this turns the engine into a patch history rather than a semantic model

## 8. Engine Change Audit

Every engine change in `V3` should pass an audit.

### 8.1 Required questions

1. does this change introduce a new semantic truth, or only an implementation workaround
2. if it is a workaround, why is it not staying in runtime/adapter/executor
3. does this change introduce a new authority conflict
4. does this change make protocol meaning depend on timer behavior or event order
5. can the same effect be expressed by existing state dimensions or existing events
6. what invariant does this change preserve or introduce

### 8.2 Default stance

1. new state is rejected by default
2. new event is rejected by default
3. new semantic transition is rejected by default

The burden of proof is on the change.

### 8.3 Acceptance bar for a new state

A new state may enter the engine only if all are true:

1. it expresses a new semantic distinction
2. that distinction cannot be represented by existing orthogonal dimensions
3. it is not timer-driven
4. it is not transport-detail-driven
5. it supports a new invariant that can be stated clearly

### 8.4 Acceptance bar for a new event

A new event may enter the engine only if all are true:

1. it represents a new independent fact source
2. it cannot be modeled as parameters of an existing event category
3. it does not create dual authority
4. it is not introduced only to compensate for runtime ordering

## 9. V2 Versus V3 Example Cases

### Case 1: Same-address transient replica restart

Problem in time-polluted systems:

1. if master does not observe the disconnect in time, recovery may not restart

`V3` expectation:

1. primary marks reachability lost
2. primary keeps probing last known endpoint
3. if same address returns, probe succeeds
4. bounded facts decide catch-up or rebuild
5. master refresh is helpful but not required for same-address recovery

### Case 2: Changed-address restart

Problem in time-polluted systems:

1. primary keeps probing old endpoint forever
2. event ordering between heartbeat and probe changes semantics

`V3` expectation:

1. primary keeps probing old endpoint harmlessly
2. master observes new VS heartbeat and issues newer endpoint truth
3. primary adopts newer endpoint version
4. recovery proceeds from refreshed identity truth

### Case 3: Rebuild completion race

Problem in time-polluted systems:

1. completion ack arrives
2. core announces success
3. executor later fails during cleanup
4. system emits contradictory terminal outcomes

`V3` expectation:

1. completion ack updates observed fact only
2. terminal success is emitted only after executor/session closure

### Case 4: Heartbeat miss versus barrier failure ordering

Problem in time-polluted systems:

1. whichever loop notices first can drive different semantics

`V3` expectation:

1. master updates identity truth
2. primary updates reachability truth
3. primary computes recovery truth from bounded facts
4. event order changes convergence latency, not semantic result

## 10. Relationship To Current V2

This draft does not say `V2` should be discarded.

Instead, it provides:

1. a semantic target to evaluate future `V2` fixes
2. a way to distinguish durable improvements from local workaround accumulation
3. a future migration path if `V3` becomes an implementation effort

The intended use is:

1. future recovery changes should be checked against this draft
2. when a bug is found, first ask whether it is:
   1. a missing fact
   2. a wrong authority
   3. a time-polluted semantic rule
   4. a runtime muscle bug
3. only the first three belong in the semantic engine

## 11. Spec And Runtime Split

`V3` should prefer a split between:

1. a human-reviewable and machine-readable semantic spec
2. one or more runtime implementations

This is recommended because:

1. semantic truth should not be trapped inside one language implementation
2. future Go and Rust engines should be able to load and enforce the same recovery model
3. auditability improves when states, events, invariants, and case expectations are explicit data rather than implied only by code structure

### 11.1 What may be machine-readable

The following are good candidates for a machine-readable spec format such as `YAML`, `JSON`, or `TOML`:

1. truth domains
2. state vocabularies
3. event vocabularies
4. command vocabularies
5. projection field definitions
6. invariant declarations
7. transition legality tables
8. conformance cases and expected outcomes

These are suitable because they are:

1. declarative
2. reviewable by humans
3. useful across implementations

### 11.2 What must remain code

The following should remain in the engine runtime implementation:

1. fact merge logic
2. monotonic precedence rules
3. bounded recovery decision logic
4. stale rejection logic
5. terminal success/failure authority logic
6. any logic that would otherwise require embedding a mini-language in metadata

The goal is to avoid building a hidden `DSL` inside `YAML`.

`V3` should not become:

1. a metadata interpreter for arbitrary semantic logic
2. a configuration-driven workaround engine

### 11.3 Recommended layering

The preferred shape is:

1. `schema`
   1. event kinds
   2. state kinds
   3. command kinds
   4. projection kinds
2. `spec`
   1. invariants
   2. transition constraints
   3. prohibited patterns
   4. conformance examples
3. `runtime`
   1. applies events
   2. merges facts
   3. enforces invariants
   4. emits commands and projections
4. `adapter`
   1. bridges runtime to `weed/` execution muscles

### 11.4 Go And Rust Implication

If `V3` later has:

1. a Go semantic engine
2. a Rust semantic engine

then both should:

1. consume the same machine-readable spec
2. pass the same conformance cases
3. produce the same command/projection results for the same input facts

This gives the project:

1. a migration path without semantic drift
2. a way to benchmark a Rust loader/runtime against the established Go behavior
3. a stronger guarantee that semantics stay stable while execution internals evolve

### 11.5 Design rule

Machine-readable metadata is encouraged only for:

1. semantic specification
2. conformance specification
3. auditability

It is not encouraged for:

1. procedural recovery logic
2. retry logic
3. transport sequencing
4. execution-muscle behavior

The guiding rule is:

**metadata should describe the semantic surface, while code executes the semantic core.**

## 12. V3 Acceptance Criteria

`V3` is not acceptable merely because its internals are cleaner.

`V3` is acceptable only when it preserves the required semantic behavior and
eliminates the known timing-polluted failure classes.

### 12.1 Minimum replacement bar

The minimum operational bar for a `V3` engine/runtime pair is to pass the same
high-value scenarios that currently define the useful `V2` benchmark:

1. `I-V3`
2. `I-R8`
3. `fast-rejoin`
4. `rebuild-retry`

These scenarios are not optional polish. They are the minimum replacement gate.

### 12.2 Semantic acceptance bar

In addition to scenario pass/fail, `V3` must satisfy the following:

1. the same bounded facts produce the same recovery decision regardless of event order
2. transport failure alone does not choose rebuild
3. terminal recovery success is emitted from one authority only
4. terminal recovery failure is emitted from one authority only
5. projection never acts as hidden control input
6. partial roster observations cannot delete an active replica session without explicit newer authoritative truth

### 12.3 Conformance acceptance bar

For the same semantic spec and the same event stream:

1. the Go runtime and any future Rust runtime must produce the same commands
2. the Go runtime and any future Rust runtime must produce the same projections
3. stale events must be rejected consistently
4. monotonic facts must converge identically

### 12.4 Explicit failure classes that V3 must close

`V3` is not ready if it still permits these failure classes:

1. heartbeat timing decides whether recovery starts
2. one barrier/ship failure semantically escalates to rebuild
3. completion ack races with executor cleanup and creates double terminal outcomes
4. sender/session identity disappears during an active recovery contract
5. event arrival order changes recovery semantics instead of only latency

## 13. Non-Goals

This draft intentionally does **not** attempt to solve every future storage
problem at once.

### 13.1 Not a full system rewrite

This draft does not require:

1. immediate replacement of all `weed/` runtime code
2. immediate replacement of `blockvol`
3. immediate replacement of the existing transport stack

### 13.2 Not a feature expansion document

This draft does not require, as part of the clean core itself:

1. `SmartWAL`
2. `LBAMap`
3. richer rebuild source selection beyond the current clean recovery contract
4. new durability modes beyond the current semantic envelope

These may become future extensions, but they are not prerequisites for the
core `V3` semantic cleanup.

### 13.3 Not a timing optimization document

This draft does not itself optimize:

1. heartbeat intervals
2. probe cadence
3. retry backoff
4. control-plane wall-clock convergence

Those are valid later concerns, but `V3` first defines what the semantics must
be independent of those timings.

## 14. Migration Stance

`V3` should be introduced as a semantically cleaner implementation path, not as
an ideological fork from `V2`.

### 14.1 Benchmark stance

`V2` remains the operational benchmark until:

1. its known benchmark-blocking bugs are closed
2. its benchmark scenarios are green
3. it provides a stable behavior reference for `V3`

`V3` should not be judged against abstract elegance only. It should be judged
against a real, scenario-backed benchmark.

### 14.2 Side-by-side stance

When possible, `V3` should be introduced in a side-by-side conformance style:

1. same semantic spec
2. same test vectors
3. same scenario suite
4. same expected commands and projections

This keeps migration disciplined and prevents silent semantic drift.

### 14.3 Adapter-first stance

The preferred migration shape is:

1. keep runtime muscles in `weed/`
2. isolate a semantic engine runtime behind an adapter boundary
3. feed the same bounded facts into both benchmark and candidate implementations where practical
4. replace implementation internals only after semantic equivalence is proven

### 14.4 No workaround carry-forward rule

`V3` must not carry forward a `V2` workaround unless it survives the `V3`
audit as a genuine semantic requirement.

The default assumption is:

1. a timing workaround in `V2` is a candidate for deletion
2. not a default input to `V3`

## 15. Future Extensions And Extension Gate

`V3` should leave room for stronger data algorithms without letting them pollute
the clean recovery core.

### 15.1 Extension class boundary

Future extensions should be classified explicitly before implementation:

1. semantic core extension
2. recoverability-class extension
3. execution-muscle extension
4. storage-representation extension

Only the first class belongs in the semantic engine by default.

### 15.2 SmartWAL

`SmartWAL` should be treated as an algorithm/storage-representation extension,
not as a core recovery truth domain.

Its likely role:

1. reduce `WAL` payload tax for large writes
2. change how payload is represented or referenced
3. expand recoverability classes without changing identity, recovery authority, or completion authority

Its likely non-role:

1. changing who decides recovery
2. changing session ownership
3. changing projection semantics
4. changing the timer/fact boundary

### 15.3 LBAMap-assisted rebuild

`LBAMap` should be treated as a recoverability/execution extension:

1. it may improve rebuild source selection or rebuild efficiency
2. it may allow more precise block selection or reference resolution
3. it must not redefine the core truth domains

### 15.4 Extension gate

Any future feature such as `SmartWAL`, `LBAMap`, or richer rebuild algorithms
must answer these questions before entering the `V3` core:

1. does it create a new long-lived semantic truth
2. or does it only strengthen a recoverability class or execution muscle
3. does it require a new invariant
4. does it change the authority split
5. does it introduce timing-sensitive semantics

If the answer is mainly about:

1. payload representation
2. storage economics
3. replay efficiency
4. rebuild efficiency

then it should remain outside the semantic core unless proven otherwise.

### 15.5 Preferred extension path

The preferred order for future extensions is:

1. specify the extension in semantic terms
2. classify it by extension class
3. add conformance cases
4. implement it in execution/storage layers first if possible
5. only then decide whether the semantic core must expand

## 16. Immediate Implication

`V3` should be treated as:

1. a semantic minimization draft
2. a guardrail against engine pollution
3. a reference model for future case comparison

Not as:

1. an immediate rewrite mandate
2. a promise to replace all of `V2`
3. a feature roadmap

The value of this draft is that it gives the project a stable standard for deciding what belongs in the engine, what belongs in runtime muscles, and what should be rejected as timing pollution.
