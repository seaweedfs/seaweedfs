# V2 Protocol Truths

Date: 2026-03-30
Status: active
Purpose: record the compact, stable truths that later phases must preserve, and provide a conformance reference for implementation reviews

## Why This Document Exists

`FSM`, `simulator`, `prototype`, and `engine` are not a code-production pipeline.
They are an evidence ladder.

So the most important output to carry forward is not only code, but:

1. accepted semantics
2. must-hold boundaries
3. failure classes that must stay closed
4. explicit places where later phases may improve or drift

This document is the compact truth table for the V2 line.

Current chosen-envelope claims, accepted baselines, evidence mappings, and
evidence invalidations are tracked separately in
`v2-protocol-claim-and-evidence.md`.

## How To Use It

For each later phase or slice, ask:

1. does the new implementation remain aligned with these truths?
2. if not, is the deviation constructive or risky?
3. which truth is newly strengthened by this phase?

Deviation labels:

- `Aligned`: implementation preserves the truth
- `Constructive deviation`: implementation changes shape but strengthens the truth
- `Risky deviation`: implementation weakens or blurs the truth

## Core Truths

### T1. `CommittedLSN` is the external truth boundary

Short form:

- external promises are anchored at `CommittedLSN`, not `HeadLSN`

Meaning:

- recovery targets
- promotion safety
- flush/visibility reasoning

must all be phrased against `CommittedLSN`.

Prevents:

- using optimistic WAL head as committed truth
- acknowledging lineage that failover cannot preserve

Evidence anchor:

- strong in design
- strong in simulator
- strong in prototype
- strong in engine

### T2. `ZeroGap <=> ReplicaFlushedLSN == CommittedLSN`

Short form:

- zero-gap requires exact equality with committed truth

Meaning:

- replica ahead is not zero-gap
- replica behind is not zero-gap

Prevents:

- unsafe fast-path completion
- replica-ahead being mistaken for in-sync

Evidence anchor:

- strong in prototype
- strong in engine

### T3. `CatchUp` is bounded replay on a still-trusted base

Short form:

- `CatchUp = KeepUp with bounded debt`

Meaning:

- catch-up is a short-gap, low-cost, bounded replay path
- it only makes sense while the replica base is still trustworthy enough to continue from

Prevents:

- turning catch-up into indefinite moving-head chase
- hiding broad recovery complexity in replay logic

Evidence anchor:

- strong in design
- strong in simulator
- strong in prototype
- strong in engine

### T4. `NeedsRebuild` is explicit when replay is not the right answer

Short form:

- `NeedsRebuild <=> replay is unrecoverable, unstable, or no longer worth bounded replay`

Meaning:

- long-gap
- lost recoverability
- no trusted base
- budget violation

must escalate explicitly.

Prevents:

- pretending catch-up will eventually succeed
- carrying V1/V1.5-style unbounded degraded chase forward

Evidence anchor:

- strong in simulator
- strong in prototype
- strong in engine

### T5. `Rebuild` is the formal primary recovery path

Short form:

- `Rebuild = frozen TargetLSN + trusted base + optional tail + barrier`

Meaning:

- rebuild is not a shameful fallback
- it is the general recovery framework

Prevents:

- overloading catch-up with broad recovery semantics
- treating full/partial rebuild as unrelated protocols

Evidence anchor:

- strong in design
- strong in prototype
- strong in engine

### T6. Full and partial rebuild share one correctness contract

Short form:

- `full rebuild` and `partial rebuild` differ in transfer choice, not in truth model

Meaning:

- both require frozen `TargetLSN`
- both require trusted pinned base
- both require explicit durable completion

Prevents:

- optimization layers redefining protocol truth
- bitmap/range paths bypassing trusted-base rules

Evidence anchor:

- strong in design
- partial in engine
- stronger real-system proof still deferred

### T7. No recovery result may outlive its authority

Short form:

- `ValidMutation <=> sender exists && sessionID matches && epoch current && endpoint current`

Meaning:

- stale session
- stale epoch
- stale endpoint
- stale sender

must all fail closed.

Prevents:

- late results mutating current lineage
- changed-address stale completion bugs

Evidence anchor:

- strong in simulator
- strong in prototype
- strong in engine

### T8. `ReplicaID` is stable identity; `Endpoint` is mutable location

Short form:

- `ReplicaID != address`

Meaning:

- address changes may invalidate sessions
- address changes must not destroy sender identity

Prevents:

- reintroducing address-shaped identity
- changed-address restarting as logical removal + add

Evidence anchor:

- strong in prototype
- strong in engine
- strong in bridge P0

### T9. Truncation is a protocol boundary, not cleanup

Short form:

- replica-ahead cannot complete until divergent tail is explicitly truncated

Meaning:

- truncation is part of recovery contract
- not a side-effect or best-effort cleanup

Prevents:

- completing recovery while replica still contains newer divergent writes

Evidence anchor:

- strong in design
- strong in engine

### T10. Recoverability must be proven from real retained history

Short form:

- `CatchUp allowed <=> required replay range is recoverable from retained history`

Meaning:

- the engine should consume storage truth
- not test-reconstructed optimism

Prevents:

- replay on missing WAL
- fake recoverability based only on watermarks

Evidence anchor:

- strong in simulator
- strong in engine
- strengthened in driver/adapter phases

### T11. Trusted-base choice must be explicit and causal

Short form:

- `snapshot_tail` requires both trusted checkpoint and replayable tail

Meaning:

- snapshot existence alone is insufficient
- fallback to full-base must be explainable

Prevents:

- over-trusting old checkpoints
- silently choosing an invalid rebuild source

Evidence anchor:

- strong in simulator
- strong in engine
- strengthened by Phase 06

### T12. Current extent cannot fake old history

Short form:

- historical correctness requires reconstructable history, not current-state approximation

Meaning:

- live extent state is not sufficient proof of an old target point
- historical reconstruction must be justified by checkpoint + retained history

Prevents:

- using current extent as fake proof of older state

Evidence anchor:

- strongest in simulator
- engine currently proves prerequisites, not full reconstruction proof

### T13. Promotion requires recoverable committed prefix

Short form:

- promoted replica must be able to recover committed truth, not merely advertise a high watermark

Meaning:

- candidate selection is about recoverable lineage, not optimistic flush visibility

Prevents:

- promoting a replica that cannot reconstruct committed prefix after crash/restart

Evidence anchor:

- strong in simulator
- partially carried into engine semantics
- real-system validation still needed

### T14. `blockvol` executes I/O; engine owns recovery policy

Short form:

- adapters may translate engine decisions into concrete work
- they must not silently re-decide recovery classification or source choice

Meaning:

- master remains control authority
- engine remains recovery authority
- storage remains truth source

Prevents:

- V1/V1.5 policy leakage back into service glue

Evidence anchor:

- strong in Phase 07 service-slice planning
- initial bridge P0 aligns
- real-system proof still pending

### T15. Reuse reality, not inherited semantics

Short form:

- V2 may reuse existing Seaweed control/runtime/storage paths
- it must not inherit old semantics as protocol truth

Meaning:

- reuse existing heartbeat, assignment, `blockvol`, receiver, shipper, retention, and runtime machinery when useful
- keep `ReplicaID`, epoch authority, recovery classification, committed truth, and rebuild boundaries anchored in accepted V2 semantics

Prevents:

- V1/V1.5 structure silently redefining V2 behavior
- convenience reuse turning old runtime assumptions into new protocol truth

Evidence anchor:

- strong in Phase 07/08 direction
- should remain active in later implementation phases

### T16. Full-base rebuild completes against an explicit achieved boundary

Short form:

- `full_base` rebuild requires `AchievedLSN >= TargetLSN`
- engine and local runtime must converge to the same achieved boundary

Meaning:

- the engine plans a frozen minimum target `TargetLSN`
- the backend may produce an actual rebuilt boundary `AchievedLSN`
- exact-target extent equality is not required on a mutable-extent backend
- after install, `checkpoint`, `nextLSN`, receiver progress, flusher checkpoint, and engine-visible rebuild completion must all align to the same `AchievedLSN`

Prevents:

- local runtime truth advancing beyond engine/accounting truth
- rebuild completion at one boundary while storage/runtime state reflects another
- treating "at least target" as safe without making the newer achieved boundary explicit

Evidence anchor:

- strengthened by `Phase 09` backend execution closure work
- phase-level decision exists
- real-system proof still depends on executor/runtime alignment

### T17. Extent/WAL recovery split must be fixed before replay begins

Short form:

- `extent copy + WAL replay` is only correct if the split boundary is explicit and gap-free

Meaning:

- extent data must represent a known recovery boundary
- WAL replay must start from the matching next boundary
- if unflushed writes could otherwise fall between extent and replay, the backend must first fix the split boundary
- snapshot/CoW/bitmap export paths follow the same rule: transport must read from a stable recovery view, not a mutating mixed state

Prevents:

- missing writes between extent copy and replay
- point-in-time export that mixes two different recovery views
- rebuild correctness depending on timing accidents instead of an explicit boundary contract

Evidence anchor:

- present in rebuild correctness reasoning from V1
- strengthened by `Phase 09` full-base execution work
- future snapshot/bitmap-based paths must preserve it explicitly

## Current Strongest Evidence By Layer


| Layer                  | Main value                                                         |
| ---------------------- | ------------------------------------------------------------------ |
| `FSM` / design         | define truth and non-goals                                         |
| simulator              | prove protocol truth and failure-class closure cheaply             |
| prototype              | prove implementation-shape and authority semantics cheaply         |
| engine                 | prove the accepted contracts survive real implementation structure |
| service slice / runner | prove truth survives real control/storage/system reality           |


## Phase Conformance Notes

### Phase 04

- `Aligned`: T7, T8
- strengthened sender/session ownership and stale rejection

### Phase 4.5

- `Aligned`: T3, T4, T5, T10, T12
- major tightening:
  - bounded catch-up
  - first-class rebuild
  - crash-consistency and recoverability proof style

### Phase 05

- `Aligned`: T1, T2, T3, T4, T5, T7, T8, T9, T10, T11
- engine core slices closed:
  - ownership
  - execution
  - recoverability gating
  - orchestrated entry path

### Phase 06

- `Aligned`: T10, T11, T14
- `Constructive deviation`: planner/executor split replaced convenience wrappers without changing protocol truth
- strengthened:
  - real storage/resource contracts
  - explicit release symmetry
  - failure-class validation against engine path

### Phase 07 P0

- `Aligned`: T8, T10, T14
- bridge now makes stable `ReplicaID` explicit at service boundary
- bridge states the hard rule that engine decides policy and `blockvol` executes I/O
- real `weed/storage/blockvol/` integration still pending

## Current Carry-Forward Truths For Later Phases

Later phases must not regress these:

1. `CommittedLSN` remains the external truth boundary
2. `CatchUp` stays narrow and bounded
3. `Rebuild` remains the formal primary recovery path
4. stale authority must fail closed
5. stable identity must remain separate from mutable endpoint
6. trusted-base choice must remain explicit and causal
7. service glue must not silently re-decide recovery policy
8. reuse reality, but do not inherit old semantics as V2 truth
9. full-base rebuild must converge to one explicit achieved boundary
10. extent/WAL recovery split must be fixed before replay

## Review Rule

Every later phase or slice should explicitly answer:

1. which truths are exercised?
2. which truths are strengthened?
3. does this phase introduce any constructive or risky deviation?
4. which evidence layer now carries the truth most strongly?

## Phase Alignment Rule

From `Phase 05` onward, every phase or slice should align explicitly against this document.

Minimum phase-alignment questions:

1. which truths are in scope?
2. which truths are strengthened?
3. which truths are merely carried forward?
4. does the phase introduce any constructive deviation?
5. does the phase introduce any risky deviation?
6. which evidence layer currently carries each in-scope truth most strongly?

Expected output shape for each later phase:

- `In-scope truths`
- `Strengthened truths`
- `Carry-forward truths`
- `Constructive deviations`
- `Risky deviations`
- `Evidence shift`

## Phase 05-07 Alignment

### Phase 05

Primary alignment focus:

- T1 `CommittedLSN` as external truth boundary
- T2 zero-gap exactness
- T3 bounded `CatchUp`
- T4 explicit `NeedsRebuild`
- T5/T6 rebuild correctness contract
- T7 stale authority must fail closed
- T8 stable `ReplicaID`
- T9 truncation as protocol boundary
- T10/T11 recoverability and trusted-base gating

Main strengthening:

- engine core adopted accepted protocol truths as real implementation structure

Main review risk:

- engine structure accidentally collapsing back to address identity or unfenced execution

### Phase 06

Primary alignment focus:

- T10 recoverability from real retained history
- T11 trusted-base choice remains explicit and causal
- T14 engine owns policy, adapters carry truth and execution contracts

Main strengthening:

- planner/executor/resource contracts
- fail-closed cleanup symmetry
- cross-layer proof path through engine execution

Main review risk:

- executor or adapters recomputing policy from convenience inputs
- storage/resource contracts becoming approximate instead of real

### Phase 07+

Primary alignment focus:

- T8 stable identity at the real service boundary
- T10 real storage truth into engine decisions
- T11 trusted-base proof remains explicit through service glue
- T14 `blockvol` executes I/O but does not own recovery policy
- T16 full-base rebuild converges to one explicit achieved boundary
- T17 extent/WAL split boundary remains explicit and gap-free

Main strengthening:

- real-system service-slice conformance
- real control-plane and storage-plane integration
- diagnosable failure replay through the integrated path

Main review risk:

- V1/V1.5 semantics leaking back in through service glue
- address-shaped identity reappearing at the boundary
- blockvol-side code silently re-deciding recovery policy

## Future Feature Rule

When a later feature expands the protocol surface (for example `SmartWAL` or a new rebuild optimization), the order should be:

1. `FSM / design`

- define the new semantics and non-goals

1. `Truth update`

- either attach the feature to an existing truth
- or add a new protocol truth if the feature creates a new long-lived invariant

1. `Phase alignment`

- define which later phases strengthen or validate that truth

1. `Evidence ladder`

- simulator, prototype, engine, service slice as needed

Do not start feature implementation by editing engine or service glue first and only later trying to explain what truth changed.

## Feature Review Rule

For any future feature, later reviews should ask:

1. did the feature create a new truth or just strengthen an existing one?
2. which phase first validates it?
3. which evidence layer proves it most strongly today?
4. does the feature weaken any existing truth?

This keeps feature growth aligned with protocol truth instead of letting implementation convenience define semantics.

## 继承图

最短可以看成三层：

```text
Layer 1: 已接受的语义与证据
-----------------------------------------
Phase 08-13
  -> protocol truths
  -> claim / evidence
  -> envelope / non-claim
  -> bounded proofs
  -> V1-under-V2-constraints test results

                ||
                || 继承“语义约束、证明边界、不能 overclaim 的规则”
                \/

Layer 2: V2 core 原型与显式自动机
-----------------------------------------
Phase 14
  -> small automata
     - assignment
     - recovery
     - boundary
     - mode
     - publication
  -> explicit state owner
  -> transition rules
  -> command emission rules
  -> projection contracts

                ||
                || 继承“状态机结构、命令规则、对外投影规则”
                \/

Layer 3: 集成运行时
-----------------------------------------
Phase 15-16
  -> weed/ adapter
  -> blockvol executor
  -> master / VS / heartbeat / lookup / debug / status
  -> real command-driven path
  -> real observation feedback path

目标：
  V2 core owns semantics
  adapter translates
  blockvol executes
```

如果换成“什么被继承、什么不被继承”，就是这个图：

```text
前面 phase 资产
|
|-- 继承
|   |-- truth constraints
|   |-- FSM/state-transition meaning
|   |-- command emission conditions
|   |-- projection/publication rules
|   |-- accepted evidence and non-claims
|
|-- 不直接继承
|   |-- V1 runtime 的状态所有权
|   |-- V1 内联控制分支就是最终 truth
|   |-- “测试跑通了所以产品语义成立” 这种隐式结论
|
v
Phase 14 core
|
|-- 输出
|   |-- explicit core engine
|   |-- owned state
|   |-- explicit commands
|   |-- explicit projections
|
v
Phase 15-16 integration
|
|-- 复用
|   |-- blockvol flusher
|   |-- shipper / receiver
|   |-- rebuild server
|   |-- existing transport/backend machinery
|
|-- 替换/上提
|   |-- 谁决定 apply_role
|   |-- 谁决定 start_receiver
|   |-- 后面继续上提 configure_shipper 等 runtime-driving decision
```

再给你一个“现在走到哪里”的定位图：

```text
[Phase 08-13]
约束 / 证据 / envelope / prototype input
        |
        v
[Phase 14]
V2 core shell 成立
        |
        v
[Phase 15]
surface consume core truth
        |
        v
[Phase 16]
runtime-driving decisions move from adapter -> core
        |
        v
[未来]
one bounded V2-native runtime path
```

一句话总结：

- `Phase 08-13` 留下的是“规则和证据”
- `Phase 14` 把它们变成“显式 core”
- `Phase 15-16` 把 core 接到真实 runtime，上提语义控制权
- `V1` 主要保留为执行 backend，不再保留为语义 owner

## 逻辑
可以。我换成更“工程视角”的讲法，不只是图，而是讲这套继承到底怎么落到代码和后续开发上。

### 核心思路

这个项目现在不是一条线直接从“协议文档”走到“最终产品”，而是四层：

1. `truth / claim / envelope`
2. `core semantic model`
3. `adapter integration`
4. `backend execution reuse`

它们的关系不是谁替代谁，而是“上层定义意义，下层负责承载”。

最重要的一点是：

- 上层决定“什么算真”
- 下层只负责“怎么做出来”

这就是为什么我一直在强调：
`V2 owns semantics, V1/blockvol owns execution`

### 四层继承图

```text
Layer A: 语义约束层
--------------------------------
sw-block/design/v2-protocol-truths.md
sw-block/design/v2-protocol-claim-and-evidence.md
sw-block/design/v2-protocol-closure-map.zh.md

定义：
- 什么叫 assigned / ready / durable / degraded / publish healthy
- 哪些 claim 已经接受
- 哪些 envelope 是允许宣称的
- 哪些 non-claim 不能偷着扩张

                ||
                || 把“正确性的标准”传下去
                \/

Layer B: V2 core 语义层
--------------------------------
sw-block/design/v2_mini_core_design.md
sw-block/design/v2-phase14plus-semantic-framework.md
sw-block/engine/replication/

定义：
- 小自动机怎么拆
- state 谁拥有
- transition 怎么发生
- command 什么时候发
- projection 允许怎么对外表达

                ||
                || 把“标准”变成“可执行语义模型”
                \/

Layer C: Adapter 集成层
--------------------------------
weed/server/...
master / volume server / heartbeat / lookup / status / debug

职责：
- 把 live assignment / heartbeat / observation 送进 core
- 把 core 的 command / projection 接回真实系统
- 对外 surface 尽量只消费 core truth

                ||
                || 把 core 接到真实运行时
                \/

Layer D: Backend 执行层
--------------------------------
weed/storage/blockvol/...

职责：
- flusher
- shipper
- receiver
- rebuild server
- checkpoint / WAL / async execution

只负责：
- 执行
- 持久化
- 传输
- 提供 observation

不负责：
- 最终语义定义
```

### 每层分别继承了什么

#### 1. 从前面 phases 继承了什么

前面 `Phase 08-13` 不是白做的，它们沉淀了三类资产：

1. 语义定义
2. 证明边界
3. 已接受证据

比如：
- durable progress 该看什么，不该看什么
- publication healthy 不能等同于 replica ready
- degraded / needs_rebuild 不能乱宣称
- 哪些测试只是 witness，哪些已经是 proof
- 哪些 envelope 是当前 chosen path，不能擅自放大成“通用产品能力”

所以现在做 `Phase 15/16`，不是重新设计世界，而是在问：

- 这个 live path 是否遵守了已经接受的 truth？
- 这个 adapter 是否把语义 owner 放错地方了？
- 这个 surface 是否产生 overclaim？

#### 2. `Phase 14` 继承并固化了什么

`Phase 14` 的价值，不是“写了一个新包”，而是把前面松散的规则固化成 core 结构。

它固定了四件事：

1. state owner
2. transition rule
3. command emission rule
4. projection contract

这意味着后面就不是“看到一个 bug 再 patch 一层”，而是可以用统一问题来判断：

- 这是 state 问题？
- 这是 transition 问题？
- 这是 command 发错时机？
- 还是 projection overclaim？

这就是原来你说的那种目标：
“以后每个算法选择都能回答：满足哪个语义约束，避免哪个 overclaim，保住哪个 proof。”

#### 3. `Phase 15` 继承了什么

`Phase 15` 不是在发明新语义，而是在做一个很重要的事情：

让 `weed/` 的真实 surface 开始消费 core-owned truth。

也就是把过去散落在 adapter 里的这些东西逐步收回：

- debug
- heartbeat
- readiness snapshot
- master registry consume
- lookup/list/status outward surface

`Phase 15` 的主题不是 runtime control，而是 surface rebinding。

意思是：

- 先不急着改谁驱动执行
- 先把谁有资格对外说话这件事收紧

所以 `Phase 15` 更像是：
“让嘴巴先归 core 管”

#### 4. `Phase 16` 继承了什么

`Phase 16` 才开始继续向下推进，去拿“手脚”。

也就是从：

- core 只是解释系统状态
- adapter 还在自己决定很多执行动作

变成：

- core 发 command
- adapter 只是执行 command
- backend 执行后返回 observation
- core 再更新 state / projection

这就是为什么我刚刚在 `16A` 先把：

- `apply_role`
- `start_receiver`

这两条路径接成 command-driven。

这一步的意义不在于“功能变多了”，而在于 runtime ownership 往上提了。

### 一张更贴 repo 的图

```text
v2-protocol-truths.md
v2-protocol-claim-and-evidence.md
v2-protocol-closure-map.zh.md
        |
        v
v2_mini_core_design.md
v2-phase14plus-semantic-framework.md
        |
        v
sw-block/engine/replication/
  - state
  - event
  - command
  - projection
        |
        v
weed/server/
  - ApplyAssignments()
  - heartbeat consume / produce
  - lookup/list/status/debug
        |
        v
weed/storage/blockvol/
  - WAL
  - flusher
  - shipper
  - receiver
  - rebuild
```

可以把它理解成：

- 文档定义语义世界
- `engine/replication` 把语义世界程序化
- `weed/server` 把程序化语义接到 live system
- `blockvol` 提供真正的执行能力

### 为什么要这样分，而不是继续修 V1

这是你前面感觉最敏锐的地方：

如果一直在 `V1` 上补测试、补约束、补文档，最后容易变成：

- 我们越来越懂 `V1`
- 但没有真正得到一个 `V2` owner

那样后面每加一个功能都要重新回答：
“这次到底是谁说了算？”

现在这种分层，就是为了避免未来每做一次事都重新付出一大轮认知成本。

理想结果是：

- 新功能先接到 core 语义层
- adapter 只做接线
- backend 只做执行
- proofs 可以复用
- envelope 不需要每次重谈

也就是说，前期看起来“多做了一层”，但长期是为了让后面的功能开发更便宜。

### 你可以怎么理解现在的进度

如果用一句最直白的话：

- `Phase 08-13`：知道什么是对的，并拿到了很多边界证据
- `Phase 14`：把“什么是对的”变成显式 core
- `Phase 15`：让对外说法开始归 core
- `Phase 16`：让真实执行决策开始归 core

所以现在不是从零开始，也不是还在原地测 `V1`。
而是在把以前的知识资产，逐步变成真正可接管 runtime 的结构。

### 你后面看代码时可以用的判断法

你如果想快速判断某一段代码属于哪一层，可以问这三个问题：

1. 它是在定义“什么是真的”吗？
如果是，那它应该靠近 `design` 或 `engine/replication`

2. 它是在把外部输入翻译给 core，或者把 core 输出翻回系统吗？
如果是，那它应该在 `weed/server`

3. 它是在真正写 WAL、flush、ship、rebuild、listen socket 吗？
如果是，那它应该属于 `blockvol` 执行层，不该拥有最终语义

这个判断法很有用，因为以后 review 时会一直遇到“这段逻辑该放哪里”。

## 当前最关键的设计纪律

现在最需要守住的纪律其实就两条：

1. 不让 `V1` 本地状态重新成为 semantic owner
2. 不让新的 outward surface 绕开 core 再发明一套 truth

只要守住这两条，前面 phases 的资产就不是散掉，而是在持续被吸收到最终架构里。

如果你想，我下一条可以继续讲一个更具体的话题：

1. “五个小自动机分别管什么”
2. “为什么 `publication` 必须单独拥有 authority”
3. “为什么 `blockvol` 的异步线程还能保留，但 semantic ownership 不能保留”

你选一个，我可以继续往下展开。