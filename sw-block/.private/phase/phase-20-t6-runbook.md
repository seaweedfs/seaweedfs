# Phase 20 T6 Runbook

Date: 2026-04-06
Status: active

## Purpose

This runbook turns `Phase 20 T6` into an executable hardware-validation
program.

It is intentionally separate from `phase-20-test.md`.

`phase-20-test.md` defines the coverage matrix and staged closure model.
This file answers the operational questions:

1. what exact command surface exists today
2. which hardware scenarios should be run first
3. what must be observed during `Stage 0`
4. what is allowed before entering `V2` failover
5. which software-side QA tests should stay aligned while hardware work proceeds

## Sources Of Truth

Primary references:

1. `sw-block/.private/phase/phase-20.md`
2. `sw-block/.private/phase/phase-20-test.md`
3. `weed/storage/blockvol/testrunner/cmd/sw-test-runner/main.go`
4. `weed/storage/blockvol/testrunner/actions/devops.go`
5. `weed/server/volume_server_block_debug.go`
6. `weed/server/master_server_handlers_block.go`
7. `weed/command/master.go`

## T6 Runner Contract

### Real CLI Surface

Current runner entrypoint:

```bash
sw-test-runner run <scenario.yaml> [flags]
```

Important observation:

1. the current CLI exposes `run`, `validate`, `list`, `coordinator`, `agent`,
   and `console`
2. it does **not** visibly expose a `run --all` mode in
   `weed/storage/blockvol/testrunner/cmd/sw-test-runner/main.go`

Practical consequence:

1. `T6` must use an explicit scenario pack
2. `T7` full-suite wording should be interpreted as a scenario list or suite
   wrapper, not as an assumed built-in `--all` implementation

### Real Master Flag

The actual master CLI flag is:

```bash
--block.v2Promotion
```

This is defined in `weed/command/master.go`.

Use this spelling everywhere for `T6/T7`.

### How Promotion Mode Enters Runner-Launched Processes

`sw-test-runner` already supports passing arbitrary master and volume flags
through scenario YAML:

1. `start_weed_master.extra_args`
2. `start_weed_volume.extra_args`

That behavior is implemented directly in
`weed/storage/blockvol/testrunner/actions/devops.go`.

### Contract By Stage

#### Stage 0

No promotion-mode toggle required.

Goal:

1. close the bootstrap membership gap on real hosts

#### Stage 1

Use:

```bash
--block.v2Promotion=false
```

But because `block.v2Promotion` already defaults to `false`, existing Stage 1
scenarios can be used unchanged unless we want explicit traceability in copied
YAMLs.

Recommended policy:

1. keep Stage 1 on existing YAMLs
2. treat `V1` failover as the authority path
3. use `V2` surfaces only as observation / diagnosis

#### Stage 2

Use:

```bash
--block.v2Promotion=true
```

Stage 2 must not rely on hidden defaults.

Recommended policy:

1. use dedicated Stage 2 YAML copies or overlays
2. append `-block.v2Promotion=true` to `start_weed_master.extra_args`
3. keep the rest of the scenario unchanged where possible so V1/V2 results stay
   comparable

Do not hand-edit running commands outside the scenario definition.
Keep the toggle visible in scenario source or in a wrapper-generated temp copy.

## Stage 0 Bootstrap Closure Checklist

`Stage 0` is the current hard gate before meaningful `V2` failover hardware
validation.

The blocker observed on hardware is:

1. promoted primary still shows `ReplicaIDs=[]`
2. `RoleApplied=true`
3. `ShipperConfigured=false`
4. mode remains stuck before `publish_healthy`

### Required Observation Surfaces

#### VS-local debug surface

Use:

```bash
curl http://<volume-admin-host>:<volume-admin-port>/debug/block/shipper
```

Primary fields to read from each volume item:

1. `core_projection.replica_ids`
2. `shipper_configured`
3. `shipper_connected`
4. `publish_healthy`
5. `publication_reason`
6. `mode`
7. `role_applied`
8. `receiver_ready`
9. `executed_core_commands`
10. `projection_mismatches`

This surface is backed by `weed/server/volume_server_block_debug.go`.

#### Master volume surface

Use:

```bash
curl http://<master-host>:<master-port>/block/volume/<volume-name>
```

Primary fields to read:

1. `volume_server`
2. `epoch`
3. `volume_mode`
4. `engine_projection_mode`
5. `cluster_replication_mode`
6. `health_state`
7. `replicas`

This surface is backed by `weed/server/master_server_handlers_block.go`.

#### Master status surface

Use:

```bash
curl http://<master-host>:<master-port>/block/status
```

Use this for summary corroboration only:

1. `healthy_count`
2. `degraded_count`
3. `unsafe_count`
4. `failovers_total`
5. `promotions_total`

### Stage 0 Pass Criteria

Healthy RF2 path must show all of the following:

1. promoted primary `core_projection.replica_ids` is not empty
2. promoted primary `shipper_configured=true`
3. promoted primary reaches `publish_healthy=true`
4. promoted primary local `mode` reaches `publish_healthy`
5. master `engine_projection_mode` reflects the local serving truth
6. master `cluster_replication_mode` returns to a healthy cluster judgment
7. no persistent `projection_mismatches` remain for the healthy path

### Stage 0 Fail Criteria

Any one of the following keeps `Stage 0` open:

1. `ReplicaIDs=[]` on the healthy promoted primary path
2. `shipper_configured=false` after recovery to a supposedly healthy topology
3. `publication_reason` still explains a missing shipper / missing replica while
   the cluster is otherwise healthy
4. master says the cluster is healthy while the promoted primary still lacks
   replica membership
5. local mode stays `allocated_only` or `bootstrap_pending` after the topology
   should have converged

### Minimum Operator Loop

When iterating on the fix, record this sequence each run:

1. before failure: `block/volume/<name>` and `/debug/block/shipper`
2. immediately after failover: same two surfaces
3. after expected recovery window: same two surfaces again
4. note whether `ReplicaIDs`, `ShipperConfigured`, and `publish_healthy`
   converged together or diverged

## Stage 1 Scenario Pack

Stage 1 means:

1. failover authority stays on `V1`
2. `V2` surfaces must stay coherent and conservative
3. no semantic collapse is allowed between local, cluster, and legacy views

### Pack Definition

| Pack ID | Scenario | Why it is in Stage 1 |
|---|---|---|
| `P20-T6-H1A` | `weed/storage/blockvol/testrunner/scenarios/internal/recovery-baseline-failover.yaml` | primary death, auto-failover, data continuity, easiest baseline |
| `P20-T6-H1B` | `weed/storage/blockvol/testrunner/scenarios/internal/suite-ha-failover.yaml` | HA failover with real cluster lifecycle and post-failover health checks |
| `P20-T6-H1C` | `weed/storage/blockvol/testrunner/scenarios/cp11b3-manual-promote.yaml` | manual promote / preflight surface / publish recovery after rejoin |
| `P20-T6-H1D` | `weed/storage/blockvol/testrunner/scenarios/lease-expiry-write-gate.yaml` | confirms write-gate semantics stay intact while T6 work proceeds |

### Stage 1 Execution Rules

1. use existing YAMLs unchanged
2. do not enable `--block.v2Promotion=true`
3. capture `block/volume/<name>` before and after failover
4. capture `/debug/block/shipper` on both candidate servers during the run

### Stage 1 Must Prove

#### `P20-T6-H1A recovery-baseline-failover`

Must prove:

1. V1 auto-failover still succeeds
2. epoch advances
3. data remains readable after failover
4. V2 surfaces honestly show whether the promoted node is complete or still
   bootstrap-limited

#### `P20-T6-H1B suite-ha-failover`

Must prove:

1. HA failover path still works on the real cluster
2. `cluster_replication_mode` degrades conservatively after primary loss
3. post-failover `engine_projection_mode` does not get confused with cluster
   health

#### `P20-T6-H1C cp11b3-manual-promote`

Must prove:

1. promotion preflight and promote APIs remain diagnosable
2. restart / rejoin can return the cluster to `publish_healthy`
3. manual promote path still carries the expected data continuity guarantee

#### `P20-T6-H1D lease-expiry-write-gate`

Must prove:

1. lease gate semantics still work during T6 work
2. a seemingly healthy local target does not bypass lease safety

### Stage 1 Command Form

One scenario at a time:

```bash
sw-test-runner run weed/storage/blockvol/testrunner/scenarios/internal/recovery-baseline-failover.yaml --results-dir results/phase20-t6/stage1/recovery-baseline-failover
```

Preferred suite pack:

```bash
sw-test-runner suite weed/storage/blockvol/testrunner/suites/phase20-t6-stage1.yaml
```

Legacy compatibility wrapper:

```powershell
powershell -File weed/storage/blockvol/testrunner/scripts/run-phase20-t6.ps1 -Stage stage1
```

## Stage 2 Readiness Pack

Stage 2 means the system is ready to test real `V2` failover authority.

It is not just "same scenarios with one flag flipped."

### Hard Readiness Gates

Do not enter Stage 2 until all are true:

1. `Stage 0` bootstrap closure is passing on hardware
2. proto regeneration is complete for evidence transport
3. real evidence RPC is wired, not just a placeholder querier
4. master startup path can visibly enable `--block.v2Promotion=true`
5. operator surfaces can distinguish:
   - `disabled`
   - `placeholder_fail_closed`
   - `transport_ready`

### Stage 2 Scenario Set

| Pack ID | Scenario source | What it must prove |
|---|---|---|
| `P20-T6-H2` | Stage 1 failover baseline copied with `-block.v2Promotion=true` | durability-first selection on real hosts |
| `P20-T6-H3` | dedicated ambiguous-evidence scenario | missing / partial / stale evidence fails closed |
| `P20-T6-H4` | `v2-failover-gate.yaml` | promoted node stays gated until recovery truth allows serving |

### Stage 2 YAML Policy

Use dedicated Stage 2 copies or overlays for scenarios that start a master.

Required edit pattern:

1. preserve the original scenario flow
2. append `-block.v2Promotion=true` to `start_weed_master.extra_args`
3. do not fold the `V2` toggle into unrelated volume or target arguments

### Stage 2 Must Prove

1. fresh evidence, not heartbeat cache, decides promotion
2. higher `CommittedLSN` wins over nicer-looking health
3. partial evidence loss causes no promotion
4. ineligible promoted nodes do not serve
5. recovery can later re-enable serving when truth improves

## Stage 3 Compare Pack

Stage 3 compares the same scenario family under `V1` and `V2` failover.

Compare at least:

1. primary selected
2. epoch behavior
3. data continuity
4. `engine_projection_mode`
5. `cluster_replication_mode`
6. gate / no-serve behavior
7. whether divergence is explained by `V2` fail-closed semantics

## QA Alignment

Hardware validation should stay aligned with existing software-side QA tests.

| QA file | T6 relevance | Why it should stay in the loop |
|---|---|---|
| `weed/server/qa_block_cp11b3_adversarial_test.go` | preflight and promotion rejection surface | keeps failover rejection semantics pinned while Stage 2 transport is unfinished |
| `weed/server/qa_block_cp13_9_mode_test.go` | mode vocabulary and transitions | prevents local / cluster / legacy surfaces from drifting semantically |
| `weed/server/qa_failover_role_test.go` | auto-failover role handling, including different-path recovery | mirrors the kind of path-sensitive bug that can reappear on hardware |

Recommended T6 software companion run:

```bash
go test ./weed/server/ -run "TestQA_T6_|TestCP13_9_|TestAutoFailover_" -count=1
```

This does not replace hardware validation.
It keeps semantic guardrails pinned while hardware work proceeds.

## Immediate Start Order

1. run `Stage 0` observation loop on the current baseline
2. start fixing the replica-membership wiring gap until `Stage 0` closes
3. run the `Stage 1` pack on existing YAMLs
4. only after `Stage 0` is closed and evidence transport is real, prepare Stage 2 YAML copies

## What Not To Do

1. do not call `T6` started just because mode surfaces look richer
2. do not enter `--block.v2Promotion=true` runs while evidence transport is still placeholder-only
3. do not hide the promotion-mode toggle in ad hoc shell history
4. do not treat `run --all` as available unless the runner actually implements it
