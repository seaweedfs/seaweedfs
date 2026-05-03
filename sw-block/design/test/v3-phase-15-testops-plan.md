# V3 Phase 15 TestOps Plan

**Date**: 2026-05-03
**Status**: DRAFT v0.3; V3 `internal/testops` contract skeleton implemented; first registered `go-test` scenario executable at `f51b79b`
**Scope**: end-to-end V3 test operations covering dev loop, gate close, regression, debug, hardware, K8s, chaos
**Code repo**: `seaweed_block` (V3 binaries / per-package go test)
**Docs repo**: `seaweedfs/sw-block/design/test/`
**Harness side**: `V:\share\v3-debug\` (Windows) = `/mnt/smb/work/share/v3-debug/` (Linux)
**Companion docs**:
- `v3-phase-15-g15a-qa-test-instruction.md` (non-priv L2)
- `v3-phase-15-g15a-privileged-qa-test-instruction.md` (priv L3)
- `v3-phase-15-g9g-qa-test-instruction.md` (G9G L2)

---

## 0. Product sentence

> *Dev and QA agents drive V3 verification through a single share-driven contract: submit a `run-request.json`, get a `result.json` plus a known-shape artifact directory, with no human round-trips for "where are the logs?" or "which commit was tested?". The pipeline scales from sub-second per-package tests up through privileged hardware and chaos, without forcing every gate to use every layer.*

This is **not** a product feature. It is the workflow infrastructure that makes
P15 gates ship faster and stay green.

---

## 1. Current state (2026-05-03)

### 1.1 What works today

| Layer | Tooling | Owner | Coverage |
|---|---|---|---|
| **L0 dev loop** | `go test ./pkg -count=1` | sw / dev agent | per-package, sub-second to ~2 min, no harness |
| **L1 component** | `go test` with `t.TempDir` + in-process subprocess | sw | G7/G8/G9*/G15a component tests |
| **L2 single-host subprocess** | `go test` spawning real `cmd/blockmaster` + `cmd/blockvolume` + `cmd/blockcsi` | sw + QA | G8 failover, G9G product loop, G15a non-priv (~120 s combined) |
| **L3 single-host privileged** | bash runner + Go test binary under `sudo`, persistent artifact dir on share | QA | G15a privileged (real `iscsiadm` + `mkfs` + `mount` + byte-equal write/read) |
| **L4 multi-host hardware** | bash scenarios on m01 + M02 over SSH, SMB-shared logs | QA | G7 (`V:\share\g5-test\scenarios-g7.sh`) |
| **L5 K8s** | none yet | — | forward-carry |
| **L6 chaos / soak** | none yet | — | forward-carry |

### 1.2 Existing harness artifacts

| Path | Owner | What it covers |
|---|---|---|
| `V:\share\g5-test\` | QA | G7 hardware harness: `scenarios-g7.sh`, `iterate-scale.sh`, `run-g7.sh`, `scenarios/g7-helpers.sh` |
| `V:\share\g15a-priv\` | QA | G15a privileged: `privileged_test.go`, `run-g15a-privileged.sh`, `runs/<RUN_ID>/` artifact dirs |

### 1.3 V2 testrunner inventory (NOT in V3)

Reference: `weed/storage/blockvol/testrunner/` in `seaweedfs` repo.

- **22 K LOC** (15 K production, 7 K tests)
- **149 YAML scenarios** (smoke, HA, fault injection, recovery, perf, snapshot, chaos, soak, K8s)
- **18 action files** (block, iscsi, nvme, fault, k8s, metrics, recovery, devops, io, system, snapshot, database, bench, cleanup)
- **6 infra files** (node, target, ha_target, iscsi_client, artifacts, fault)
- **Framework layer** (~4-5 K LOC, appears V2-agnostic in shape):
  `engine.go`, `types.go`, `parser.go`, `register.go`, `reporter.go`, `baseline.go`, `console.go`, `coordinator.go`, `cluster_manager.go`, `agent.go`, `metrics.go`, `suite.go`
- **Heavy V2 coupling** on every `actions/*.go` and `infra/*.go` (imports the V2 `blockvol` package)
- **Mature**: JUnit XML output, baseline regression, parallel phases, var substitution, artifact collection on failure, Prometheus metrics scraping

### 1.4 Gaps

- No standardized share-driven contract (every harness invents its own log layout, env vars, run-id scheme)
- No agent-readable result format (today: scrape stderr, count "PASS"/"FAIL", grep test names — fragile)
- No registry of "what scenarios exist for V3" (each gate's test instruction is a separate doc)
- No port of the testrunner framework to V3 (would unify scenarios across gates if done right)
- No K8s test path (forward-carry to dedicated cluster)
- No chaos / soak path (forward-carry)

---

## 2. Layered test pyramid (V3 target)

| Level | Latency | When | Owner | Tooling |
|---|---|---|---|---|
| **L0 dev loop** | < 2 s/pkg | every save | dev agent | `go test ./pkg -count=1` |
| **L1 component** | < 10 s | every commit | sw | `go test` with t.TempDir + subprocess in-process |
| **L2 single-host L2** | ~10-150 s | gate verification, regression | QA + dev agent | `go test` with real `cmd/*` daemons + share-driven runner |
| **L3 single-host privileged** | ~10-30 s | gate close evidence (per gate that needs OS interaction) | QA + share-driven runner | bash + sudo + Go test binary |
| **L4 multi-host hardware** | 1-10 min | gate close evidence (per gate that needs cross-node) | QA | bash scenarios on m01+M02 |
| **L5 K8s integration** | 5-30 min | gate close (CSI gates only) + dogfood | QA on K8s test cluster | helm + ginkgo or sw-test-runner-v3 |
| **L6 chaos / soak** | hours-days | release candidate validation | QA on dedicated host | sw-test-runner-v3 with fault actions |

**Discipline**: each gate picks the *minimum* layer it needs to credibly close.
Do not require L4 for a gate that L2 evidence covers honestly. Do not require
L5 for a gate that doesn't change CSI surface. Over-testing is as harmful as
under-testing — it slows everything else down.

---

## 3. Share-driven contract (the bridge)

This is the foundation everything else builds on. Without a stable contract,
neither the per-gate harnesses nor a future ported testrunner can be agent-driven.

Implementation note:

- `seaweed_block/internal/testops` now defines the V3-native contract skeleton:
  - `RunRequest`
  - `Result`
  - `PhaseResult`
  - `Driver`
  - `Registry`
  - `ShellDriver`
  - `GoTestDriver`
  - registration decoding / driver construction
- This package is the code-level insertion point for V3 scenarios. A future
  ported V2 testrunner should plug in by implementing `Driver`; it should not
  replace the contract.
- Slice 1 landed at `seaweed_block@f51b79b`: `testops/registry/g15b-manifest.json`
  loads into the registry, executes the G15b manifest/harness `go test`, and
  writes standard `run-request.json`, `result.json`, and `test-stdout.log`
  artifacts.

### 3.1 Layout

```
V:\share\v3-debug\
├── bridge\
│   ├── run-bridge.sh           # main entry: reads run-request.json → dispatches → writes result.json
│   ├── scenarios\              # one driver script per registered scenario
│   │   ├── g15a-privileged.sh
│   │   ├── g15a-non-privileged.sh
│   │   ├── g9g-l2.sh
│   │   ├── g8-failover-l2.sh
│   │   └── g7-recovery-3scenarios.sh
│   └── README.md               # schema docs + how to add a new scenario
├── src\<commit>\               # tarred source per commit (for build=true mode)
├── bin\<commit>\               # built binaries per commit (cached across runs)
└── runs\<RUN_ID>\              # one artifact dir per run
    ├── result.json
    ├── run-request.json        # echo of input, for reproducibility
    ├── test-stdout.log
    ├── <daemon>.log            # per-daemon logs
    ├── iscsi-session-pre.txt
    ├── iscsi-session-post.txt
    └── (scenario-specific extras)
```

### 3.2 `run-request.json` schema (v1.0)

```json
{
  "schema_version": "1.0",
  "scenario": "g15a-privileged",
  "source": {
    "repo": "seaweed_block",
    "commit": "ac49adb",
    "tar_path": "V:\\share\\v3-debug\\src\\ac49adb.tar"
  },
  "binaries": {
    "build": true,
    "bin_dir": "V:\\share\\v3-debug\\bin\\ac49adb"
  },
  "scenario_params": {
    "iqn": "iqn.2026-05.io.seaweedfs:g15a-priv-v1"
  },
  "artifact_dir": "V:\\share\\v3-debug\\runs\\20260503T153000Z",
  "run_id": "20260503T153000Z",
  "timeout_s": 180
}
```

Field rules:
- `schema_version` mandatory; bridge rejects unknown versions.
- `scenario` is an enum from §6 registry; bridge rejects unknown scenarios.
- `source.commit` is mandatory and propagates into result.json.
- `binaries.build = true` → bridge runs `go build` from `source.tar_path` into `binaries.bin_dir`. `false` → bridge reuses existing binaries. Most debug iterations use `false` after first build.
- `scenario_params` is scenario-specific; documented per scenario.
- `artifact_dir` is mandatory (no default); bridge creates if missing.
- `run_id` is the bridge-stable identifier; bridge uses this for log names.

### 3.3 `result.json` schema (v1.0)

```json
{
  "schema_version": "1.0",
  "run_id": "20260503T153000Z",
  "scenario": "g15a-privileged",
  "source_commit": "ac49adb",
  "status": "pass",
  "summary": "Phase 1-6 all green; 4096 bytes byte-equal; no dangling iSCSI sessions",
  "wall_clock_s": 5.93,
  "phase_results": [
    {"name": "ControllerPublish", "status": "pass", "duration_s": 0.4},
    {"name": "NodeStage",         "status": "pass", "duration_s": 2.1},
    {"name": "NodePublish",       "status": "pass", "duration_s": 0.1},
    {"name": "WriteReadByteEqual","status": "pass", "duration_s": 0.3},
    {"name": "Cleanup",           "status": "pass", "duration_s": 2.7},
    {"name": "NoDanglingSession", "status": "pass", "duration_s": 0.1}
  ],
  "artifact_dir": "V:\\share\\v3-debug\\runs\\20260503T153000Z",
  "artifacts": {
    "test_stdout":           "test-stdout.log",
    "blockmaster_log":       "blockmaster.log",
    "blockvolume_primary_log":"blockvolume-primary.log",
    "blockvolume_replica_log":"blockvolume-replica.log",
    "blockcsi_log":          "blockcsi.log",
    "iscsi_session_pre":     "iscsi-session-pre.txt",
    "iscsi_session_post":    "iscsi-session-post.txt"
  },
  "checksums": {
    "blockmaster_sha256":  "...",
    "blockvolume_sha256":  "...",
    "blockcsi_sha256":     "..."
  },
  "non_claims": [
    "no K8s integration",
    "no multi-node attach",
    "single 4 KB I/O — no perf claim"
  ]
}
```

Field rules:
- `status` ∈ {`pass`, `fail`, `error`} — `fail` = test assertion failed; `error` = harness/build/spawn problem.
- `phase_results` is structured so agents can answer "which phase?" without log scraping.
- `artifacts` are relative paths under `artifact_dir`.
- `checksums` make "did the binary actually change between runs?" trivially answerable.
- `non_claims` echoes what the scenario explicitly does NOT validate (prevents agents from over-interpreting a green result).

### 3.4 Scenario driver contract

Each `bridge/scenarios/<name>.sh` script:
- Receives one argument: path to a normalized `run-request.json`
- Must produce `<artifact_dir>/result.json` matching v1.0 schema
- Must persist all logs to `<artifact_dir>/`
- Exit code 0 = `status:"pass"`; non-zero = `status` per result.json

This isolates scenario complexity inside the driver. The bridge layer stays
thin: parse → dispatch → wait → return result path.

### 3.5 TestOps foundation architecture

The stable TestOps product is **not** the V2 testrunner codebase. The stable
product is a three-layer contract:

```text
agent / QA / CI
  -> run-request.json / result.json contract
  -> scenario registry + artifact layout
  -> pluggable scenario drivers
       - bash driver
       - go-test driver
       - privileged host driver
       - k8s driver
       - future V3-native YAML testrunner driver
```

This is the key V3 support rule:

1. The bridge contract is canonical and long-lived.
2. Scenario drivers are replaceable implementation details.
3. Ported V2 testrunner pieces may become one driver backend, not the platform itself.

Why this matters:

- G15a privileged and G7 hardware are naturally bash/host driven.
- G8/G9/G15a L2 are naturally `go test` driven.
- G15b is naturally Kubernetes driven.
- Future chaos/soak may benefit from a YAML testrunner.

Forcing all of those through one runner on day one would either slow gates down
or reintroduce V2 assumptions. The bridge keeps the external workflow stable
while allowing each scenario to use the right execution mechanism.

Architectural invariant:

> A green TestOps scenario means "this registered scenario satisfied its claim
> and produced canonical artifacts"; it does **not** imply all scenarios use the
> same internal runner.

---

## 4. TestRunner port decision matrix

The user's question: *if the V2 testrunner framework works, port it wholesale*.
The answer: **conditional, in phases**. Do not commit to wholesale port up front.

### 4.1 What's portable vs what isn't

| V2 component | Portability | V3 cost | Decision |
|---|---|---|---|
| `engine.go`, `types.go`, `parser.go` | High — V2-agnostic shape | ~1 week | **Phase B candidate** if we want native V3 scenarios |
| `register.go`, `reporter.go`, `baseline.go`, `console.go`, `coordinator.go`, `cluster_manager.go`, `agent.go`, `metrics.go`, `suite.go` | Medium — V2-coupled in initialization | ~1-2 weeks | Phase B if Phase A is the bottleneck |
| `actions/block.go`, `actions/iscsi.go`, `actions/recovery.go`, `actions/fault.go`, `actions/k8s.go`, `actions/metrics.go`, `actions/io.go`, `actions/system.go` | Low — every action body uses V2 `blockvol` types | rewrite, not port; ~2-3 weeks for a useful subset | **Phase C** — port only actions we actually need |
| `actions/snapshot.go`, `actions/database.go`, `actions/bench.go`, `actions/benchmark.go`, `actions/nvme.go`, `actions/devops.go`, `actions/cleanup.go`, `actions/helpers.go` | Mixed — some are V2-specific (snapshot/database), some are reusable patterns (cleanup/helpers) | varies | Port on demand, not preemptively |
| `infra/node.go`, `infra/target.go`, `infra/ha_target.go`, `infra/iscsi_client.go`, `infra/artifacts.go`, `infra/fault.go` | Medium — shapes reusable, V2 daemon assumptions in bodies | ~1 week | Phase B/C |
| 149 YAML scenarios | Mostly data — depends on action name + parameter compat | cheap if action shape preserved; otherwise rewrite | Port the ~5-10 scenarios the gates need; leave the rest |

### 4.2 The "wholesale port" trap

> *"If V2 testrunner is working, just port everything."*

This sounds efficient but in practice creates three problems:

1. **Scope creep**. 149 scenarios + 18 action files means months of port work before the first useful scenario runs in V3. Bridge approach ships value in days.
2. **V2 contamination risk**. Action bodies were written when V2 authority semantics were live (heartbeat-as-authority, promote/demote RPC, local-role-as-authority). Wholesale port risks importing those V2 anti-patterns into V3 scenario logic, especially in `actions/recovery.go`, `actions/fault.go`, and `actions/k8s.go`. The `v3-phase-15-control-plane-evolution.md §7` checklist applies to test code too.
3. **Maintenance debt**. We'd inherit 22 K LOC. The smaller V3-native test base today is also smaller surface for bugs and refactors.

### 4.3 Recommended phased approach

**Phase A — Bridge only (now → 1 week)**
- Ship `V:\share\v3-debug\bridge\` with the schema in §3.
- Wrap existing `g15a-privileged`, `g15a-non-privileged`, `g9g-l2`, `g8-failover-l2`, `g7-recovery-3scenarios` as scenario drivers.
- Each driver is a thin bash wrapper around the existing `go test` invocation.
- **Decision point at end of A**: does the bridge meet 80% of agent-driven test needs without a framework port?

**Phase B — Selective framework port (week 2-3, conditional on Phase A signal)**
- Port `engine.go`, `parser.go`, `types.go`, `register.go`, `reporter.go` to a new `seaweed_block/internal/testrunner/` package.
- **Strip every V2 import**. Use V3 types (`core/lifecycle`, `core/authority`) where needed; otherwise abstract via interfaces.
- Wire the bridge's scenario dispatch to call into this package as an alternative driver type alongside bash drivers.
- First V3-native scenario: re-implement `g15a-privileged.sh` as a YAML scenario using ported framework. Compare wall-clock + maintainability vs bash version.
- **Decision point at end of B**: is YAML+framework better than bash+go-test for our scenario shapes?

**Phase C — Selective action port (week 4-6, conditional on Phase B signal)**
- Port the 5-10 actions actually used by V3 gate scenarios:
  - `block.create`, `block.delete`, `block.attach`, `block.detach` (G9-class)
  - `iscsi.discovery`, `iscsi.login`, `iscsi.logout`, `iscsi.write_read` (G15a-class)
  - `fault.kill_process`, `fault.network_partition` (G8-class, future)
  - `metrics.scrape_prometheus` (G17-lite, future)
- Each ported action: rewrite body using V3 binaries / V3 types; keep registry signature similar to V2 for YAML compat.
- **Do not port** snapshot, database, bench, nvme, devops actions until a V3 gate needs them.

**Phase D — Wholesale port (conditional, only if A/B/C all prove out)**
- Port remaining actions selectively as gates demand them.
- Port the 5-10 most useful YAML scenarios for K8s and chaos.
- **Stop point**: 22 K LOC of V2 testrunner ⇒ ~12-15 K LOC of V3-native testrunner once V2-specific actions are dropped. Stop when we have what V3 gates need; don't port for completeness.

### 4.4 What we will never port

These are V2-specific or V2-anti-pattern:

- V2 `promote`/`demote` action calls (V3 forbids — authority is publisher-minted)
- V2 `heartbeat-as-authority` assumptions in cluster_manager
- V2 snapshot scenarios that assume V2 snapshot semantics
- V2 database actions (PostgreSQL etc) — if needed for V3, write fresh
- V2 perf baselines — V3 perf SLO is G21, not G15a

---

## 5. Phased roadmap

```text
P0 (now, 2026-05-03):
  ✓ Document existing harnesses (g15a-priv, g7) in design/test/
  ✓ Lock share-driven contract v1.0 schema (this doc §3)
  ⏳ Architect §1.A ratification of contract + Phase A scope

P1 (week 1):
  - Ship bridge skeleton: V:\share\v3-debug\bridge\run-bridge.sh + 5 scenario drivers
  - Convert existing g15a-priv runner to bridge scenario format
  - Land sw-block/design/test/v3-phase-15-testops-bridge-instruction.md
  - QA verifies: agent submits run-request.json for all 5 scenarios, gets valid result.json
  - G15b exception: K8s lab driver may be registered during P1 because G15b is
    the active gate, but it still uses the same bridge contract; do not treat
    that as permission to start Phase B/C framework port early.

P2 (week 2, conditional):
  - Begin Phase B framework port (engine + parser + types only first)
  - Re-implement g15a-priv as YAML scenario through ported framework
  - Compare: agent-experience, maintainability, wall-clock

P3 (week 3-4):
  - Port 5-10 selective actions (Phase C)
  - Wrap G8 fault scenarios (kill primary, kill replica) as YAML scenarios
  - Begin K8s test cluster setup

P4 (week 5-8):
  - K8s integration scenarios (g15a + workload pod)
  - Chaos scenario set (kill + network + disk-full × repeat)
  - Soak scenario (4-hour run with byte-equal verification at end)

P5 (release candidate):
  - Full G22 cluster validation bundle (per v3-phase-15-priority-mvp-plan.md §P15-P8)
  - Manifest of all gates × all evidence levels
  - Regression baseline + automated diff
```

**Phase gates**: P2 only starts if P1 ships a working bridge that agents
actually use. P3 only starts if P2 framework port proves better than bash. P4
only after P3 actions exist. **Do not run phases in parallel** — each phase's
decision informs the next.

---

## 6. Initial scenario registry

Each scenario has: a driver in `bridge/scenarios/`, a test instruction in
`design/test/`, and a known-green pin to a commit.

| Scenario | Driver | Instruction | Pinned commit | Layer |
|---|---|---|---|---|
| `g15a-privileged` | `g15a-privileged.sh` | `v3-phase-15-g15a-privileged-qa-test-instruction.md` | `ac49adb` | L3 |
| `g15a-non-privileged` | `g15a-non-privileged.sh` | `v3-phase-15-g15a-qa-test-instruction.md` | `ac49adb` | L2 |
| `g15b-manifest` | `go-test` via `testops/registry/g15b-manifest.json` | `v3-phase-15-g15b-k8s-qa-test-instruction.md` | `f51b79b` | L1/L2 |
| `g15b-k8s-static` | `g15b-k8s-static.sh` | `v3-phase-15-g15b-k8s-qa-test-instruction.md` | `5375add` | L5 |
| `g9g-l2` | `g9g-l2.sh` | `v3-phase-15-g9g-qa-test-instruction.md` | `7ed9ab2` | L2 |
| `g8-failover-l2` | `g8-failover-l2.sh` | (forward-carry: derive from G8 mini-plan §12) | `b320336` | L2 |
| `g7-recovery-3scenarios` | `g7-recovery.sh` | (existing: V:\share\g5-test\scenarios-g7.sh) | `d09fcc6` | L4 |

After P1 ships, every gate close that hits L2/L3/L4 should have a registered
scenario. Adding a new scenario = drop a driver script, add a row here, write
the test instruction.

---

## 7. Discipline boundaries

The TestOps layer must not become a path for V2 anti-patterns or scope creep
into V3.

### 7.1 What TestOps does not do

1. **TestOps does not replace `go test`** as the dev loop. `go test ./pkg -count=1` stays the fastest feedback channel; the bridge is for cross-process / privileged / hardware / agent-driven work.
2. **TestOps does not own product policy**. No scenario should encode "primary should auto-promote after 30 s" — that's product/engine semantics; scenarios verify, they don't define.
3. **TestOps does not bypass the structural guard pattern** (`v3-phase-15-control-plane-evolution.md §7.1`). Test infrastructure that needs to read `Publisher` state must still use the read-only seam, not direct mutation.
4. **TestOps does not import V2 authority semantics**. No promote/demote, no heartbeat-as-authority, no local-role-as-authority — even in test code.
5. **TestOps does not own debug**. Bridge produces logs; reading and reasoning about them is still a human/agent job. Don't bake "auto-diagnose failure" into scenarios.

### 7.2 What TestOps does

1. **Standardize the I/O contract** between agents and verification.
2. **Persist and organize artifacts** with a stable schema.
3. **Make scenarios re-runnable and diff-able** across commits.
4. **Provide a registry** of "what scenarios exist for which gate".
5. **Hide harness mechanics** so dev/QA agents see consistent surface across L0..L6.

---

## 8. §1.A questions for architect

Proposed bindings for ratification:

| Q | Question | Default |
|---|---|---|
| **Q1** | Is share-driven `run-request.json`/`result.json` the canonical contract for L2 and above? | YES — bash-only invocations are deprecated for new harnesses; existing G7 harness gets wrapped as a scenario, not rewritten |
| **Q2** | Phase A bridge — bash drivers only, or also Go-based drivers from day one? | Bash only for P1 — keeps the bridge thin; Go drivers come in Phase B if framework port goes ahead |
| **Q3** | TestRunner port — wholesale, selective, or never? | **Selective in phases** per §4.3 (A → B → C → D, each phase gated on the prior's signal) |
| **Q4** | Where does ported testrunner live in V3? | `seaweed_block/internal/testrunner/` (private to the V3 module; not a published API) |
| **Q5** | Who owns scenario authoring? | sw owns scenarios that depend on internal state; QA owns scenarios that exercise public surfaces; both review each other's |
| **Q6** | K8s test cluster — when do we provision? | Phase P4 (week 5+); not blocking P1-P3 |
| **Q7** | Should TestOps emit metrics to a Prometheus endpoint? | Forward-carry — wait until G17-lite observability lands so we have a target |

Architect should override defaults before P1 starts.

Current sw recommendation:

- Ratify Q1/Q3/Q4 as default.
- Continue P1 on top of `internal/testops`, not directly on raw shell scripts; first executable registration is `g15b-manifest` at `f51b79b`.
- Treat V2 testrunner port as a future `Driver` implementation.

---

## 9. Forward-carry / out of scope for v0.1

| Item | Why deferred | Earliest landing |
|---|---|---|
| K8s integration scenarios | Need provisioned test cluster | P4 |
| Chaos / soak / fault injection | Need ported `actions/fault.go` (Phase C) | P4 |
| Prometheus metrics scraping | Need G17-lite observability endpoints | post-G17-lite |
| Performance / SLO scenarios | G21 owns perf | post-G21 |
| Multi-language scenario authoring (e.g., Python clients) | YAGNI for now | indefinite |
| Cloud / multi-cloud scenario runners (AWS/GCP/Azure) | Single dev cluster sufficient for beta | indefinite |
| GUI / dashboard for scenario results | result.json is sufficient for agent consumption | indefinite |
| Long-term result archive (DB-backed) | Filesystem under `runs/<RUN_ID>/` is sufficient for current cadence | post-MVP |

---

## 10. Close criteria for this plan

This TestOps plan v0.1 is "accepted" when:

1. Architect ratifies §1.A questions above.
2. P0 docs land (this plan + bridge instruction).
3. P1 bridge ships with at least 3 of 5 registered scenarios working end-to-end.
4. At least one dev/QA agent submits a `run-request.json` and consumes the resulting `result.json` without human intervention to find logs or interpret status.
5. Forward-carry items §9 are honestly tracked, not silently skipped.

---

## 11. Appendix — quick reference for dev agents

### 11.1 Submit a run

```bash
# from anywhere with SMB access to V:\share\v3-debug\
cat > /tmp/req.json <<EOF
{
  "schema_version": "1.0",
  "scenario": "g15a-privileged",
  "source": {"repo":"seaweed_block","commit":"ac49adb","tar_path":"V:\\\\share\\\\v3-debug\\\\src\\\\ac49adb.tar"},
  "binaries": {"build": false, "bin_dir": "V:\\\\share\\\\v3-debug\\\\bin\\\\ac49adb"},
  "scenario_params": {},
  "artifact_dir": "V:\\\\share\\\\v3-debug\\\\runs\\\\my-run-001",
  "run_id": "my-run-001",
  "timeout_s": 180
}
EOF
bash V:/share/v3-debug/bridge/run-bridge.sh /tmp/req.json
```

### 11.2 Read the result

```bash
cat V:/share/v3-debug/runs/my-run-001/result.json | jq .status
cat V:/share/v3-debug/runs/my-run-001/result.json | jq '.phase_results[] | select(.status != "pass")'
cat V:/share/v3-debug/runs/my-run-001/blockvolume-primary.log
```

### 11.3 Add a new scenario

1. Drop `bridge/scenarios/<new-name>.sh` (must produce `result.json` v1.0 in `<artifact_dir>`).
2. Add a row to §6 registry with pinned commit.
3. Land `sw-block/design/test/v3-phase-15-<gate>-qa-test-instruction.md`.
4. Open a PR for review.

---

## 12. Architect-review checklist

| Check | Where addressed |
|---|---|
| Scope truth — what IS this plan and what isn't | §0 + §7 |
| Phasing realism — gates that can stop the plan if signals don't pan out | §4.3 + §5 phase gates |
| V2 contamination risk | §4.4 + §7.1 |
| Unblocks downstream work | §6 registry + §11 quick reference |
| Architect choice points | §8 Q1-Q7 |
| Honest forward-carry | §9 |

---

## 13. Sign

| Role | Signed | When | Basis |
|---|---|---|---|
| **QA** (drafter) | ✅ 2026-05-03 | — | drafted v0.1; verified harness state; assessed V2 testrunner port viability |
| **sw** | ⏳ pending | — | review §3 contract, §4.3 phase gates, §6 registry |
| **architect** (single-sign) | ⏳ pending | — | ratify §8 Q1-Q7 before P1 starts |
