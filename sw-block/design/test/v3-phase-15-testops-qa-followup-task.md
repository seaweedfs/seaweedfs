# V3 Phase 15 TestOps / TestRunner QA Follow-Up Task

**Date**: 2026-05-03
**Status**: QA follow-up task after G15b K8s PASS and `phase-15@48f2e0f`
**Owner**: QA, with sw review on registry/schema changes
**Scope**: mature the V3 TestOps substrate so scenarios can be registered, run, and reported consistently across dev, privileged host, hardware, and Kubernetes labs

---

## Goal

Turn the current G15b-proven TestOps path into a reusable substrate:

```text
scenario registry
  -> driver selection
  -> run-request.json
  -> driver/harness execution
  -> result.json + canonical artifact dir
  -> gate close evidence
```

This is workflow infrastructure, not product behavior. TestOps must never mint authority, change placement, promote/demote replicas, or replace product recovery logic.

---

## Current Green Baseline

Code baseline:

- `seaweed_block phase-15@48f2e0f`
- G15b K8s static PV PASS on M02 at `95b7217`
- G15b merged into `phase-15` at `48f2e0f`

Existing registered scenarios:

| Scenario | Driver | Registry file | Known green |
|---|---|---|---|
| `g15b-manifest` | `go-test` | `testops/registry/g15b-manifest.json` | `f51b79b` |
| `g15b-k8s-static` | `shell` | `testops/registry/g15b-k8s-static.json` | `95b7217` |

Known command baseline:

```powershell
go test ./internal/testops ./cmd/blockcsi -count=1
go test ./internal/testops ./core/csi ./cmd/blockcsi ./core/host/master ./cmd/blockmaster ./cmd/blockvolume -count=1
```

K8s baseline artifact:

```text
V:\share\g15b-k8s\runs\20260503T172122Z-95b7217\
```

---

## QA Task 0 — CLI Entry Point Bootstrap (prerequisite to Task 1)

**State as of 2026-05-03**: `internal/testops/` contains the library (`Registry`, `RunRequest`, `Result`, `ShellDriver`, `GoTestDriver`, `registration.go` loader). **No `cmd/` entry point exists**, so today the only way to invoke a registered scenario is via Go test code or by hand-shelling the driver path.

QA cannot satisfy Task 1's "QA can invoke one command by scenario name" without a CLI. Sw must land one of:

- **Option A (preferred)**: `cmd/sw-testops` binary that takes `--scenario <name> --commit <sha> --artifact-dir <path>`, loads `testops/registry/<name>.json`, dispatches the driver, writes `result.json`, prints final status + artifact path on stdout. ~150 LOC.
- **Option B (interim)**: a `go run ./internal/testops/cli` package or a `scripts/sw-testops.sh` wrapper that accepts the same flags and shells through.
- **Option C (forbidden)**: keep the substrate library-only and require QA to hand-build invocations per scenario. This defeats the registry's purpose and Task 1's acceptance criterion.

**Acceptance**:
- One of A/B exists, documented in `internal/testops/README.md` or equivalent.
- `<binary> --scenario g15b-manifest --commit <sha> --artifact-dir /tmp/x` runs locally and exits 0 with `pass`.
- `<binary> --help` shows scenario list + flag set.
- `result.json` is written to the supplied `--artifact-dir`.

**Owner**: sw to land. QA to validate by running both registered scenarios via the new CLI before starting Task 1.

**Blocks**: Task 1, Task 2 (auditing artifacts requires a runner producing them via the canonical path), and the First Concrete QA Assignment.

---

## QA Task 1 — Registry Smoke Runner (depends on Task 0)

Build a QA-side smoke that can run a registered scenario by name.

Minimum behavior:

```text
input:  scenario name + source commit + artifact dir
action: load testops/registry/<scenario>.json
        create run-request.json
        invoke the configured driver
output: result.json + logs
```

First targets:

1. `g15b-manifest`
   - Must run locally with no Kubernetes.
   - Expected runtime: under 2 seconds.
   - Expected status: `pass`.

2. `g15b-k8s-static`
   - Must run only on a K8s lab with required preconditions.
   - Expected result on M02: `pass`.
   - Must preserve artifact directory on both pass and fail.
   - QA-known-green pin: `V:\share\g15b-k8s\runs\20260503T172122Z-95b7217\` (use as oracle for "what artifacts must appear").

Acceptance:

- QA can invoke one command by scenario name.
- The command prints final status and artifact path.
- The command does not require a human to remember the raw script path.
- The generated `result.json` uses the V3 TestOps schema (matches `internal/testops/types.go::Result` field set).
- Required-capability pre-flight: if a registry-listed capability is missing on the host (e.g., `kubectl` for `g15b-k8s-static` on a non-K8s box), the runner exits with `status:"error"` and a clear `summary` — NOT `status:"fail"`. This distinction matters: "fail" = scenario asserted a bad outcome; "error" = harness couldn't run it.

---

## QA Task 2 — Artifact Contract Audit

Audit the artifact dirs produced by registered scenarios.

For every scenario, verify:

- `run-request.json` exists.
- `result.json` exists.
- stdout/stderr or equivalent driver logs exist.
- daemon logs exist for cross-process scenarios.
- cleanup evidence exists for privileged/K8s scenarios.
- failure path preserves logs before cleanup.

Start with:

```text
g15b-manifest
g15b-k8s-static
```

Then add:

```text
g15a-privileged
g9g-l2
g8-failover-l2
g7-recovery-3scenarios
```

Acceptance:

- Produce one table listing required artifacts per scenario.
- Mark missing artifacts as follow-up tasks, not implicit pass.
- Keep historical artifact dirs immutable.

---

## QA Task 3 — Register Next Scenarios

Add registry entries only after the underlying scenario has a known-green evidence run.

Recommended order:

| Priority | Scenario | Driver | Reason |
|---|---|---|---|
| 1 | `g15a-privileged` | `shell` | already has m01 privileged evidence and harness |
| 2 | `g9g-l2` | `go-test` | fast product-loop L2 scenario |
| 3 | `g8-failover-l2` | `go-test` | data-continuity evidence, important regression |
| 4 | `g7-recovery-3scenarios` | `shell` | hardware recovery baseline |

Rules:

- Do not register a scenario as green until QA has an evidence pin.
- `known_green_commit` is evidence, not a forced checkout.
- `non_claims` must be explicit.
- The registry must not contain authority-shaped desired state such as epoch, endpointVersion, primary, ready, or healthy.

Acceptance:

- Each new registry file has a matching QA instruction.
- `go test ./internal/testops -count=1` validates registration shape.
- If a scenario cannot run on the dev workstation, tests may validate registration construction only.

---

## QA Task 4 — V2 TestRunner Port Assessment

**Reference**: this task **executes** the phased decision matrix in `v3-phase-15-testops-plan.md §4` (Phase A → B → C → D). Do not re-derive the high-level decision; instead produce concrete LOC counts + per-file dispositions to make Phase B/C/D go/no-go answerable.

Assess V2 `weed/storage/blockvol/testrunner/` as a future driver implementation, not as a replacement for V3 TestOps.

Port policy (carried forward from `v3-phase-15-testops-plan.md §4.4`):

- Keep V3 `RunRequest` / `Result` / registry as canonical.
- Port only V2 framework pieces that help execute scenarios.
- Do not port V2 authority/promotion/demotion semantics.
- Do not let YAML scenarios call product internals directly.

Deliverable:

```text
sw-block/design/test/v3-phase-15-v2-testrunner-port-audit.md
```

Required table (concrete LOC, not "TBD"):

| V2 area | LOC | V3 decision | Reason | Phase gate |
|---|---:|---|---|---|
| parser / variable substitution | TBD | candidate | likely reusable | Phase B |
| reporter / JUnit XML | TBD | candidate | useful output | Phase B |
| engine / coordinator / cluster_manager | TBD | candidate (audit V2 coupling first) | core scenario sequencing | Phase B |
| action registry | TBD | rewrite/rebind | V2 action bodies are coupled | Phase C |
| blockvol infra/actions (block, iscsi, io, system) | TBD | rewrite per-action | V2 product coupling | Phase C — only port what V3 gates need |
| nvme / snapshot / database / bench / devops actions | TBD | defer / never | V2-specific or YAGNI | n/a |
| promote/demote/failover actions | TBD | **never port as authority** | violates V3 control-plane discipline | n/a |
| 149 YAML scenarios | TBD | port ≤10 selectively | most reference V2 actions that are not portable | Phase D |

Acceptance:

- Clear recommendation per row: `port`, `rewrite`, `defer`, `never`.
- No code port starts before audit review.
- Audit explicitly marks any V2 action whose name overlaps a V3 truth-domain (e.g., V2 `promote_volume` ≠ V3 publisher mint) — rename or reject in V3 driver layer to avoid confusion.

---

## QA Task 5 — Failure-Mode Harness Discipline

For every privileged/hardware/K8s scenario, test the harness failure path intentionally.

Examples:

- break blockmaster service name;
- remove `iscsi_tcp` module precondition;
- force pod timeout;
- make expected image missing;
- kill one daemon before attach.

Acceptance:

- Harness exits non-zero.
- Artifact dir contains enough logs to diagnose first failing layer.
- Cleanup still runs.
- No dangling iSCSI sessions or K8s resources remain.

This is required because G15b proved that failure-path evidence is as important as pass-path evidence.

---

## QA Task 6 — Scenario Authoring Guide

After Tasks 0–3 land, dev/QA agents need a single doc to add a new scenario without re-deriving conventions each time.

Deliverable:

```text
sw-block/design/test/v3-phase-15-testops-scenario-authoring-guide.md
```

Must cover:

1. Registry JSON shape (every field, with example) — pulled from `internal/testops/types.go` to stay in sync.
2. Driver type selection: when to use `shell` vs `go-test`.
3. Required vs optional fields; defaults.
4. Required-capability vocabulary (which strings are recognized for pre-flight gating).
5. `non_claims` discipline: must mirror the QA test instruction; agent must not register a scenario whose `non_claims` exceed what the test actually proves.
6. Forbidden field set (per `v3-phase-15-control-plane-evolution.md §7.1`): no `epoch`, `endpoint_version`, `assignment`, `ready`, `healthy`, `primary` in registry JSON.
7. End-to-end recipe: write driver script → write registry JSON → write QA instruction → land the three together → run via Task 0 CLI → confirm green → file evidence pin.

Acceptance:

- A dev agent can add a new scenario without consulting QA or sw beyond the doc.
- The doc cites at least 2 worked examples (`g15b-manifest` for go-test, `g15b-k8s-static` for shell).
- A new entry in `testops/registry/` is a 3-file PR: registry JSON + driver (or test) + QA instruction.

---

## QA Task 7 — Schedule and Dependencies

```text
Task 0 (CLI bootstrap)             [SW]    blocks 1, 2, First Concrete Assignment
  └─ Task 1 (registry smoke)       [QA]    blocks 2 verification
       └─ Task 2 (artifact audit)  [QA]    informs Task 3
            └─ Task 3 (register next scenarios)  [QA + SW review]
Task 5 (failure-mode discipline)   [QA]    interleaves with Task 1 (cheap parallelism)
Task 4 (V2 port audit)             [QA]    independent; gate before any Phase B port code
Task 6 (authoring guide)           [QA]    after Task 3 has at least 3 scenarios registered
```

Critical-path estimate (single owner, no parallelism):

| Task | Owner | Effort |
|---|---|---|
| 0 CLI bootstrap | sw | ~2-4h |
| 1 Registry smoke | QA | ~2h after Task 0 |
| 2 Artifact audit | QA | ~3h |
| 3 Register 4 scenarios | QA | ~1h per scenario after Task 2; total ~4-6h |
| 4 V2 port audit | QA | ~4-6h (read 22K LOC + write audit doc) |
| 5 Failure-mode tests | QA | ~2h per scenario; do during Tasks 1+3 |
| 6 Authoring guide | QA | ~2h after Task 3 |

Sequenceable in ~5 working days end-to-end for a single QA owner, longer with concurrent product work or if Task 0 grows remote-host support in v1.

---

## QA Task 8 — Open Questions for sw / Architect

Block on these before locking the substrate as canonical.

| # | Question | Default if not answered |
|---|---|---|
| Q1 | Does the registry runner need an async/queue mode for soak/chaos scenarios, or is synchronous-only acceptable for v1? | Synchronous-only for v1; async deferred to forward-carry |
| Q2 | Should `result.json` schema version follow semver (`1.0` → `1.1` for additive fields) or just bump on any change? | Semver — additive fields don't bump major; agents tolerate unknown fields |
| Q3 | Does the registry support per-scenario environment overrides (e.g., scenario-specific `KUBECONFIG`, `G15A_PRIVILEGED`)? Current `DriverSpec.Env []string` already supports `KEY=value` entries, but the authoring guide must define the convention and forbid secrets in registry files. | Keep `driver.env []string` for v1; add a secret-safe override mechanism only when a real scenario needs it |
| Q4 | Should `result.json` include a `binary_checksums` field (per `v3-phase-15-testops-plan.md §3.3` schema), or compute on demand? | Add field, populated by drivers when `binaries.bin_dir` is set |
| Q5 | Is there a "registry validate" command planned (lint registry JSON against `internal/testops/types.go`)? Useful for CI. | Yes — add `cmd/sw-testops validate <file>` in Task 0 scope |
| Q6 | Where does the bridge in `v3-phase-15-testops-plan.md §3` (`V:\share\v3-debug\bridge\`) fit relative to `cmd/sw-testops` (Task 0)? Same binary or different layer? | The Task 0 CLI **is** the bridge; `V:\share\v3-debug\` becomes the share-side `runs/` artifact root only |

---

## QA Task 9 — Cross-Platform Notes

The dev workstation is Windows (no native iSCSI initiator, no privileged mount). Many scenarios cannot run there.

Required behavior:

- Task 0 CLI must run on Windows for *registry validate* and *go-test driver* scenarios.
- For `shell` driver scenarios that target Linux capabilities, the CLI must:
  - either refuse with `status:"error"` + clear "scenario requires Linux host with capability X" summary; OR
  - support a `--remote-host <user@host>` flag that ships source via `git archive | ssh tar x` and runs the driver remotely (matches the manual G15b/G15a-priv pattern QA used for the prior runs).
- Document the platform matrix in the authoring guide (Task 6).

Acceptance:

- A Windows dev agent can run `sw-testops --scenario g15b-manifest` locally and get green.
- A Windows dev agent can run `sw-testops --scenario g15b-k8s-static --remote-host testdev@192.168.1.184` and get green (or get a clear "remote-host mode not yet implemented; see manual procedure in QA instruction" error).

---

## Non-Goals

QA should not:

- build a dashboard before the file contract is stable;
- port all V2 testrunner YAML before the V3 registry is proven;
- add product behavior to make tests easier;
- rely on static PV target facts as the normal G15b evidence path;
- treat TestOps pass as broader than the scenario's non-claims;
- introduce a results database before the filesystem `runs/<RUN_ID>/` layout becomes a bottleneck;
- conflate `cmd/sw-testops` with the future K8s test cluster orchestrator (out of scope per `v3-phase-15-testops-plan.md §9`).

---

## First Concrete QA Assignment

**Pre-condition**: Task 0 CLI has landed and is callable as `sw-testops`. If not, this assignment is BLOCKED — file the block and wait, do not improvise hand-shelled invocations.

Run this sequence on the latest `phase-15` HEAD:

1. Confirm tree: `git -C <repo> rev-parse HEAD`
2. Validate registry shape locally: `go test ./internal/testops -count=1`
3. Run `g15b-manifest` via CLI: `sw-testops --scenario g15b-manifest --commit <HEAD> --artifact-dir /tmp/g15b-manifest-$(date -u +%Y%m%dT%H%M%SZ)`
   Expected: PASS in <2s, `result.json` present, status="pass".
4. Run `g15b-k8s-static` via CLI on M02 (or via `--remote-host testdev@192.168.1.184` per Task 9):
   `sw-testops --scenario g15b-k8s-static --commit <HEAD> --artifact-dir /mnt/smb/work/share/g15b-k8s/runs/<RUN_ID>`
   Expected: PASS in ~30s, all 17 declared artifacts present per `testops/registry/g15b-k8s-static.json::artifacts`.
5. File this report to `V:\share\v3-debug\reports\<RUN_ID>-first-assignment.md`:

```text
First TestOps Bridge Verification
=================================
Tree:               <commit sha>
Date (UTC):         <ISO 8601>
Lab(s):             <local | M02 | both>
CLI binary path:    <path>
CLI version/sha:    <output of sw-testops --version>

Scenario 1: g15b-manifest
  Status:           <pass | fail | error>
  Wall clock:       <seconds>
  Artifact dir:     <path>
  result.json path: <path>
  Required artifacts present (per registry):  <count present>/<count declared>
  Missing artifacts (if any):                  <list or "none">
  Cleanup verified:                            <yes | n/a for go-test>
  Non-claims match QA instruction:             <yes | discrepancies listed>

Scenario 2: g15b-k8s-static
  Status:           <pass | fail | error>
  Wall clock:       <seconds>
  Artifact dir:     <path>
  result.json path: <path>
  Required artifacts present:                  <count>/<count>
  Missing artifacts:                           <list or "none">
  Cleanup verified:                            <yes — list of checks: iscsi sessions, k8s resources, mounts>
  Non-claims match QA instruction:             <yes | list discrepancies>
  Pod final state:                             <Succeeded | other — describe>
  Checksum verification observed in pod.log:  <yes | quote the line>

Bridge verdict:
  Substrate ready for Tasks 2-6:               <yes | what blocks>
  Open questions surfaced (Task 8):            <list any new ones>
```

Stricter than the prior `yes/no` template: every "yes" must cite the artifact field or log line that supports it. "Cleanup verified: yes" without listing what was checked is rejected.

If this works, TestOps has crossed from "per-gate scripts" to "registered scenario runner" for the first real K8s scenario.

---

## Done When (overall completion criteria)

This QA follow-up is "complete" when ALL of:

1. Task 0 CLI binary exists and is documented.
2. Task 1 smoke runner passes for both currently-registered scenarios via the CLI.
3. Task 2 produces an artifact-audit table covering both currently-registered scenarios + at least one of (g15a-privileged, g9g-l2).
4. Task 3 has at least 3 additional scenarios registered (from the priority list) with evidence pins.
5. Task 4 V2 port audit document exists with concrete LOC counts and per-row dispositions.
6. Task 5 failure-mode discipline applied to at least 2 scenarios (not just one).
7. Task 6 authoring guide exists and was used to register at least one scenario by an agent other than the original author.
8. Task 8 open questions all answered (or explicitly deferred with forward-carry rationale).
9. The First Concrete QA Assignment report exists, signed by QA.

Estimated wall clock to all-9-done: ~5 working days, single QA owner, assuming sw lands Task 0 within 1 day of acceptance.

---

## QA Response (2026-05-03 draft)

I (QA) commit to:

- Tasks 1, 2, 3, 5, 6, 9 — clear QA work, ready when Task 0 lands.
- Task 4 — own the audit document; will produce concrete LOC counts and per-file dispositions.

I push back on:

- The original First Concrete QA Assignment as written: it assumes a CLI exists. Without Task 0, I'd have to hand-invoke `bash scripts/run-g15b-k8s-static.sh ...` again — which is exactly the per-gate-script pattern TestOps is meant to replace. Updated to require Task 0 first.
- The original Task 4 Acceptance ("clear recommendation: selective port, shell bridge, or defer"): the recommendation already exists in `v3-phase-15-testops-plan.md §4.3` (selective in phases A→B→C→D). Updated Task 4 to **execute** that decision matrix with concrete LOC, not re-derive it.

I flag for sw / architect:

- Task 0 needs an owner (sw, by my reading). Assigning to QA would mean QA writes infrastructure code that QA also tests — bad incentive structure.
- Task 8 Q3 (driver env overrides) and Q5 (registry validate command) likely affect Task 0's scope. Answering before Task 0 starts saves a re-cut.

I have not started any task yet because Task 0 isn't done.
