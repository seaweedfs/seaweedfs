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

## QA Task 1 — Registry Smoke Runner

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

Acceptance:

- QA can invoke one command by scenario name.
- The command prints final status and artifact path.
- The command does not require a human to remember the raw script path.
- The generated `result.json` uses the V3 TestOps schema.

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

Assess V2 `weed/storage/blockvol/testrunner/` as a future driver implementation, not as a replacement for V3 TestOps.

Port policy:

- Keep V3 `RunRequest` / `Result` / registry as canonical.
- Port only V2 framework pieces that help execute scenarios.
- Do not port V2 authority/promotion/demotion semantics.
- Do not let YAML scenarios call product internals directly.

Deliverable:

```text
sw-block/design/test/v3-phase-15-v2-testrunner-port-audit.md
```

Minimum table:

| V2 area | LOC / files | V3 decision | Reason |
|---|---:|---|---|
| parser / variable substitution | TBD | candidate | likely reusable |
| reporter / JUnit XML | TBD | candidate | useful output |
| action registry | TBD | rewrite/rebind | V2 action bodies are coupled |
| blockvol infra/actions | TBD | do not port as-is | V2 product coupling |
| promote/demote/failover actions | TBD | never port as authority | violates V3 control-plane discipline |

Acceptance:

- Clear recommendation: selective port, shell bridge, or defer.
- No code port starts before audit review.

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

## Non-Goals

QA should not:

- build a dashboard before the file contract is stable;
- port all V2 testrunner YAML before the V3 registry is proven;
- add product behavior to make tests easier;
- rely on static PV target facts as the normal G15b evidence path;
- treat TestOps pass as broader than the scenario's non-claims.

---

## First Concrete QA Assignment

Run this sequence:

1. Confirm `phase-15@48f2e0f` is checked out.
2. Run `go test ./internal/testops ./cmd/blockcsi -count=1`.
3. Re-run `g15b-k8s-static` on M02 using the registered scenario metadata as the source of truth.
4. Produce `result.json` and artifact path.
5. File one short report:

```text
Scenario: g15b-k8s-static
Tree:
Lab:
Status:
Wall clock:
Artifact dir:
Result.json present: yes/no
Required logs present: yes/no
Cleanup verified: yes/no
Non-claims unchanged: yes/no
```

If this works, TestOps has crossed from "per-gate scripts" to "registered scenario runner" for the first real K8s scenario.
