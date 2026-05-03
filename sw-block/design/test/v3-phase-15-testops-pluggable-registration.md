# V3 Phase 15 TestOps — Pluggable Registration Design

**Date**: 2026-05-03
**Status**: architect draft; complements `v3-phase-15-testops-plan.md`
**Scope**: how V3 gates/projects expose test scenarios so TestOps can discover, register, and run them independently
**Code anchor**: `seaweed_block/internal/testops` introduced at `c2b5d9a`; first registered `go-test` scenario landed at `f51b79b`

---

## §0 Product Sentence

V3 should be testable through a stable TestOps registration surface:

```text
project / gate exposes scenario registration
  -> TestOps registry binds scenario name to driver
  -> TestOps consumes run-request.json
  -> driver runs go-test / shell / privileged host / k8s / future YAML runner
  -> result.json + artifact directory are emitted in canonical shape
```

This lets TestOps run V3 independently without importing V3 internals and without forcing every gate into one runner implementation.

---

## §1 Core Rule

Every V3 gate that needs L2+ evidence should register a TestOps scenario.

Registration is:

- data + driver binding;
- test workflow metadata;
- artifact contract;
- non-claims.

Registration is **not**:

- product authority;
- placement policy;
- failover policy;
- runtime plugin loading;
- a backdoor into internal state mutation.

---

## §2 What Becomes Pluggable

Pluggable:

| Surface | Pluggable unit | Example |
|---|---|---|
| L1/L2 `go test` scenario | `GoTestDriver` | G8 failover L2, G9G product loop |
| Host privileged scenario | `ShellDriver` | G15a privileged iSCSI/mkfs/mount |
| Multi-host hardware scenario | `ShellDriver` / future `SSHDriver` | G7 recovery #2/#5/#6 |
| K8s scenario | `K8sDriver` or shell wrapper | G15b static PV/PVC/pod |
| Future YAML scenario | `YAMLDriver` | ported V2 testrunner engine/parser |

Not pluggable:

| Surface | Reason |
|---|---|
| `blockmaster` authority publisher | Product truth; must not be loaded as test plugin. |
| `blockvolume` recovery/replication engine | Product truth; TestOps observes, does not replace. |
| CSI controller/node service implementation | Product surface; TestOps drives it through CSI/K8s calls. |
| Placement/failover policy | Product semantics; registration cannot define policy. |

The important distinction:

> The test execution path is pluggable. The product runtime truth is not.

---

## §3 V3 TestOps Path

The V3 path is layered:

```text
internal/testops
  ├── RunRequest / Result schema
  ├── Driver interface
  ├── Registry
  └── driver implementations

testops/registry/
  ├── g15a-privileged.json
  ├── g15b-manifest.json
  ├── g15b-k8s-static.json
  ├── g9g-l2.json
  └── g8-failover-l2.json

V:\share\v3-debug\bridge\
  ├── run-bridge.sh
  ├── run-bridge.exe or go wrapper (future)
  ├── scenarios\
  │   ├── g15a-privileged.sh
  │   ├── g15b-k8s-static.sh
  │   └── g7-recovery.sh
  └── runs\<RUN_ID>\result.json + logs
```

Ownership:

- `internal/testops`: V3 code repo.
- `testops/registry`: V3 code repo, because it pins real commands/paths relative to the code tree.
- `design/test/*.md`: docs repo, because it explains QA contract and close evidence.
- `V:\share\v3-debug\bridge`: harness side, mutable by QA/dev agents.

---

## §4 Registration Shape

Recommended file shape:

```json
{
  "schema_version": "1.0",
  "scenario": "g15b-k8s-static",
  "gate": "G15b",
  "layer": "L5",
  "driver": {
    "type": "shell",
    "path": "scripts/run-g15b-k8s-static.sh"
  },
  "default_timeout_s": 600,
  "required_capabilities": [
    "kubectl",
    "privileged-k8s-node",
    "iscsiadm",
    "mount"
  ],
  "required_images": [
    "sw-block:local",
    "sw-block-csi:local"
  ],
  "qa_instruction": "sw-block/design/test/v3-phase-15-g15b-k8s-qa-test-instruction.md",
  "known_green_commit": "5375add",
  "artifacts": [
    "result.json",
    "run-request.json",
    "pod.log",
    "blockmaster.log",
    "blockvolume-r1.log",
    "blockvolume-r2.log",
    "blockcsi-controller.log"
  ],
  "non_claims": [
    "no dynamic provisioning",
    "no failover under live mount",
    "single-node only"
  ]
}
```

Rules:

- `scenario` is globally unique.
- `driver.type` must map to a TestOps `Driver`.
- `known_green_commit` is evidence, not a constraint. Agents may run newer commits.
- `non_claims` must be present for every L2+ scenario.
- The registration file must not contain authority-shaped fields such as epoch, endpoint version, primary, healthy, or ready unless the scenario is explicitly about observing those read-only facts.

---

## §5 Driver Types

Initial driver types:

| Driver | Purpose | Current status |
|---|---|---|
| `shell` | Runs an existing script that reads normalized request and writes result. | Implemented as `internal/testops.ShellDriver`. |
| `go-test` | Runs `go test` package/focus commands and maps output to result. | Implemented as `internal/testops.GoTestDriver` at `f51b79b`. |
| `k8s` | Applies manifests, waits for resources, collects logs. | Can start as shell wrapper; later native. |
| `privileged-host` | Runs sudo/host OS checks and captures pre/post state. | Can start as shell wrapper. |
| `yaml` | Runs future ported V2 testrunner parser/engine. | Future; conditional. |

The first bridge can implement all non-shell drivers as shell wrappers. Native drivers are optimization and safety improvements, not prerequisites.

---

## §6 Scenario Lifecycle

To add a new V3 scenario:

1. Write or identify the backing test/harness.
2. Add registration file under `testops/registry/`.
3. Add/refresh QA instruction under `sw-block/design/test/`.
4. Add the scenario row to `v3-phase-15-testops-plan.md` §6.
5. Run through TestOps once and capture `result.json`.
6. Use that result as close evidence only if the scenario's non-claims match the gate claim.

To update a scenario:

1. Keep scenario name stable if the claim is unchanged.
2. Bump registration fields if driver/timeout/artifact shape changes.
3. Update `known_green_commit` only after verification.
4. Keep old artifact dirs; never mutate historical result directories.

---

## §7 Anti-Patterns

Do not:

1. Register a scenario that mutates product authority directly.
2. Encode `primary=true` / `healthy=true` as desired state in TestOps metadata.
3. Use static PV target facts as the default close path for G15b while claiming ControllerPublish evidence.
4. Let a YAML runner call V2 promote/demote or heartbeat-as-authority semantics.
5. Treat a `pass` result as broader than the scenario's non-claims.
6. Hide missing artifacts by returning `status=pass`.
7. Make production code import `internal/testops`.

The last rule is strict:

> Product code must not depend on TestOps. TestOps depends on product binaries and public/control surfaces.

---

## §8 Initial Registry Targets

| Scenario | Driver | Layer | Known green | Status |
|---|---|---|---|---|
| `g15b-manifest` | `go-test` | L1/L2 | `eb13105` | registered and executable at `f51b79b` |
| `g15b-k8s-static` | `shell` | L5 | `5375add` preflight only; K8s run pending | ready to register as pending-lab |
| `g15a-privileged` | `shell` | L3 | `ac49adb` | ready to register |
| `g15a-non-privileged` | `go-test` | L2 | `ac49adb` | ready to register |
| `g9g-l2` | `go-test` | L2 | `7ed9ab2` | ready to register |
| `g8-failover-l2` | `go-test` | L2 | `b320336` | needs instruction extraction |
| `g7-recovery-3scenarios` | `shell` | L4 | `d09fcc6` | wrap existing `g5-test` harness |

---

## §9 Slice 1 Result

`testops/registry/g15b-manifest.json` and a minimal `go-test` driver landed at `seaweed_block@f51b79b`.

Why this was first:

- It is non-privileged.
- It is fast.
- It proves the registration path without requiring K8s or m01.
- It gives QA an example registration file to copy.

Pass condition, verified on `p15-g15b/k8s-static-pv@f51b79b`:

```powershell
go test ./internal/testops ./cmd/blockcsi -count=1
```

The `internal/testops` suite includes the smoke that loads `g15b-manifest.json`, runs the registered scenario through `GoTestDriver`, and emits a valid `result.json` in a temp artifact dir.

Next slice: register `g15b-k8s-static` as a shell-driver scenario once the M02 rerun is green on the `eb13105+` manifest/harness fix.

---

## §10 Sign

| Role | Status | Basis |
|---|---|---|
| sw | draft | captured V3 pluggable TestOps path after `internal/testops` skeleton; Slice 1 executable registration landed at `f51b79b` |
| QA | pending | review registration shape and artifact expectations |
| architect | pending | ratify product/runtime non-plugin boundary |
