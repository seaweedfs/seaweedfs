# V3 Phase 15 G15a QA Test Instruction

Date: 2026-05-03
Status: non-privileged L2 close-candidate on `p15-g15a/csi-static-mvp@ac49adb`
Scope: blockcsi static MVP control-plane path through real product daemons; non-privileged only

## Headline

At `p15-g15a/csi-static-mvp@ac49adb`, G15a proves a real `blockcsi` driver can
serve CSI Identity, look up assigned frontend facts from a real `blockmaster`
status path, and return a populated `publish_context` from `ControllerPublishVolume`
that matches the actual `iscsiAddr` and `iqn` exposed by a real `blockvolume`
iSCSI target.

This instruction covers what runs on plain CI / Windows / unprivileged Linux.
Privileged steps (real `iscsiadm` discovery+login, `mkfs`, `mount`, file write/read)
require an authenticated Linux test host and are out of scope for this instruction;
see §Non-claims and §Forward-carry.

## Environment

- Repo: `seaweed_block`
- Branch: `p15-g15a/csi-static-mvp`
- Minimum commit: `ac49adb`
- Fidelity: subprocess L2 with real `cmd/blockmaster` + real `cmd/blockvolume` (×2 for r1/r2) + real `cmd/blockcsi`
- Platform: any Go-capable host (Windows, macOS, plain Linux without iscsiadm); no `root` / privileged container required
- Go version: project default (CGO_ENABLED=1 on Windows; no `-race` flag on Windows per project policy)

## Full Regression Command

```bash
go test ./core/csi ./cmd/blockcsi ./core/host/volume ./core/host/master ./core/authority ./cmd/blockmaster ./cmd/blockvolume -count=1
```

Expected: all 7 packages PASS.

Wall-clock guidance on a typical dev box:

| Package | Approx. time |
|---|---|
| `core/csi` | < 0.1 s |
| `cmd/blockcsi` | ~25-30 s (includes L2 build + subprocess spawn) |
| `core/host/volume` | < 0.1 s |
| `core/host/master` | < 0.2 s |
| `core/authority` | ~7 s |
| `cmd/blockmaster` | < 0.1 s |
| `cmd/blockvolume` | ~120-150 s (includes G8 + G9 subprocess tests) |
| **Total** | ~3 min |

## Focused G15a L2 Command

```bash
go test ./cmd/blockcsi -run TestG15a_BlockCSIControllerPublishUsesMasterFrontendFact -count=1 -v
```

Expected: PASS in ~20-25 s on a local dev box. Includes building all three binaries
(`blockmaster`, `blockvolume`, `blockcsi`) in a temp dir, spawning them, polling
`ControllerPublishVolume` for up to 15 s until the published `iscsiAddr`/`iqn`
match the values the test seeded into the volume.

## Scenario Checklist

Each scenario lists the backing test so dev agents can re-run a single scenario
in isolation when iterating.

### CSI service-layer scenarios (component, fast)

1. **Identity service handles nil requests safely.**
   - `core/csi/identity_endpoint_test.go::TestIdentity_NilRequests`
   - `core/csi/identity_endpoint_test.go::TestParseEndpoint`

2. **CSI package does not import authority and does not construct AssignmentFacts.**
   Structural guard from `v3-phase-15-control-plane-evolution.md §7.1`.
   - `core/csi/boundary_guard_test.go::TestG15a_CSI_DoesNotImportAuthorityOrConstructAssignmentFacts`

3. **`ControllerPublishVolume` returns iSCSI publish_context built from a verified target fact.**
   - `core/csi/controller_test.go::TestControllerPublish_ReturnsISCSIPublishContextFromTargetFact`

4. **`ControllerPublishVolume` fails closed when no verified target exists.**
   - `core/csi/controller_test.go::TestControllerPublish_FailsClosedWithoutVerifiedTarget`

5. **`ControllerPublishVolume` fails closed when the target has no frontend fact attached.**
   - `core/csi/controller_test.go::TestControllerPublish_FailsClosedWhenTargetHasNoFrontendFact`

6. **`ControllerPublishVolume` propagates backend lookup errors as `Internal`.**
   - `core/csi/controller_test.go::TestControllerPublish_PropagatesLookupErrorsAsInternal`

7. **Controller capabilities do not advertise dynamic provisioning at G15a.**
   - `core/csi/controller_test.go::TestControllerCapabilities_G15aDoesNotAdvertiseDynamicProvisioning`

8. **`ValidateVolumeCapabilities` confirms an existing target without minting state.**
   - `core/csi/controller_test.go::TestValidateVolumeCapabilities_ConfirmsExistingTarget`

9. **`NodeStage` honors `publish_context` over `volume_context` when both are present.**
   - `core/csi/node_test.go::TestNodeStage_UsesPublishContextBeforeVolumeContext`

### Master backend lookup scenarios (component, fast)

10. **Master backend maps iSCSI status frontend into `TargetFact`.**
    - `core/csi/master_backend_test.go::TestControlStatusLookup_MapsISCSIStatusFrontend`

11. **Master backend maps NVMe status frontend into `TargetFact`.**
    - `core/csi/master_backend_test.go::TestControlStatusLookup_MapsNVMeStatusFrontend`

12. **Master backend fails closed when no frontend has been assigned.**
    - `core/csi/master_backend_test.go::TestControlStatusLookup_FailClosedWithoutAssignedFrontend`

### L2 subprocess scenario (the headline)

13. **Real `blockmaster` + `blockvolume` (×2) + `blockcsi` ⇒ `ControllerPublish` returns the volume's actual frontend fact.**
    - `cmd/blockcsi/main_test.go::TestG15a_BlockCSIControllerPublishUsesMasterFrontendFact`
    - This is the headline integration scenario. The test:
      - builds all three binaries in `t.TempDir()`,
      - launches `blockmaster`,
      - launches `blockvolume` r1 with seeded iSCSI listen address and IQN,
      - launches `blockvolume` r2,
      - launches `blockcsi --master <addr>`,
      - polls `ControllerPublishVolume(VolumeId="v1", NodeId="node-a")` for up to 15 s,
      - asserts `publish_context["iscsiAddr"]` equals r1's seeded `iscsiAddr` and `publish_context["iqn"]` equals r1's seeded IQN.
    - Pass condition: published context matches seeded values within the deadline.
    - Skip condition: `-short` flag.

### Binary smoke scenario

14. **`blockcsi` binary starts and serves Identity.**
    - `cmd/blockcsi/main_test.go::TestBlockCSI_BinaryStartsAndServesIdentity`

## Non-claims (this instruction does not validate)

The following are explicitly **out of scope** for this instruction. Do not assume
they pass on the basis of this checklist:

1. **Real `iscsiadm` discovery / login** — requires Linux + initiator + privileged execution.
2. **Real `mkfs.<fs>` against the attached block device** — requires kernel block device + privileged execution.
3. **Real `mount` / bind-mount into a target path** — requires privileged execution + mount namespace.
4. **Filesystem write/read of test files through the mounted CSI volume** — requires the full privileged path above.
5. **`NodeUnstage` / `NodeUnpublish` cleanup against real OS state** — requires the full privileged path above.
6. **Kubernetes CSI driver registration / kubelet integration** — requires a real K8s node.
7. **Multiple concurrent `ControllerPublish` calls / N-node `NodeStage` fan-out** — single happy path only.
8. **Snapshot / clone / resize CSI verbs** — G15a does not advertise these capabilities.
9. **CSI security / RBAC / TLS** — beta-defer until G16.
10. **Performance / SLO** — G21.

## Forward-carry (next gate / next environment)

| Item | Owner | Where it lands |
|---|---|---|
| **G15a-5 M01 privileged**: real `iscsiadm` discovery+login, `mkfs`, `mount`, file write/read byte-equal, `NodeUnstage`/`NodeUnpublish` cleanup | sw + QA on M01 | next G15a sub-step; uses the same `cmd/blockcsi` binary, just on a privileged Linux host |
| **CSI dynamic provisioning** (`CreateVolume`/`DeleteVolume`) | sw | post-G15a / G15b mini-plan |
| **CSI snapshot/clone/resize** | sw | G10 / G11 / G15b separate gates |
| **CSI security (TLS / token)** | sw | G16 |
| **Kubernetes integration test** (real kubelet, real PVC, real pod) | QA | G15a-K8s sub-step on M01 or dedicated K8s test cluster |

## Close sentence

> *G15a non-privileged L2 proves the V3 product loop produces an assignment, blockvolume exposes a real iSCSI frontend fact, and `blockcsi`'s `ControllerPublishVolume` correctly retrieves attach information from master status — all through real product daemons, with no privileged OS operations.*

## QA evidence on this instruction (one-time pin)

```
Tested by: QA
Date: 2026-05-03
Tree: p15-g15a/csi-static-mvp@ac49adb
Focused L2: TestG15a_BlockCSIControllerPublishUsesMasterFrontendFact PASS 22.13s
Full 7-pkg regression: all PASS
Worktree: C:/work/seaweed_block_g9c
Platform: Windows 11 Pro (Go default, CGO_ENABLED=1, no -race per project policy)
```

Re-run on a different host or commit will produce a fresh evidence pin; this
block is the canonical pin for the close-candidate state at `ac49adb`.
