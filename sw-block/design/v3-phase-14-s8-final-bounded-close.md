# V3 Phase 14 S8 Final Bounded Close

Date: 2026-04-19
Status: draft
Purpose: close the bounded P14 internal topology/control-plane claim and hand off product surfaces to P15 without pretending P14 is production-ready block storage

## 1. Target

`P14 S8` closes one bounded internal control-plane product shape:

`heartbeat/observation -> ClusterSnapshot -> TopologyController -> Publisher durable authority -> VolumeBridge -> adapter/engine -> observed convergence/restart evidence`.

S8 is not another protocol slice and not a broad product surface slice. It is the final acceptance and evidence package for the internal single-active-master topology/control-plane loop built by S4-S7.

## 2. What S8 Must Prove

S8 must prove, at the appropriate test layer, that:

1. observation is system-fed, not test-authored
2. topology supportability is explicit and fail-closed
3. authority is durable, single-owner, and restart-recovered
4. controller decisions are system-driven for the accepted topology set
5. convergence has bounded fate: confirmed, stuck, or superseded
6. restart re-anchors current authority through the real bridge/adapter route
7. stale observation, stale old-slot delivery, and unsupported topology do not silently move the system backward
8. per-volume isolation holds under mixed supported/unsupported states
9. the accepted topology set and unsupported list are explicit
10. every product-facing gap is handed to P15, not hidden inside P14 closure

## 3. What S8 Does Not Prove

S8 does not prove:

1. CSI completion
2. iSCSI/NVMe product frontend completion
3. external volume lifecycle API completion
4. real user data-path production readiness
5. V2/V3 migration readiness
6. multi-master HA / leader election / distributed authority
7. final performance or soak readiness

Those are P15 tracks and the P15 final cluster validation gate.

S8 may prepare scenario shapes and port test runner muscle for those later gates, but it must not claim them as P14 closure.

## 4. Accepted Topology Claim

The S8 supported topology claim is bounded to:

1. single active master
2. multiple volumes
3. three bounded replica slots per volume
4. distinct server per slot
5. one current authoritative primary
6. two bounded failover/rebalance candidates
7. publisher-owned `Epoch` / `EndpointVersion`
8. durable current authority line, one record per volume
9. passive convergence: publish, observe, confirm / stuck / supersede
10. real `VolumeBridge` delivery into the adapter/engine path

Anything outside this set is unsupported or deferred.

## 5. Required Evidence Matrix

S8 must produce a closure table with these columns:

| Claim | L0 Unit | L1 Component | L2 Process | L3 Scenario | Status | Residual |
|---|---|---|---|---|---|---|

Minimum claims:

1. observation supportability
2. duplicate/missing/conflicting inventory handling
3. durable authority reload
4. no old-slot durable revival
5. controller-driven bind/reassign/refresh
6. convergence confirm/clear
7. publish-not-observed stuck evidence
8. supersede by newer authority
9. restart re-anchor via `VolumeBridge`
10. stale observation cannot move backward
11. unsupported topology records evidence
12. per-volume isolation
13. placement/rebalance within accepted topology
14. P15 handoff gaps identified

## 6. Required Test Levels

### L0 Unit / Invariant

Required command baseline:

```text
go test ./core/engine ./core/adapter ./core/authority -count=1
```

Must include boundary guards for:

1. no non-authority `AssignmentInfo` minting
2. no observation code constructing assignment asks
3. no store minting authority
4. no hidden adapter ingress

### L1 Component

Required routes:

1. `ObservationHost -> TopologyController -> Publisher -> VolumeBridge -> VolumeReplicaAdapter`
2. observation-fed failover confirms and clears desired
3. stuck evidence appears without ask churn
4. supersede drops stale desired
5. restart route reloads and re-anchors authority
6. old-slot live delivery is rejected by adapter monotonicity
7. unsupported topology records evidence

### L2 Local Process

Required shapes:

1. real `sparrow` durable authority restart smoke
2. process-level S4-S7 route smoke if the current binary surface can expose it cleanly
3. if process route smoke cannot be added without inventing P15 surfaces, S8 must record the blocker and carry it to P15 T1/T2/T3

L2 tests must check exit code, artifact/log output, lock release, reload count, and no backward mint.

### L3 Hardware / Scenario

S8 should port scenario shapes from V2 and classify each as:

1. runnable in P14 internal mode
2. blocked by missing P15 frontend/API/data-path surface
3. deferred to P15 final cluster validation

Initial scenario candidates:

1. restart recovery
2. failover
3. partition / delayed heartbeat
4. unsupported topology
5. IO continuity only if frontend/data path exists

S8 can close P14 with L3 blockers only if the blockers are genuinely P15 product-surface gaps, not internal truth gaps.

## 7. V2 Port Plan

### Classify now (S8 scope), port deferred to P15

S8 **does not port testrunner machinery into the V3 tree** (round-2 doc-consistency fix, aligned with `v3-phase-14-s8-v2-scenario-classification.md` §2). The items below are the V2 muscles S8 inventories and classifies; actual porting is a P15 Final Gate (Cluster Validation Agent) deliverable, not an S8 one.

1. `weed/storage/blockvol/testrunner/`
   - **classify now**: runner architecture, scenario YAML shape, action vocabulary, artifact collection, report generation — recorded as PORT-MECHANISM in the classification table. **Port in P15 Final Gate.**

2. `weed/storage/blockvol/testrunner/scenarios/public` and selected internal scenarios
   - **classify now**: map each scenario shape to RUNNABLE-P14 / BLOCKED-FRONTEND / BLOCKED-OPS / BLOCKED-HA / BLOCKED-PERF. **Port in P15 Final Gate.**

3. `weed/storage/blockvol/test/component/`
   - **classify now**: component harness structure and failure-injection style. **Port in P15 T1 Frontend + Data Path** as that track's real-route harness.

4. `weed/server/qa_block_*`
   - **classify now**: coverage taxonomy and route scenario ideas as reference for the evidence matrix (§5). **Port only as applicable per P15 track needs.**

5. `learn/test` evidence convention
   - **classify now**: manifest / result / report / run-notes convention referenced as the target shape for future P15 evidence bundles. **Adopt when P15 ships real L3/L4 runs.**

### Do not port

1. `promotion.go`
2. `HandleAssignment` / `promote` / `demote`
3. local runtime self-promotion
4. heartbeat timing directly becoming authority
5. V2 authority ownership or HA assumptions
6. CSI/iSCSI/NVMe product integration as P14 close criteria

## 8. Deliverables

S8 must deliver:

1. final P14 supported-topology statement
2. final P14 unsupported/deferred list
3. acceptance evidence matrix
4. scenario-port classification table
5. tester sign-off packet template
6. 14A final targeted review checklist
7. P15 handoff statement mapping every remaining gap to a P15 track
8. code/tests only where needed to make the evidence route real

## 9. Reject Conditions

Reject S8 if:

1. closure is based only on local unit tests
2. a claimed route still uses manual assignment choreography
3. S8 invents CSI/API/operator surfaces to make tests look complete
4. unsupported topology becomes silent idle
5. stale observation can emit a newer decision from an old basis
6. durable restart can revive stale old-slot truth
7. convergence can churn epochs without observation confirmation
8. S8 claims production readiness without P15 frontend/data-path/security/cluster validation

## 10. Closure Statement Shape

The final S8 closure statement must say exactly:

1. what P14 now owns
2. what P14 explicitly does not own
3. what evidence exists at L0/L1/L2/L3
4. what risks remain
5. which P15 track owns each product gap

The expected final claim is:

`P14 has closed one bounded single-active-master internal topology/control-plane truth loop for the accepted topology set. P14 has not closed CSI, external API, frontend data path, migration, security, deployment, performance, or production readiness.`
