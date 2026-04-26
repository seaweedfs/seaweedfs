# G5-4 m01+M02 Cluster Bring-Up — Hand-off to sw

**Date**: 2026-04-26 (v0.3 — bring-up resolved; binary T4-wiring gap surfaced as G5-4 implementation work)
**Status**: ✅ Bring-up gate RESOLVED via sw's `--expected-slots-per-volume` flag (`seaweed_block@f5de7c5`) + `seaweedfs@e21c68693` answer doc. **NEW finding**: cmd/blockvolume binary lacks T4a-T4d replication wiring — the `host.go:73 ReplicationVolume` slot was added at T4a-5 but **never wired in main.go**. This is real G5-4 implementation work (150-300 LOC + design), not a config patch.
**From**: QA (rounds 2026-04-26 — local debug → m01+M02 verification → binary-wiring gap discovery)
**To**: sw + architect (G5-4 implementation kickoff input)
**Context**: G5-4 m01 hardware first-light per [g5-kickoff §3 batch G5-4](v3-phase-15-g5-kickoff.md). Skeleton script committed at `seaweed_block@eabafe8` (`scripts/iterate-m01-replicated-write.sh`).

---

## §00 v0.3 finding — binary T4 replication wiring is the actual G5-4 work

After bring-up resolved (§0 below), QA verified the cross-node 2-node cluster on m01+M02 (`192.168.1.181` + `192.168.1.184`):

```
m01 primary status: {"VolumeID":"v1","ReplicaID":"r1","Epoch":1,"EndpointVersion":1,"Healthy":true}
M02 replica log:    blockvolume: volume v1 authority is now r1@1 (not this replica r2);
                    recording supersede, not applying to adapter
                    blockvolume: durable open: frontend: volume not ready
```

Primary side fully Healthy; replica side stuck because **`cmd/blockvolume/main.go` has zero references to `ReplicationVolume / ReplicaListener / ReplicaPeer`**.

**Sw-confirmed root** (round 2026-04-26):
- `--t1-readiness` is primary-only by design (`core/host/volume/healthy_executor.go:10-28` godoc)
- `volume.Config.ReplicationVolume` slot exists (`host.go:73`) with godoc *"T4a-5 production wiring sets this"* — but **T4a-5 only added the field; the wiring never landed**
- T4d-4 part B wired `WithEngineDrivenRecovery()` for the **component test framework** (`cluster.go:357-369`), NOT for the binary
- Result: V3 components compose end-to-end (proven by T4d HARD GATE #3); the production binary still constructs a primary-only data plane

**Why this is real implementation work, not a quick patch** (per sw round 2026-04-26):

| Design decision | Options |
|---|---|
| Role inference (primary vs replica) | (a) From `AssignmentInfo.PrimaryReplicaID == self.ReplicaID`; (b) CLI flag operator-declared; (c) topology lookup deterministic |
| Peer discovery | From `AssignmentFact.Peers` (already exists per T4a-5 P-refined); wire peer-set updater on assignment events |
| Listener lifecycle | `ReplicaListener` bind addr (probably existing `--data-addr`); Stop on shutdown |
| Engine instantiation | One engine per volume, fed by assignment subscription |

**Estimate** (sw): 150-300 LOC + tests. **Real design work, not 50-LOC**. Same governance loop T4 used: kickoff → architect ratify → mini-plan → architect ratify → G-1 (vs T4d-4 part B component-framework wiring as V3-native PORT source) → code.

**G5-4 kickoff revision needed** — see `v3-phase-15-g5-kickoff.md` revision (binary-wiring promoted to its own batch ahead of m01 hardware first-light, since the script requires the binary).

**Acceptable interim**: primary-only smoke per sw's option C (G5 scenarios that don't need replica writes — e.g., walstore cadence under sustained primary load) can proceed without the binary wiring. Anything involving real cross-node replication waits.

---

## §0 Resolution (added 2026-04-26 v0.2)

**Root cause** (found via local Windows reproduction — m01/M02 was unnecessary for this debug):

`cmd/blockmaster/main.go:39`:
```
fs.IntVar(&f.expectedSlotsPerVol, "expected-slots-per-volume", 3,
    "RF/expected slot count per volume; the controller rejects observation
     snapshots whose slot count differs (default 3, set to 2 for 2-node smoke clusters)")
```

QA's 2-node topology (r1+r2) had **2 slots**; default `expected-slots-per-volume = 3`. Controller silently rejected the observation snapshot → no assignment minted → volumes stuck at "volume not ready".

**Fix**: add `--expected-slots-per-volume 2` to the blockmaster command for 2-node test clusters.

**Verification (local single-node, 3rd attempt):**

```
---master log---
{"component":"blockmaster","phase":"listening","addr":"127.0.0.1:9180"}
---primary log---
{"component":"blockvolume","phase":"status-listening","status_addr":"127.0.0.1:9290"}
{"component":"blockvolume","phase":"assignment-received","volume_id":"v1","replica_id":"r1","epoch":1,"endpoint_version":1}
2026/04/26 10:39:35 storage: recovery defensive scan (head==tail=0 checkpoint=0)
blockvolume: durable recovered: recovered LSN=0
---primary status---
{"VolumeID":"v1","ReplicaID":"r1","Epoch":1,"EndpointVersion":1,"Healthy":true}
```

✅ Primary fully Healthy + assignment received + durable opened.

**Lesson learned (added to QA process)**: for V3 bring-up debug, **try single-node local reproduction first** before ssh'ing to m01/M02. The "cluster bring-up gate" is V3 logic, not network topology — local reproduces same failure mode in seconds, with full Read/Edit/grep access to source code.

---

## §0a Secondary finding — non-primary replica doesn't reach "Healthy" via T1 path

When the topology has 2 slots (r1 primary, r2 replica), the replica's `--t1-readiness` HealthyPathExecutor sees the assignment but logs:

```
blockvolume: volume v1 authority is now r1@1 (not this replica r2);
  recording supersede, not applying to adapter
```

→ replica's adapter projection never reaches `Healthy=true` → replica stays at `volume not ready`.

This is **architecturally correct** for the T1 minimum-readiness scope (T1 only handles the primary case per `core/host/volume/healthy_executor.go:10-28` godoc). For G5-4's replica-side bring-up, the script must wire the actual T4a-T4d **ReplicationVolume + ReplicaPeer + ReplicaListener** stack — NOT just `--t1-readiness` HealthyPathExecutor.

This is a **G5-4 implementation question for sw**: what's the equivalent flag/setup to bring up a replica that participates in the replication path (vs T1's primary-only path)? Likely needs:
- The full ReplicationVolume binding (already done via T4d-4 part B `ReplicationVolume↔adapter` wiring)
- A different executor than `HealthyPathExecutor` — or `HealthyPathExecutor` needs to handle the secondary case
- Possibly a `--enable-replication` flag or similar

This is the **next gap to surface** for G5-4. The 2-node bring-up itself works (proven above) — the replica just doesn't reach Healthy via T1. Real T4d engine-driven path needs different wiring.

---

## §1 What I'm asking sw to answer

**Question**: what's the canonical V3 flow to bring a 2-node cluster from cold-start to "primary + replica both healthy"?

My attempt below got both volumes stuck at `blockvolume: durable open: frontend: volume not ready`. The volumes connect to master successfully but never reach "ready" state.

**Specific gaps I need filled:**
1. Is there a missing CLI flag or config beyond what's listed in `--help`?
2. Does `topology.yaml` need fields beyond `volumes/slots/{replica_id,server_id}`?
3. Does master need an explicit "mint assignment" trigger, or does it fire automatically from topology + observed heartbeats?
4. Is there a settling period > 4 seconds expected before "ready"?
5. Is there example bring-up test code I can reference (e.g., sparrow integration test, or `cmd/blockmaster/*_test.go`)?

---

## §2 Infrastructure verified READY (no action needed)

| Layer | Status | How verified |
|---|---|---|
| m01 + M02 reachability | ✅ | `ping 192.168.1.184` from m01 = 0.92ms |
| SMB share cross-node binary distribution | ✅ | `v:/share` on Windows = `/mnt/smb/work/share/` on both Linux nodes |
| Binary execution on M02 | ✅ | M02 ran `blockvolume --help` from SMB share without rebuild |
| Master cross-node listen | ✅ | `blockmaster --listen 0.0.0.0:9180` bound; `ss -tlnp` confirms |
| Network reachability primary↔master, replica↔master | ✅ | Both `blockvolume` processes connected to master without error |

---

## §3 What I ran (copy-paste reproducible)

### 3.1 Build binaries on m01 + drop to SMB share

```bash
ssh -i /c/work/dev_server/testdev_key testdev@192.168.1.181 "
cd /opt/work/seaweed_block_t4d4 && \
go build -o /mnt/smb/work/share/g5-binaries/blockvolume ./cmd/blockvolume/ && \
go build -o /mnt/smb/work/share/g5-binaries/blockmaster ./cmd/blockmaster/
"
```

**Result**: ✅ both binaries built successfully (16 MiB blockmaster, 18 MiB blockvolume)

### 3.2 Verify M02 can execute the binary

```bash
ssh -i /c/work/dev_server/testdev_key testdev@192.168.1.184 \
  "/mnt/smb/work/share/g5-binaries/blockvolume --help 2>&1 | head -3"
```

**Result**: ✅ `Usage of blockvolume: -ctrl-addr string ...` (executes from SMB share without scp)

### 3.3 Setup directories + topology YAML on m01

```bash
ssh -i /c/work/dev_server/testdev_key testdev@192.168.1.181 \
  'mkdir -p /tmp/g5sm/{master-store,primary-durable,logs}'

ssh -i /c/work/dev_server/testdev_key testdev@192.168.1.181 \
  "printf 'volumes:\n  - volume_id: v1\n    slots:\n      - replica_id: r1\n        server_id: m01-primary\n      - replica_id: r2\n        server_id: m02-replica\n' > /tmp/g5sm/topology.yaml && cat /tmp/g5sm/topology.yaml"
```

**Result**: ✅ topology.yaml created; schema deduced from `cmd/blockmaster/topology.go:30-40` struct tags

```yaml
volumes:
  - volume_id: v1
    slots:
      - replica_id: r1
        server_id: m01-primary
      - replica_id: r2
        server_id: m02-replica
```

### 3.4 Start blockmaster on m01

```bash
ssh -i /c/work/dev_server/testdev_key testdev@192.168.1.181 \
  "nohup /mnt/smb/work/share/g5-binaries/blockmaster \
    --authority-store /tmp/g5sm/master-store \
    --listen 0.0.0.0:9180 \
    --topology /tmp/g5sm/topology.yaml \
    --t0-print-ready \
    > /tmp/g5sm/logs/master.log 2>&1 </dev/null & disown
sleep 2
tail -10 /tmp/g5sm/logs/master.log"
```

**Result**: ✅ blockmaster running, listening on `[::]:9180`

```
2026/04/26 10:23:25 durable authority lock acquired (store="/tmp/g5sm/master-store")
2026/04/26 10:23:25 durable authority reload: 0 records
2026/04/26 10:23:25 blockmaster: lock acquired, reloaded=0, listen=[::]:9180
{"component":"blockmaster","phase":"listening","addr":"[::]:9180"}
```

### 3.5 Start primary blockvolume on m01

```bash
ssh -i /c/work/dev_server/testdev_key testdev@192.168.1.181 \
  "nohup /mnt/smb/work/share/g5-binaries/blockvolume \
    --master 127.0.0.1:9180 \
    --server-id m01-primary --volume-id v1 --replica-id r1 \
    --ctrl-addr 0.0.0.0:9210 --data-addr 0.0.0.0:9220 \
    --durable-root /tmp/g5sm/primary-durable --durable-impl walstore \
    --durable-blocks 16384 --durable-blocksize 4096 \
    --status-addr 127.0.0.1:9290 \
    --t0-print-ready --t1-readiness \
    > /tmp/g5sm/logs/primary.log 2>&1 </dev/null & disown
sleep 4
tail -15 /tmp/g5sm/logs/primary.log"
```

**Expected**: primary registers with master, master mints assignment from topology.yaml, primary opens its durable storage at `/tmp/g5sm/primary-durable`, status endpoint binds at `127.0.0.1:9290`.

**Actual**:
```
{"component":"blockvolume","phase":"status-listening","status_addr":"127.0.0.1:9290"}
blockvolume: durable open: frontend: volume not ready
```

⚠ **Status endpoint never bound** (port 9290 not listening; `curl 127.0.0.1:9290` returns `connect refused`).

### 3.6 Start replica blockvolume on M02

```bash
ssh -i /c/work/dev_server/testdev_key testdev@192.168.1.184 \
  "mkdir -p /tmp/g5sm/{replica-durable,logs} && \
nohup /mnt/smb/work/share/g5-binaries/blockvolume \
    --master 192.168.1.181:9180 \
    --server-id m02-replica --volume-id v1 --replica-id r2 \
    --ctrl-addr 0.0.0.0:9211 --data-addr 0.0.0.0:9221 \
    --durable-root /tmp/g5sm/replica-durable --durable-impl walstore \
    --durable-blocks 16384 --durable-blocksize 4096 \
    --status-addr 127.0.0.1:9290 \
    --t0-print-ready --t1-readiness \
    > /tmp/g5sm/logs/replica.log 2>&1 </dev/null & disown
sleep 4
tail -15 /tmp/g5sm/logs/replica.log"
```

**Actual**: same shape as primary — only `status-listening` log emitted then process stuck. Same `volume not ready` likely.

### 3.7 Master log after both volumes connected

```bash
ssh -i /c/work/dev_server/testdev_key testdev@192.168.1.181 \
  "tail -25 /tmp/g5sm/logs/master.log"
```

**Result**: master log unchanged from 3.4 — **no "heartbeat from m01-primary" or "heartbeat from m02-replica" log lines**, no "minting assignment" log lines. Either:
- (a) volumes never sent heartbeats successfully (but they connected per their logs), or
- (b) master heartbeat-receive path doesn't log by default, or
- (c) master expects something more than passive topology to mint assignments

```
2026/04/26 10:23:25 durable authority lock acquired (store="/tmp/g5sm/master-store")
2026/04/26 10:23:25 durable authority reload: 0 records
2026/04/26 10:23:25 blockmaster: lock acquired, reloaded=0, listen=[::]:9180
{"component":"blockmaster","phase":"listening","addr":"[::]:9180"}
```

### 3.8 Cleanup

```bash
ssh -i /c/work/dev_server/testdev_key testdev@192.168.1.181 \
  "sudo pkill -9 -f blockmaster 2>/dev/null; sudo pkill -9 -f blockvolume 2>/dev/null"
ssh -i /c/work/dev_server/testdev_key testdev@192.168.1.184 \
  "sudo pkill -9 -f blockvolume 2>/dev/null"
```

**Result**: ✅ both nodes clean

---

## §4 What I think the gap is (sw to confirm or correct)

The error `blockvolume: durable open: frontend: volume not ready` happens BEFORE the status endpoint binds, BEFORE durable storage opens. The blockvolume seems to be waiting for an assignment-from-master before completing initialization.

If that's correct, then either:
1. **Master needs to actively mint + push** assignment to volumes (not just have topology loaded passively), and there's a trigger I'm missing
2. **Volume needs to wait long enough** for master heartbeat → topology resolution → assignment dispatch (4s wasn't enough, but how long is right?)
3. **Topology YAML needs more** (e.g., `expected_servers` section, `epoch`, `endpoint_version`, or other authority fields)
4. **There's a bootstrap admin command** to trigger initial assignment dispatch

A quick way to find out: sw can point me at any working bring-up integration test (probably in `core/replication/integration_*_test.go` or `cmd/blockmaster/*_test.go`) that brings a multi-node cluster up. I'll mirror its pattern in the script.

---

## §5 What sw can do to debug

### 5.1 Run the same sequence on m01 with fresher eyes

All commands in §3 are copy-pasteable. The setup is reproducible:
- Binaries at `/mnt/smb/work/share/g5-binaries/{blockmaster,blockvolume}` (built 2026-04-26 from `seaweed_block@e642ae8`+ working tree at part C land time; rebuild if needed for `a0be6d5` test fixture race fix)
- Working dirs at `/tmp/g5sm/` on both m01 and M02

### 5.2 Possible things to try

- Add `--log-level=debug` or similar verbose flag if blockvolume / blockmaster supports it
- Read `/tmp/g5sm/logs/{master,primary,replica}.log` after bring-up attempt — the silent failure suggests a log channel that's not flushing or not at default level
- Check if blockmaster needs explicit `slots[].expected` or `slots[].epoch` fields
- Check if there's a `blockadmin` CLI tool for triggering assignments

### 5.3 If a working bring-up sequence is available somewhere

Pointer to:
- An L3 integration test that spins up multi-node cluster
- An existing m01 script that brings up cluster (none in `seaweed_block/scripts/` other than my skeleton)
- `cmd/sparrow` test code (sparrow has integration tests; might include cluster bootstrap)
- Any documentation of the expected bring-up sequence

Will let me update the G5-4 script skeleton with the right pattern.

---

## §6 Where the G5-4 script skeleton lives

`seaweed_block@eabafe8` — `scripts/iterate-m01-replicated-write.sh` (272 LOC). Marked DRAFT v0.1.

Sections that work today (per §2 infra-verified):
- Config block (env-overridable)
- `sync_and_build` (build on m01, scp/SMB-share binary to M02)
- Helpers (log/die/collect_diagnostics)

Section blocked on this hand-off:
- `start_cluster` — currently has the same flags I used in §3.4–3.6 above; will fail same way until bring-up sequence is correct
- All scenario bodies (TODO-marked, depend on `start_cluster` working)

---

## §7 Once sw answers — QA next steps

1. Update `iterate-m01-replicated-write.sh` `start_cluster` with correct bring-up sequence
2. Verify the corrected sequence brings cluster to "primary + replica healthy" state
3. Author scenario bodies per G5-4 architect ratification (currently awaiting in `g5-kickoff.md` §8)
4. Run full matrix: walstore + smartwal × 4 scenarios = 8 runs

ETA after sw answers: ~half day to update + verify; scenario bodies depend on G5 mini-plan.
