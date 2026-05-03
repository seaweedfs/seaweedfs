# V3 Phase 15 G15a Privileged QA Test Instruction

Date: 2026-05-03
Status: privileged-path verified on m01 at `p15-g15a/csi-static-mvp@ac49adb`
Scope: real `iscsiadm` discovery+login + `mkfs.ext4` + `mount` + bind-mount + filesystem write/read byte-equal + clean teardown, all driven through `cmd/blockcsi` NodeStage / NodePublish against real product daemons
Companion: `v3-phase-15-g15a-qa-test-instruction.md` (non-privileged L2 control-plane verification)

## Headline

At `p15-g15a/csi-static-mvp@ac49adb`, on Linux host m01 (192.168.1.181, Ubuntu
6.17 kernel, open-iscsi, sudo NOPW), G15a's `cmd/blockcsi` binary drives the
full privileged data-plane sequence end-to-end: `iscsiadm` discovery, login,
device-by-IQN wait, `mkfs.ext4`, mount-to-staging, bind-mount-to-target, 4KB
random byte-equal write/read through the mounted device, `NodeUnpublish`
(unmount target), `NodeUnstage` (unmount staging + iscsiadm logout),
`ControllerUnpublish`, with no dangling iSCSI sessions remaining.

## Environment

- **Repo**: `seaweed_block`
- **Branch**: `p15-g15a/csi-static-mvp`
- **Minimum commit**: `ac49adb`
- **Host**: m01 (192.168.1.181) — Linux 6.17, open-iscsi, sudo NOPW for `testdev`
- **SSH**: `ssh -i C:/work/dev_server/testdev_key testdev@192.168.1.181`
- **SMB harness path**: `V:\share\g15a-priv\` (Windows) = `/mnt/smb/work/share/g15a-priv/` (Linux)
- **Go**: 1.26.2 (project default)
- **Privileges required**: root (test invokes `sudo` and runs the test binary as root so `iscsiadm` and `mount` work)
- **Why m01 specifically**: the only host in our test fleet with kernel iSCSI initiator + sudo NOPW + clean baseline; G15a-K8s (real kubelet PVC) is forward-carry to a dedicated K8s test cluster

## Pre-conditions

Before running, verify on m01:

```bash
ssh -i C:/work/dev_server/testdev_key testdev@192.168.1.181 '
  which iscsiadm mkfs.ext4 sudo go &&
  go version &&
  sudo -n true && echo SUDO_NOPW_OK &&
  sudo iscsiadm -m session 2>&1 | head -3
'
```

Expected:

- `iscsiadm`, `mkfs.ext4`, `sudo`, `go` all resolved
- Go ≥ 1.24
- `SUDO_NOPW_OK` printed
- `iscsiadm -m session` reports `No active sessions` (or no session for `iqn.2026-05.io.seaweedfs:g15a-priv-v1`)

If any baseline iSCSI session exists for the test IQN, log out before running:

```bash
sudo iscsiadm -m node -T iqn.2026-05.io.seaweedfs:g15a-priv-v1 --logout || true
```

## Run command

### One-shot via the harness runner with persistent artifacts

The harness at `V:\share\g15a-priv\` contains:

- `privileged_test.go` — the Go test runner (single file, ~250 LOC). Reads `G15A_ARTIFACT_DIR` env var; if set, all per-daemon logs land there instead of `t.TempDir()`.
- `run-g15a-privileged.sh` — bash wrapper that builds binaries, stages the test, compiles, runs under sudo, prints baseline + post-run iSCSI sessions, and stages logs to a per-run artifact directory.

Ship a fresh source tree to m01 and invoke with a persistent run directory:

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"

# from your dev box (Windows or Linux)
git -C /path/to/seaweed_block archive --format=tar HEAD | \
  ssh -i C:/work/dev_server/testdev_key testdev@192.168.1.181 \
    'rm -rf /tmp/g15a-priv/src && mkdir -p /tmp/g15a-priv/src && tar x -C /tmp/g15a-priv/src'

# trigger the run on m01 with persistent artifact dir
ssh -i C:/work/dev_server/testdev_key testdev@192.168.1.181 "
  cp /mnt/smb/work/share/g15a-priv/privileged_test.go /tmp/g15a-priv/src/cmd/blockcsi/privileged_test.go
  cd /tmp/g15a-priv/src
  go build -o /tmp/g15a-priv/bin/blockmaster ./cmd/blockmaster
  go build -o /tmp/g15a-priv/bin/blockvolume ./cmd/blockvolume
  go build -o /tmp/g15a-priv/bin/blockcsi ./cmd/blockcsi
  go test -c -o /tmp/g15a-priv/blockcsi-priv.test ./cmd/blockcsi
  mkdir -p /mnt/smb/work/share/g15a-priv/runs/$RUN_ID
  sudo G15A_PRIVILEGED=1 G15A_BIN_DIR=/tmp/g15a-priv/bin \
       G15A_ARTIFACT_DIR=/mnt/smb/work/share/g15a-priv/runs/$RUN_ID \
       /tmp/g15a-priv/blockcsi-priv.test \
       -test.run TestG15aPriv -test.v -test.timeout=180s \
       2>&1 | tee /mnt/smb/work/share/g15a-priv/runs/$RUN_ID/test-stdout.log
"
echo "Artifacts at: V:\\share\\g15a-priv\\runs\\$RUN_ID\\"
```

Expected wall clock: ~10 s build + ~6 s test = ~16 s total. Artifacts persist
on the SMB share for inspection without an additional SSH round-trip.

> **Important**: the `G15A_ARTIFACT_DIR` env var is what makes logs survive a
> green test. Without it, `privileged_test.go` falls back to `t.TempDir()` and
> Go cleans up on success, leaving only the test stdout phase markers — no
> per-daemon logs to inspect.

### Artifact directory layout

After a green run, `V:\share\g15a-priv\runs\<RUN_ID>\` contains:

| File / dir | Size (typical) | Purpose |
|---|---|---|
| `test-stdout.log` | ~1 KB | Test framework output: 6 phase markers + PASS line. Read first to see the timeline. |
| `blockmaster.log` | ~270 B | Master daemon stderr. Quiet — startup, lock acquired, listen address. |
| `blockvolume-primary.log` | **~20 KB** | **Most informative log.** Contains: `g7-debug` adapter R/S/H state transitions, real iSCSI session lifecycle (kernel initiator IQN visible), every SCSI WRITE/SYNCHRONIZE_CACHE during `mkfs.ext4`, replication barrier waits with r2 at each sync, the test payload write at `lba=0`, and session invalidate on cleanup. |
| `blockvolume-replica.log` | ~270 B | r2 observes r1 is primary, records supersede, doesn't apply locally. |
| `topology.yaml` | ~130 B | The 2-slot RF=2 spec the master loaded. |
| `staging/` `store-primary/` `store-replica/` `authority-store/` | empty post-cleanup | Mount points + durable roots. Empty after `NodeUnstage` — that emptiness is itself evidence cleanup worked. |

**Currently missing**: `blockcsi.log` (the CSI driver's own iscsiadm/mount call log) is **not** in the persisted dir. The current harness writes it to `t.TempDir()` directly, bypassing `G15A_ARTIFACT_DIR`. Forward-carry: extend `startG15aCSI` helper to honor `art` parameter (3-line change). Workaround until then: wrap with `strace -f -e trace=execve` to capture iscsiadm/mkfs/mount syscalls.

### What the dev agent can verify directly from the logs

From `blockvolume-primary.log`:

1. **Real kernel iSCSI initiator attached** (look for):
   ```
   session: FullFeature initiator="iqn.2004-10.com.ubuntu:01:..." target="iqn.2026-05.io.seaweedfs:g15a-priv-v1" type="Normal"
   ```
   (Discovery session before this gets reset by peer — normal two-phase iscsiadm behavior, not a bug.)

2. **mkfs.ext4 generated ~40 SCSI WRITEs** for filesystem metadata (search `iscsi: SCSI WRITE handler lba=`). The test payload write is the one with `lba=0 transferLen=1 dataOut=4096`.

3. **Every WRITE replicated to r2 with barrier handshake** — for the test payload at `lsn=42`:
   ```
   replication: OnLocalWrite volume=v1 lba=0 lsn=42 peers=1
   replication: ship ok peer=r2 lba=0 lsn=42
   replication: barrier wait peer=r2 addr=... targetLSN=42 epoch=1
   replication: barrier ack peer=r2 targetLSN=42 achievedLSN=42
   ```
   Proves byte-equal payload was both durable on primary AND replicated to r2 before the read.

4. **Cleanup observed** at the end:
   ```
   executor: invalidate session 1 for r2: replica peer closed
   ```

### Manual / debug invocation (no harness wrapper)

If you want to iterate on the test logic without re-shipping source, the test
file lives at `/mnt/smb/work/share/g15a-priv/privileged_test.go`. Edit it
directly via SMB, then on m01 run the inner block of the SSH heredoc above.

## Phase-by-phase scenario

The single test `TestG15aPriv_NodeStageBindWriteReadCleanup` drives 6 phases.
Each phase logs its own success line so you can see in `-test.v` output exactly
where a failure happened.

### Phase 1 — ControllerPublishVolume returns matching frontend fact

- Calls `ControllerPublishVolume(VolumeId=v1, NodeId=node-a)` against `blockcsi`.
- Polls up to 15 s; passes when `publish_context["iscsiAddr"]` and `["iqn"]` match the values seeded into `blockvolume`'s iSCSI listen flags.
- Log: `Phase 1 OK: ControllerPublish returned iscsiAddr=... iqn=...`

### Phase 2 — NodeStageVolume drives iscsiadm + mkfs + mount

- Calls `NodeStageVolume` with the `publish_context` from Phase 1.
- Internally `cmd/blockcsi` invokes `iscsiadm -m discovery` then `iscsiadm -m node ... --login`, waits for `/dev/disk/by-path/*<iqn>*` to appear, then `mkfs.ext4` and `mount` to the staging path.
- Verified by `mountpoint -q <staging>` (with `/proc/mounts` fallback).
- Log: `Phase 2 OK: NodeStage completed (iscsiadm discovery+login + mkfs + mount)` + `Phase 2 verified: <staging> is a mountpoint`

### Phase 3 — NodePublishVolume bind-mounts staging → target

- Calls `NodePublishVolume` to bind-mount staging into the target path.
- Verified by `mountpoint -q <target>`.
- Log: `Phase 3 OK: NodePublish completed (bind mount staging → target)`

### Phase 4 — Byte-equal write/read through the CSI mount

- Generates 4096 random bytes via `crypto/rand`.
- Writes to `<target>/g15a-priv-byteequal.bin`.
- Calls `sync` to flush page cache.
- Reads back, asserts byte-equal.
- Log: `Phase 4 OK: 4096 bytes round-tripped byte-equal through CSI-mounted iSCSI device`

### Phase 5 — Explicit cleanup chain

- `NodeUnpublishVolume` → unmount target.
- `NodeUnstageVolume` → unmount staging + `iscsiadm --logout`.
- `ControllerUnpublishVolume` → soft-tolerated (G15a static MVP may return Unimplemented; the test logs and continues).
- Log: `Phase 5 OK: NodeUnpublish + NodeUnstage clean`

### Phase 6 — No dangling iSCSI session

- Runs `iscsiadm -m session`, asserts the test IQN is **not** present in output.
- Log: `Phase 6 OK: no dangling iSCSI session for iqn.2026-05.io.seaweedfs:g15a-priv-v1`

## QA evidence pins

### Pin 1 — first verification (ephemeral logs, t.TempDir())

```
Tested by: QA
Date: 2026-05-03
Tree: p15-g15a/csi-static-mvp@ac49adb
Host: m01 (192.168.1.181), Linux 6.17, Go 1.26.2 linux/amd64
Result: PASS in 5.69s
Baseline iSCSI sessions: none
Post-run iSCSI sessions: none
Logs: ephemeral (t.TempDir, cleaned by Go on success)
```

### Pin 2 — re-run with persistent artifact dir

```
Tested by: QA
Date: 2026-05-03
Tree: p15-g15a/csi-static-mvp@ac49adb (same source)
Host: m01 (192.168.1.181), Linux 6.17, Go 1.26.2 linux/amd64
Result: PASS in 5.93s
Baseline iSCSI sessions: none
Post-run iSCSI sessions: none
Run ID: 20260503T153623Z
Artifact dir: V:\share\g15a-priv\runs\20260503T153623Z\
Persisted: test-stdout.log, blockmaster.log, blockvolume-primary.log (19 KB),
           blockvolume-replica.log, topology.yaml
Missing: blockcsi.log (forward-carry harness fix)
```

Phases all green in both runs:

1. ControllerPublish returned matching iscsiAddr + iqn
2. NodeStage (iscsiadm discovery+login + mkfs.ext4 + mount) + mountpoint verified
3. NodePublish (bind mount staging → target) + mountpoint verified
4. 4096 random bytes round-tripped byte-equal through CSI-mounted iSCSI device
5. NodeUnpublish + NodeUnstage clean (ControllerUnpublish acceptable Unimplemented at static MVP)
6. No dangling iSCSI session for the test IQN

```
Test file: V:\share\g15a-priv\privileged_test.go
Runner script: V:\share\g15a-priv\run-g15a-privileged.sh
```

Re-run on a different commit will produce a fresh evidence pin; the two pins
above are canonical for the privileged-path verification at `ac49adb`.

## Non-claims (this instruction does not validate)

1. **K8s integration** — no real `kubelet` / PVC / Pod / scheduler involved. Real kubelet integration is forward-carry to a dedicated K8s test cluster.
2. **Multi-node attach** — single node (m01), single ControllerPublish/NodeStage cycle. Multi-attach (RWX) is not tested.
3. **Partition table / filesystem variety** — only `ext4` direct on the device. No XFS, no LVM, no GPT partitioning.
4. **Block volume mode** — only mount-mode (filesystem) tested. Raw block-mode CSI capability is not exercised.
5. **Concurrent write/read across reconnect** — single sequential write+sync+read. Concurrent or post-restart write/read is not covered.
6. **Failover during stage** — primary is not killed mid-NodeStage. G8 covers primary-kill data continuity at the storage layer; CSI-layer failover under workload is forward-carry.
7. **Persistence across plugin restart** — test starts and stops the `cmd/blockcsi` plugin once; "restart plugin then unstage stale volume" is not exercised.
8. **CSI snapshot / clone / resize** — G15a static MVP does not advertise these capabilities; not relevant.
9. **Performance / IOPS / throughput** — single 4KB I/O, no perf claim. G21 owns SLO.
10. **CHAP / mutual auth / TLS** — beta-defer until G16.

## Forward-carry

| Item | Owner | Where it lands |
|---|---|---|
| **Plugin restart + stale-volume unstage**: kill `cmd/blockcsi` mid-staging, restart, verify `NodeUnstageVolume` cleans up | sw + QA on m01 | G15a-6 followup or G15a-K8s |
| **Multi-node attach (single-writer enforcement)**: second node attempts NodeStage same volume, expect rejection | QA on m01 + a second Linux host | G15a-RWO test (separate instruction) |
| **Block-mode capability**: NodeStage with `Block{}` access type instead of `Mount{}` | sw must wire through, QA verifies | G15a block-mode followup |
| **Real K8s kubelet driving the same flow**: PVC → Pod schedules → kubelet calls CSI → workload writes/reads | QA | G15a-K8s on dedicated cluster |
| **Failure injection during NodeStage** (e.g., iscsiadm timeout, mkfs failure) | sw + QA | G15a-fault followup |
| **Post-G8 failover under live mount**: kill primary while file is being written, verify CSI re-discovery / device re-attach behavior | sw + QA | G15a × G8 cross-gate test |
| **`blockcsi.log` capture in artifact dir**: extend `startG15aCSI` helper to honor `art` (3-line change) so the CSI driver's own iscsiadm/mount call log lands alongside daemon logs | QA harness | trivial follow-up; bundle with first edit to `privileged_test.go` |

## Cleanup / re-run safety

The test always registers `t.Cleanup` for both `NodeUnpublishVolume` and
`NodeUnstageVolume` so even on test failure mid-flight, the iSCSI session and
mounts get torn down. If a test panic somehow leaves residue, manual cleanup:

```bash
sudo umount /tmp/TestG15aPriv*/001/target 2>/dev/null || true
sudo umount /tmp/TestG15aPriv*/001/staging 2>/dev/null || true
sudo iscsiadm -m node -T iqn.2026-05.io.seaweedfs:g15a-priv-v1 --logout 2>/dev/null || true
sudo iscsiadm -m node -T iqn.2026-05.io.seaweedfs:g15a-priv-v1 -o delete 2>/dev/null || true
```

After cleanup, `sudo iscsiadm -m session` should report `No active sessions`.

## Close sentence

> *G15a privileged QA proves the V3 `cmd/blockcsi` binary correctly drives the full Linux block storage stack — kernel iSCSI initiator, ext4 format, kernel mount, bind-mount, filesystem I/O — against real V3 product daemons, with byte-equal data correctness and clean teardown leaving no kernel-level residue.*
