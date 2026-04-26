# G5-4 m01+M02 Bring-Up — sw answer

**Date**: 2026-04-26
**From**: sw
**To**: QA
**Refs**: [v3-phase-15-g5-m0102-bringup-handoff.md](v3-phase-15-g5-m0102-bringup-handoff.md)

---

## §1 Root cause

`cmd/blockmaster/main.go:82` hardcoded `ExpectedSlotsPerVolume: 3`.

QA's topology has 2 slots (m01-primary + m02-replica). The TopologyController calls `validateVolumeTopology(snap, 3)` on every observed-state snapshot. With 2 slots in the snapshot, validation returns `topology: volume v1 has 2 slots, want 3` and the entire `SubmitObservedState` call returns the error — no assignments minted, no per-volume processing.

That error propagates back to the caller (observation host) but the controller doesn't log it on the master log channel. **That's why §3.7 master log was silent** — the controller was rejecting every snapshot, but the rejection went to a return value, not a log line.

Downstream chain (the symptom QA saw):
- master rejects snapshot → publisher never gets a per-volume line → no assignment ever fanned out
- volumes connect to master, subscribe to assignments, wait
- volume's `dp.Open()` calls `waitHealthy()` (10s timeout) → projection never flips Healthy → returns `frontend.ErrNotReady` → `blockvolume: durable open: frontend: volume not ready`
- volume exits before status server stays up

So none of QA's hypotheses (§4 #1–#4) was the active cause; the gap is one missing CLI flag.

## §2 Answers to §1 questions

| # | Question | Answer |
|---|---|---|
| 1 | Missing CLI flag? | **YES — `--expected-slots-per-volume`**. Just added (commit below). Default stays 3 (production RF=3); pass `2` for the 2-node smoke. |
| 2 | Topology YAML needs more fields? | No. The schema you used (volumes/slots/{replica_id, server_id}) is exactly right. |
| 3 | Master needs explicit "mint" trigger? | No. Master mints automatically once heartbeats arrive AND topology validation passes. Your heartbeats arrived; topology validation failed. |
| 4 | Settling period > 4s? | No. Once the slot-count gate passes, mint happens within one heartbeat round-trip (sub-second). Volumes will reach Healthy within ~3s. |
| 5 | Reference test? | `core/authority/convergence_route_test.go` (especially `TestConvergence_Route1_*` around line 200) shows the canonical heartbeat → mint → publish → subscribe chain. All those tests use 3-slot configs because the controller's invariant is "system targets RF=3 in production." |

## §3 The fix

Adding `--expected-slots-per-volume` flag to blockmaster (committed in seaweed_block working tree):

```
diff --git a/cmd/blockmaster/main.go b/cmd/blockmaster/main.go
@@ flags struct
+    expectedSlotsPerVol int
@@ parseFlags
+    fs.IntVar(&f.expectedSlotsPerVol, "expected-slots-per-volume", 3,
+        "RF/expected slot count per volume; the controller rejects observation
+         snapshots whose slot count differs (default 3, set to 2 for 2-node
+         smoke clusters)")
@@ run
-        ControllerConfig:  authority.TopologyControllerConfig{ExpectedSlotsPerVolume: 3},
+        ControllerConfig:  authority.TopologyControllerConfig{ExpectedSlotsPerVolume: f.expectedSlotsPerVol},
```

Build + tests pass. Ready to land.

## §4 Updated bring-up sequence for the 2-node smoke

Two changes vs §3 of the handoff:

1. **Rebuild the blockmaster binary** to pick up the new flag
2. **Pass `--expected-slots-per-volume 2`** to blockmaster

Updated §3.4:

```bash
ssh -i /c/work/dev_server/testdev_key testdev@192.168.1.181 \
  "nohup /mnt/smb/work/share/g5-binaries/blockmaster \
    --authority-store /tmp/g5sm/master-store \
    --listen 0.0.0.0:9180 \
    --topology /tmp/g5sm/topology.yaml \
    --expected-slots-per-volume 2 \
    --t0-print-ready \
    > /tmp/g5sm/logs/master.log 2>&1 </dev/null & disown"
```

§3.5 / §3.6 stay unchanged. With this flag, the bring-up sequence should reach steady state without further changes.

## §5 What QA should see post-fix

- §3.4 master log: same listening line as before
- §3.5/§3.6 primary + replica logs: after ~1–3s, additional lines beyond `status-listening`:
  - `assignment-received` (with epoch=1, ev=1) — only if `--t0-print-ready` was passed; volume is now MemberPresent
  - `durable recovered: ...` — replaces the old `durable open: frontend: volume not ready` error
  - status endpoint binds; `curl 127.0.0.1:9290` returns JSON
- §3.7 master log: heartbeat ingest is silent by default but `tail -f master.log` over a longer window may show authority store writes; the *evidence* that the master is working is the volume-side `assignment-received` log line, not master-log content

## §6 Followup beyond this smoke

The 2-node configuration is a smoke-test convenience. Production V3 targets RF=3. So:

- The default stays 3 (operators don't pass the flag in production)
- `--expected-slots-per-volume 2` is for QA scripts and L3-style smoke tests
- Long-term: per-volume RF (the validator should compare snapshot-slot-count against the *topology's* slot count for that volume, not a single global) — that's a real refactor, not a smoke fix. Carry to a separate ticket if needed.

## §7 Verification

I haven't run the full bring-up sequence on m01 myself (no clean way for me to run interactive ssh from this session). Recommend QA:

1. Pull/rebuild blockmaster on m01 from current `phase-15` tip + the new flag commit
2. Re-run §3.4–§3.7 with the updated `--expected-slots-per-volume 2` flag
3. Confirm volumes reach steady state

If steady state isn't reached, send me the post-fix master + primary + replica logs and I'll dig further.
