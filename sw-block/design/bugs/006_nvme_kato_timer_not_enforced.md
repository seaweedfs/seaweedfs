# BUG-006 — NVMe KATO timer stored but not enforced

**Status**: OPEN — filed 2026-04-22 during T3-end retrospective (T3-DEF-7 latent gap)
**Severity**: Medium — no correctness impact in short sessions; causes zombie sessions + KATO-based liveness contract violation under long-running / network-partition scenarios
**Component**: `core/frontend/nvme/session.go` (Keep-Alive Timeout handling)
**Discovered-by**: QA retrospective of T3-DEF-5/6/7 review round
**Discovered-when**: 2026-04-22
**Owner**: sw
**Target track**: G21 (perf / hardening gate) OR post-T3 cycle — NOT T3
**Blocks**: not T3; tracks for next NVMe hardening window

---

## 1. One-line symptom

Host-advertised KATO (Keep-Alive Timeout, seconds) is parsed + stored on admin Connect, but the session has no watchdog goroutine that tears down the connection when the host stops sending Keep-Alive admin commands within the KATO window. A dead host thus leaves a zombie session holding CNTLID + AER slot + Backend identity reference indefinitely (until TCP reset).

Visible hint: m01 Matrix D kernel dmesg warning `nvme nvme0: long keepalive RTT (2522123190 ms)` — the host-side driver observed impossibly large KeepAlive RTT because the target never timed it out, letting kernel accumulate stale state before its own recovery logic kicked in.

## 2. Contract reference

Per `v2-v3-contract-bridge-catalogue.md` §2.2.14 (NVMe Session state):

> **C1-NVME-SESSION-KATO-STORED-NOT-ENFORCED** — "Session MUST arm a watchdog for `KATO` ms after admin Connect; if no Keep-Alive admin command arrives before the deadline, session tears itself down (close sockets, release CNTLID/AER/BackendRef)."

Current code at `session.go` parses CDW12 into `s.katoMS` but never arms a timer. Contract status today: **VIOLATED (silently)**.

NVMe-oF spec §3.3.2 requires: `If no Keep Alive command is processed within the KATO interval, the controller shall treat the association as failed and terminate all outstanding commands and release controller resources.`

## 3. Why this wasn't caught

| Test layer | Coverage |
|---|---|
| L0 unit | session close-path goroutine-leak test exists; doesn't simulate host going silent |
| L1 QA addendum (T3-DEF-6 `CleanupOnClose_NoLeaksAcrossCycles`) | Clean close path; host always sends close |
| T3c scenarios | Short-duration; KATO default (120s) never hits |
| m01 Matrix A-F | Fast operations; dead-host scenario not exercised |

No existing test drives "host silent for > KATO" against a live target.

## 4. Impact

- **Functional (no correctness impact for happy path)**: sessions that close cleanly are fine.
- **Resource leak under partition**: network partition leaves CNTLID + AER slot + pendingCapsules entries uncleared until TCP keepalive / RST arrives (minutes–hours at OS default).
- **Liveness contract violation**: multi-path clients relying on ANA updates expect target to notice dead path within KATO; currently target is silent, client-driven timeout kicks in instead.
- **Diagnostic pollution**: "long keepalive RTT" dmesg warnings confuse operators (observed on m01 already).

## 5. Fix sketch

~50–100 LOC + one goroutine per session:

```go
// session.go — after admin Connect parses katoMS:
if s.katoMS > 0 {
    s.kaDeadline.Store(time.Now().Add(time.Duration(s.katoMS) * time.Millisecond).UnixNano())
    go s.kaWatchdog()
}

// On every admin/io command completion:
s.kaDeadline.Store(time.Now().Add(time.Duration(s.katoMS) * time.Millisecond).UnixNano())

// kaWatchdog:
func (s *Session) kaWatchdog() {
    for {
        dl := time.Unix(0, s.kaDeadline.Load())
        now := time.Now()
        if now.After(dl) {
            s.log("KATO expired; terminating association")
            _ = s.Close()
            return
        }
        select {
        case <-s.closeCh:
            return
        case <-time.After(dl.Sub(now)):
        }
    }
}
```

Edge cases to cover in test:
- `katoMS == 0` → watchdog MUST NOT arm (spec: 0 = disabled).
- Normal Keep-Alive flow resets deadline; no false-positive termination.
- Close during watchdog tick → no race / double-Close.
- `kaDeadline` updates must use atomic (session is multi-goroutine).

## 6. Test plan (to land with fix)

- L0 unit: `TestSession_KATO_FiresOnSilence` — set KATO=500ms, stop sending commands, expect session torn down within 500-750ms.
- L0 unit: `TestSession_KATO_ResetOnKeepAlive` — send Keep-Alive every 300ms for 3s with KATO=500ms, expect session alive.
- L0 unit: `TestSession_KATO_Zero_NoWatchdog` — KATO=0, sleep 2s with no traffic, expect session alive.
- L1 QA: extend `t3_qa_session_cleanup_addendum_test.go` with KATO-based teardown cycle + leak check.

## 7. Catalogue linkage

Existing catalogue row `C1-NVME-SESSION-KATO-STORED-NOT-ENFORCED` in `v2-v3-contract-bridge-catalogue.md` §2.2.14 is the anchor (already reclassified to **VIOLATED** with BUG-006 citation 2026-04-22). Upon fix, flip the verdict to SATISFIED (not add a new row) — timer enforcement is the "partial" half of the existing PRESERVE-partial tag being completed.

Port-verdict semantics: the row is V3 REBUILD vs V2's full-timer path (V2 iSCSI had M/E phase + TCP-layer keepalive; V3 NVMe needs native KATO watchdog at session layer).

## 8. Exit criteria

- Watchdog goroutine lands; KATO armed on admin Connect.
- 3 L0 tests + 1 L1 QA extension green.
- Catalogue row C1-NVME-SESSION-KATO-STORED-NOT-ENFORCED flips verdict from VIOLATED to SATISFIED.
- m01 repro: deliberately hang host after connect, observe target session torn down within KATO window (no more "long keepalive RTT" dmesg pattern).
- BUG moves to CLOSED.

## 9. Log

| Date | Actor | Event |
|---|---|---|
| 2026-04-22 | QA | Filed as T3-DEF-7 during T3-end retrospective; m01 Matrix D dmesg warning `long keepalive RTT (2522123190 ms)` cited as visible symptom; target track G21 or post-T3 window |
