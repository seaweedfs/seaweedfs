# Phase 5 Dev Log

Append-only communication between agents. Newest entries at bottom.
Each entry: `[date] [role] message`

Roles: `DEV`, `REVIEWER`, `TESTER`, `ARCHITECT`

---

[2026-03-03] [DEV] CP5-1 ALUA + multipath complete. Added ALUA provider + REPORT TPG (implicit ALUA), VPD 0x83
NAA+TPG+RTP descriptors, TPGS=01 in INQUIRY, standby write fencing, and -tpg-id flag. Added UUID to VolumeInfo for
shared NAA. Added multipath config and setup script. 4 multipath integration tests added. 10 ALUA unit tests added
(SCSI tests total 53). Reviewer fixes applied: RoleNone maps to Active/Optimized to avoid single-node regression;
REPORT TPG advertises T_SUP when state is Transitioning; TPG ID validation; non-ASCII log fix. Added 2 tests:
alua_role_none_allows_writes and alua_report_tpg_transitioning. All unit tests pass, Linux cross-compile verified.

[2026-03-03] [TESTER] CP5-1 adversarial suite: 16 tests added/validated (state boundaries, VPD 0x83, REPORT TPG,
concurrency, INQUIRY invariants). All 16 PASS. No regressions in engine + iSCSI tests.

[2026-03-03] [DEV] CP5-2 CoW snapshots completed. Fixes applied from review: DeleteSnapshot pauses flusher before
closing delta; RestoreSnapshot checks PauseAndFlush error + defers Resume; CreateSnapshot holds snapMu across check/insert;
Delete/Restore use beginOp/endOp; lock order documented (flushMu -> snapMu); non-ASCII punctuation removed; persistSuperblock
now returns error and callers propagate. All tests passing (known pre-existing flaky
rebuild_full_extent_midcopy_writes under full-suite load).

[2026-03-03] [TESTER] CP5-2 QA adversarial suite: 22 tests in 5 groups (races, role rejection, edge cases, lifecycle,
restore correctness) all PASS. Confirms fixes for delete_during_flush_cow, concurrent_create_same_id, and restore path
nextLSN reset.

[2026-03-03] [DEV] CP5-3 implementation complete. CHAP: ValidateCHAPConfig with ErrCHAPSecretEmpty and CLI guard
requires -chap-secret when -chap-user is set. Login SecurityNeg echoes AuthMethod=CHAP on second PDU after verify; test
assertion added. Metrics adapter docs clarify counters count attempts; /metrics inherits admin auth noted in header
comment. All CP5-3 tests pass; only pre-existing flaky rebuild_catchup_concurrent_writes observed under full suite.

[2026-03-03] [TESTER] CP5-3 QA adversarial: 28 tests added (16 CHAP + 12 resize) all PASS. No new bugs. Full
regression clean except pre-existing flaky rebuild_catchup_concurrent_writes.
