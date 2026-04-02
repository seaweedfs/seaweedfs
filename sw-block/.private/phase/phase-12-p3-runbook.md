# Phase 12 P3 — Bounded Runbook

Scope: diagnosis of three symptom classes on the accepted RF=2 sync_all chosen path.

All diagnosis steps reference ONLY explicit bounded read-only surfaces:
- `LookupBlockVolume` — gRPC RPC returning current primary VS + iSCSI address
- `FailoverDiagnostic` — volume-oriented failover state snapshot
- `PublicationDiagnostic` — lookup vs authority coherence snapshot
- `RecoveryDiagnostic` — active recovery task set snapshot
- Blocker ledger — finite file at `phase-12-p3-blockers.md`

## S1: Failover/Recovery Convergence Stall

**Visible symptom:** Volume remains unavailable after a VS death; lookup still returns the old primary.

**Diagnosis surfaces:**
- `LookupBlockVolume(volumeName)` — check if `VolumeServer` is still the dead server
- `FailoverDiagnostic` — check `Volumes[]` for the affected volume

**Diagnosis steps:**
1. Call `LookupBlockVolume(volumeName)`. If `VolumeServer` changed from the dead server, failover succeeded.
2. If unchanged: read `FailoverDiagnostic`. Find the volume by name in `Volumes[]`.
3. If found with `DeferredPromotion=true`: lease-wait — failover is deferred until lease expires.
4. If found with `PendingRebuild=true`: failover completed, rebuild is pending for the dead server.
5. If `DeferredPromotionCount[deadServer] > 0` in the aggregate: deferred promotions are queued.
6. If the volume does not appear in either lookup change or `FailoverDiagnostic`: escalate.

**Conclusion classes (from surfaces only):**
- **Lease-wait:** `FailoverDiagnostic.DeferredPromotionCount[deadServer] > 0` — normal, bounded by lease TTL.
- **Rebuild-pending:** `FailoverDiagnostic.Volumes[].PendingRebuild=true` — failover done, rebuild queued.
- **Converged:** `LookupBlockVolume` shows new primary, no failover entries — resolved.
- **Unresolved:** None of the above — escalate.

## S2: Publication/Lookup Mismatch

**Visible symptom:** `LookupBlockVolume` returns an iSCSI address or volume server that doesn't match expected state.

**Diagnosis surfaces:**
- `LookupBlockVolume(volumeName)` — operator-visible publication
- `PublicationDiagnostic` — explicit coherence check (lookup vs authority)

**Diagnosis steps:**
1. Call `PublicationDiagnosticFor(volumeName)`. Check `Coherent` field.
2. If `Coherent=true`: lookup matches registry authority — no mismatch.
3. If `Coherent=false`: read `Reason` for explanation. Compare `LookupVolumeServer` vs `AuthorityVolumeServer` and `LookupIscsiAddr` vs `AuthorityIscsiAddr`.
4. Cross-check with `LookupBlockVolume` directly: repeated lookups should be self-consistent.

**Conclusion classes (from surfaces only):**
- **Coherent:** `PublicationDiagnostic.Coherent=true` — no mismatch.
- **Stale client:** Coherent but client sees old value — bounded by client re-query.
- **Unresolved:** `PublicationDiagnostic.Coherent=false` with no transient cause — escalate.

## S3: Leftover Runtime Work After Convergence

**Visible symptom:** After volume deletion or steady-state convergence, recovery tasks should have drained.

**Diagnosis surfaces:**
- `RecoveryDiagnostic` — `ActiveTasks` list (replicaIDs with active recovery work)

**Diagnosis steps:**
1. Call `RecoveryManager.DiagnosticSnapshot()`. Read `ActiveTasks`.
2. If `ActiveTasks` is empty: clean — no leftover work.
3. If non-empty: check whether any task replicaID contains the deleted volume's path.
4. If a deleted volume's replicaID is present in `ActiveTasks`: residue — escalate.
5. If all tasks are for live volumes: non-empty but expected — normal in-flight work.

**Conclusion classes (from surfaces only):**
- **Clean:** `RecoveryDiagnostic.ActiveTasks` is empty — runtime converged.
- **Non-empty, no residue:** Tasks present but none for the deleted/converged volume — normal.
- **Residue:** Deleted volume's replicaID still in `ActiveTasks` — escalate.
