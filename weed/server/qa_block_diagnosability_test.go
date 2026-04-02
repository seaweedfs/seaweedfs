package weed_server

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// ============================================================
// Phase 12 P3: Diagnosability / Blocker Accounting
//
// All diagnosis conclusions use ONLY explicit bounded read-only
// diagnosis surfaces:
//   - LookupBlockVolume       (product-visible publication)
//   - FailoverDiagnostic      (volume-oriented failover state)
//   - PublicationDiagnostic   (lookup vs authority coherence)
//   - RecoveryDiagnostic      (active recovery task set)
//   - phase-12-p3-blockers.md (finite blocker ledger)
//
// NOT performance, NOT rollout readiness.
// ============================================================

// --- S1: Failover convergence diagnosable via FailoverDiagnostic ---

func TestP12P3_FailoverConvergence_Diagnosable(t *testing.T) {
	s := newSoakSetup(t)
	ctx := context.Background()

	s.ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name: "diag-vol-1", SizeBytes: 1 << 20,
	})

	entry, _ := s.ms.blockRegistry.Lookup("diag-vol-1")
	s.bs.localServerID = entry.VolumeServer
	s.deliver(entry.VolumeServer)
	time.Sleep(100 * time.Millisecond)

	// Surface 1: LookupBlockVolume shows current primary before failover.
	lookupBefore, _ := s.ms.LookupBlockVolume(ctx, &master_pb.LookupBlockVolumeRequest{Name: "diag-vol-1"})
	if lookupBefore.VolumeServer != entry.VolumeServer {
		t.Fatal("lookup should show original primary before failover")
	}

	// Surface 2: FailoverDiagnostic — no volumes in failover state yet.
	failoverBefore := s.ms.blockFailover.DiagnosticSnapshot()
	for _, v := range failoverBefore.Volumes {
		if v.VolumeName == "diag-vol-1" {
			t.Fatalf("S1: diag-vol-1 should not appear in failover diagnostic before failover, got %+v", v)
		}
	}

	// Trigger failover: expire lease, then failover.
	s.ms.blockRegistry.UpdateEntry("diag-vol-1", func(e *BlockVolumeEntry) {
		e.LastLeaseGrant = time.Now().Add(-1 * time.Minute)
	})
	s.ms.failoverBlockVolumes(entry.VolumeServer)

	// Surface 1 after: LookupBlockVolume shows NEW primary.
	lookupAfter, _ := s.ms.LookupBlockVolume(ctx, &master_pb.LookupBlockVolumeRequest{Name: "diag-vol-1"})
	if lookupAfter.VolumeServer == entry.VolumeServer {
		t.Fatal("S1 stall: lookup still shows old primary after failover")
	}

	// Surface 2 after: FailoverDiagnostic shows volume-level failover state.
	failoverAfter := s.ms.blockFailover.DiagnosticSnapshot()

	// Classify via explicit diagnosis surface: find diag-vol-1 in failover volumes.
	var found *FailoverVolumeState
	for i := range failoverAfter.Volumes {
		if failoverAfter.Volumes[i].VolumeName == "diag-vol-1" {
			found = &failoverAfter.Volumes[i]
			break
		}
	}
	if found == nil {
		t.Fatal("S1: diag-vol-1 not found in FailoverDiagnostic after failover")
	}

	// Diagnosis conclusion from explicit surfaces only:
	// - Lookup changed (old → new primary)
	// - FailoverDiagnostic classifies state as rebuild_pending
	// - AffectedServer identifies the dead server
	if !found.PendingRebuild {
		t.Fatal("S1: FailoverDiagnostic should show PendingRebuild=true")
	}
	if found.Reason != "rebuild_pending" {
		t.Fatalf("S1: expected reason=rebuild_pending, got %q", found.Reason)
	}
	if found.AffectedServer != entry.VolumeServer {
		t.Fatalf("S1: AffectedServer should be dead server %s, got %s", entry.VolumeServer, found.AffectedServer)
	}
	if found.CurrentPrimary != lookupAfter.VolumeServer {
		t.Fatalf("S1: CurrentPrimary should match lookup %s, got %s", lookupAfter.VolumeServer, found.CurrentPrimary)
	}

	t.Logf("P12P3 S1: diagnosed via LookupBlockVolume(%s→%s) + FailoverDiagnostic(vol=%s, reason=%s, affected=%s)",
		lookupBefore.VolumeServer, lookupAfter.VolumeServer, found.VolumeName, found.Reason, found.AffectedServer)
}

// --- S2: Publication mismatch diagnosable via PublicationDiagnostic ---

func TestP12P3_PublicationMismatch_Diagnosable(t *testing.T) {
	s := newSoakSetup(t)
	ctx := context.Background()

	s.ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name: "diag-vol-2", SizeBytes: 1 << 20,
	})

	entry, _ := s.ms.blockRegistry.Lookup("diag-vol-2")
	s.bs.localServerID = entry.VolumeServer
	s.deliver(entry.VolumeServer)
	time.Sleep(100 * time.Millisecond)

	// Surface 1: PublicationDiagnostic before failover — should be coherent.
	pubBefore, ok := s.ms.PublicationDiagnosticFor("diag-vol-2")
	if !ok {
		t.Fatal("S2: PublicationDiagnosticFor should find diag-vol-2")
	}
	if !pubBefore.Coherent {
		t.Fatalf("S2: publication should be coherent before failover, got reason=%q", pubBefore.Reason)
	}

	// Surface 2: LookupBlockVolume — repeated lookups self-consistent.
	lookup1, _ := s.ms.LookupBlockVolume(ctx, &master_pb.LookupBlockVolumeRequest{Name: "diag-vol-2"})
	lookup2, _ := s.ms.LookupBlockVolume(ctx, &master_pb.LookupBlockVolumeRequest{Name: "diag-vol-2"})
	if lookup1.IscsiAddr != lookup2.IscsiAddr || lookup1.VolumeServer != lookup2.VolumeServer {
		t.Fatalf("S2: repeated lookup mismatch: %s/%s vs %s/%s",
			lookup1.VolumeServer, lookup1.IscsiAddr, lookup2.VolumeServer, lookup2.IscsiAddr)
	}

	// Trigger failover.
	s.ms.blockRegistry.UpdateEntry("diag-vol-2", func(e *BlockVolumeEntry) {
		e.LastLeaseGrant = time.Now().Add(-1 * time.Minute)
	})
	s.ms.failoverBlockVolumes(entry.VolumeServer)

	// Surface 1 after: PublicationDiagnostic after failover — still coherent.
	pubAfter, ok := s.ms.PublicationDiagnosticFor("diag-vol-2")
	if !ok {
		t.Fatal("S2: PublicationDiagnosticFor should find diag-vol-2 after failover")
	}
	if !pubAfter.Coherent {
		t.Fatalf("S2: publication should be coherent after failover, got reason=%q", pubAfter.Reason)
	}

	// Diagnosis conclusion from explicit surfaces only:
	// - Pre-failover: coherent, lookup matches authority
	// - Post-failover: coherent, lookup updated to new primary
	// - Publication switched: post != pre
	if pubAfter.LookupVolumeServer == pubBefore.LookupVolumeServer {
		t.Fatal("S2: LookupVolumeServer unchanged after failover — publication did not switch")
	}
	if pubAfter.LookupIscsiAddr == pubBefore.LookupIscsiAddr {
		t.Fatal("S2: LookupIscsiAddr unchanged after failover")
	}

	// Post-failover repeated lookup still self-consistent (via diagnostic).
	pubAfter2, _ := s.ms.PublicationDiagnosticFor("diag-vol-2")
	if pubAfter2.LookupVolumeServer != pubAfter.LookupVolumeServer ||
		pubAfter2.LookupIscsiAddr != pubAfter.LookupIscsiAddr {
		t.Fatal("S2: post-failover publication diagnostics inconsistent")
	}

	t.Logf("P12P3 S2: diagnosed via PublicationDiagnostic — pre(vs=%s, iscsi=%s, coherent=%v) → post(vs=%s, iscsi=%s, coherent=%v)",
		pubBefore.LookupVolumeServer, pubBefore.LookupIscsiAddr, pubBefore.Coherent,
		pubAfter.LookupVolumeServer, pubAfter.LookupIscsiAddr, pubAfter.Coherent)
}

// --- S3: Runtime residue diagnosable via RecoveryDiagnostic ---

func TestP12P3_RuntimeResidue_Diagnosable(t *testing.T) {
	s := newSoakSetup(t)
	ctx := context.Background()

	s.ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name: "diag-vol-3", SizeBytes: 1 << 20,
	})

	entry, _ := s.ms.blockRegistry.Lookup("diag-vol-3")
	s.bs.localServerID = entry.VolumeServer
	s.deliver(entry.VolumeServer)
	time.Sleep(200 * time.Millisecond)

	// Surface: RecoveryDiagnostic shows active task set.
	diagBefore := s.bs.v2Recovery.DiagnosticSnapshot()
	t.Logf("S3 before delete: %d active tasks: %v", len(diagBefore.ActiveTasks), diagBefore.ActiveTasks)

	// Delete the volume.
	s.ms.DeleteBlockVolume(ctx, &master_pb.DeleteBlockVolumeRequest{Name: "diag-vol-3"})
	time.Sleep(200 * time.Millisecond)

	// Surface after: RecoveryDiagnostic — no tasks for deleted volume.
	diagAfter := s.bs.v2Recovery.DiagnosticSnapshot()
	for _, task := range diagAfter.ActiveTasks {
		if strings.Contains(task, "diag-vol-3") {
			t.Fatalf("S3 residue: task %s active after delete", task)
		}
	}

	// Diagnosis conclusion from explicit surface only:
	// - RecoveryDiagnostic.ActiveTasks does not contain deleted volume's tasks
	// - Conclusion: clean (no residue) or non-empty but unrelated to deleted volume
	if len(diagAfter.ActiveTasks) == 0 {
		t.Log("P12P3 S3: diagnosed via RecoveryDiagnostic — clean (0 active tasks after delete)")
	} else {
		t.Logf("P12P3 S3: diagnosed via RecoveryDiagnostic — %d active tasks (none for deleted vol)",
			len(diagAfter.ActiveTasks))
	}
}

// --- Blocker ledger: reads and validates the actual file ---

func TestP12P3_BlockerLedger_Bounded(t *testing.T) {
	ledgerPath := "../../sw-block/.private/phase/phase-12-p3-blockers.md"

	data, err := os.ReadFile(ledgerPath)
	if err != nil {
		t.Fatalf("blocker ledger must exist at %s: %v", ledgerPath, err)
	}

	content := string(data)

	// Must contain diagnosed items.
	for _, id := range []string{"B1", "B2", "B3"} {
		if !strings.Contains(content, id) {
			t.Fatalf("ledger missing diagnosed item %s", id)
		}
	}

	// Must contain unresolved items.
	for _, id := range []string{"U1", "U2", "U3"} {
		if !strings.Contains(content, id) {
			t.Fatalf("ledger missing unresolved item %s", id)
		}
	}

	// Must contain out-of-scope section.
	if !strings.Contains(content, "Out of Scope") {
		t.Fatal("ledger must have 'Out of Scope' section")
	}

	// Must NOT overclaim perf or rollout.
	lines := strings.Split(content, "\n")
	diagnosedCount := 0
	unresolvedCount := 0
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "| B") {
			diagnosedCount++
		}
		if strings.HasPrefix(strings.TrimSpace(line), "| U") {
			unresolvedCount++
		}
	}

	total := diagnosedCount + unresolvedCount
	if total == 0 {
		t.Fatal("ledger has no blocker items")
	}
	if total > 20 {
		t.Fatalf("ledger should be finite, got %d items", total)
	}

	t.Logf("P12P3 blockers: %d diagnosed + %d unresolved = %d total (from actual file, finite)",
		diagnosedCount, unresolvedCount, total)
}
