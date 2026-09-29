package filer

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// readPersistedLedger pulls the persisted ledger payload straight from the store
// the filer is wired to, the same way a restarting filer would.
func readPersistedLedger(t *testing.T, f *Filer) ([]string, bool) {
	t.Helper()
	raw, err := f.Store.KvGet(context.Background(), []byte(KvKeyDeletionLedger))
	if err != nil {
		if err == ErrKvNotFound {
			return nil, false
		}
		t.Fatalf("unexpected error reading ledger: %v", err)
	}
	var ids []string
	if err := json.Unmarshal(raw, &ids); err != nil {
		t.Fatalf("persisted payload not valid JSON: %v", err)
	}
	return ids, true
}

// withPersistedConfig pins the ledger knobs at viper's override tier so the test
// sees deterministic semantics regardless of global SetDefault ordering (the
// production getters SetDefault(true), which would otherwise win the tier race).
func withPersistedConfig(t *testing.T, enabled bool) {
	t.Helper()
	util.GetViper().Set("filer.deleteQueue.persist", enabled)
}

// newLedgerTestFiler builds a Filer with the pieces the ledger touches, backed by
// a real (stub) store, bypassing NewFiler's master/aggregator wiring.
func newLedgerTestFiler(store FilerStore) *Filer {
	return &Filer{
		FileIdDeletionQueue: util.NewUnboundedQueue(),
		DeletionRetryQueue:  NewDeletionRetryQueue(),
		deletionQuit:        make(chan struct{}),
		Store:               NewFilerStoreWrapper(store),
	}
}

// TestDeletionLedgerSnapshotAndRecover is the core guarantee: enqueued-but-
// unconfirmed deletions survive a simulated process restart and land back in the
// hot queue (minus whatever was confirmed terminal in the meantime).
func TestDeletionLedgerSnapshotAndRecover(t *testing.T) {
	withPersistedConfig(t, true)
	store := newStubFilerStore()
	f := newLedgerTestFiler(store)

	f.queueDeletions("1,01", "1,02", "1,03")
	f.snapshotDeletionLedger()

	persisted, ok := readPersistedLedger(t, f)
	if !ok {
		t.Fatalf("expected persisted ledger under %q", KvKeyDeletionLedger)
	}
	if len(persisted) != 3 {
		t.Fatalf("expected 3 persisted ids, got %d: %v", len(persisted), persisted)
	}
	for _, want := range []string{"1,01", "1,02", "1,03"} {
		if !contains(persisted, want) {
			t.Errorf("expected %q in persisted ledger, got %v", want, persisted)
		}
	}

	// Confirmed-gone for one id, then snapshot again.
	f.forgetDeletion("1,02")
	f.snapshotDeletionLedger()
	persisted, _ = readPersistedLedger(t, f)
	if len(persisted) != 2 {
		t.Fatalf("expected 2 persisted ids after forget, got %d: %v", len(persisted), persisted)
	}
	if contains(persisted, "1,02") {
		t.Errorf("forgotten id still in persisted ledger: %v", persisted)
	}

	// --- simulated restart: a fresh Filer with NO ledger in memory must
	// recover the still-pending ids from the store and re-queue them. ---
	f2 := newLedgerTestFiler(store)
	// recoveryGrace 0 so the background re-enqueue is near-instant.
	util.GetViper().Set("filer.deleteQueue.recoveryGrace", time.Duration(0))
	f2.reloadDeletionLedger()

	// Wait for the async recovery goroutine to drain into the hot queue.
	seen := make(map[string]bool)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		f2.FileIdDeletionQueue.Consume(func(ids []string) {
			for _, id := range ids {
				seen[id] = true
			}
		})
		if len(seen) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !seen["1,01"] || !seen["1,03"] {
		t.Fatalf("expected recovered ids 1,01 and 1,03 back in hot queue, got %v", seen)
	}
	if seen["1,02"] {
		t.Errorf("forgotten id 1,02 must NOT come back: %v", seen)
	}
}

// TestDeletionLedgerRetryKeepsEntry pins the key semantic: a non-terminal
// (retryable) failure must keep the entry persisted, so a crash mid-retry does
// not orphan the chunk. This is the whole reason forgetDeletion is only called on
// success / not-found / permanent.
func TestDeletionLedgerRetryKeepsEntry(t *testing.T) {
	withPersistedConfig(t, true)
	store := newStubFilerStore()
	f := newLedgerTestFiler(store)

	f.queueDeletions("9,01")
	f.snapshotDeletionLedger()

	// A retryable outcome deliberately does NOT call forgetDeletion, so the id
	// must still be in the durable ledger.
	persisted, ok := readPersistedLedger(t, f)
	if !ok {
		t.Fatalf("ledger missing — retryable ids must remain persisted")
	}
	if len(persisted) != 1 || persisted[0] != "9,01" {
		t.Fatalf("retryable id must stay in ledger, got %v", persisted)
	}
}

// TestDeletionLedgerDisabled verifies the kill switch: with persist off, nothing
// reaches KV, and reload refuses to recover even if a ledger exists in the store
// (so an operator turning the feature off never resurrects a stale ledger).
func TestDeletionLedgerDisabled(t *testing.T) {
	// First, with persistence ON, put a real ledger into the store.
	withPersistedConfig(t, true)
	store := newStubFilerStore()
	fOn := newLedgerTestFiler(store)
	fOn.queueDeletions("5,01")
	fOn.snapshotDeletionLedger()
	if _, ok := readPersistedLedger(t, fOn); !ok {
		t.Fatalf("setup: expected a persisted ledger with persist enabled")
	}

	// Now flip the switch OFF and check the contract.
	withPersistedConfig(t, false)

	// 1. Snapshot does not touch KV (no new writes, no clear).
	f := newLedgerTestFiler(store)
	f.queueDeletions("6,01") // in-memory only
	f.snapshotDeletionLedger()
	persisted, ok := readPersistedLedger(t, f)
	if !ok {
		t.Fatalf("setup: ledger should still exist from the enabled phase")
	}
	// The enabled-phase ledger had "5,01"; disabled phase must not have added "6,01".
	if contains(persisted, "6,01") {
		t.Errorf("persist disabled but snapshot wrote 6,01: %v", persisted)
	}

	// 2. Reload with the switch off must NOT recover, even though a ledger exists.
	f2 := newLedgerTestFiler(store)
	f2.reloadDeletionLedger()
	if f2.pendingDeletionCount() != 0 {
		t.Fatalf("reload must be a no-op when disabled, got %d pending", f2.pendingDeletionCount())
	}
}

// TestDeletionLedgerZeroValueFiler proves nil-map safety for the struct literals
// used across the test suite (they never call NewFiler).
func TestDeletionLedgerZeroValueFiler(t *testing.T) {
	withPersistedConfig(t, true)
	f := &Filer{
		FileIdDeletionQueue: util.NewUnboundedQueue(),
		DeletionRetryQueue:  NewDeletionRetryQueue(),
		deletionQuit:        make(chan struct{}),
	}
	f.queueDeletions("0,01")
	if f.pendingDeletionCount() != 1 {
		t.Fatalf("zero-value filer should lazily init the ledger, got %d", f.pendingDeletionCount())
	}
	f.forgetDeletion("0,01")
	if f.pendingDeletionCount() != 0 {
		t.Fatalf("forget on zero-value filer failed, got %d", f.pendingDeletionCount())
	}
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
