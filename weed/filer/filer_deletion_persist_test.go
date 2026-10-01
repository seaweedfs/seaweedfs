package filer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// readPersistedLedger pulls the persisted ledger payload straight from the store
// the filer is wired to, the same way a restarting filer would.
func readPersistedLedger(t *testing.T, f *Filer) ([]string, bool) {
	t.Helper()
	raw, err := f.Store.KvGet(context.Background(), []byte(f.deletionLedgerKey()))
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
// The overrides are restored on cleanup so later tests see production defaults.
func withPersistedConfig(t *testing.T, enabled bool) {
	t.Helper()
	v := util.GetViper()
	v.Set("filer.deleteQueue.persist", enabled)
	v.Set("filer.deleteQueue.recoveryGrace", defaultDeletionRecoveryGrace)
	t.Cleanup(func() {
		v.Set("filer.deleteQueue.persist", true)
		v.Set("filer.deleteQueue.recoveryGrace", defaultDeletionRecoveryGrace)
	})
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

// Filers sharing one metadata store must not overwrite each other's ledgers:
// each filer keys its ledger by its own address.
func TestDeletionLedgerScopedPerFiler(t *testing.T) {
	withPersistedConfig(t, true)
	store := newStubFilerStore()
	fA := newLedgerTestFiler(store)
	fA.Dlm = lock_manager.NewDistributedLockManager("filer-a:8888")
	fB := newLedgerTestFiler(store)
	fB.Dlm = lock_manager.NewDistributedLockManager("filer-b:8888")

	fA.queueDeletions("1,01")
	fB.queueDeletions("2,02")
	fA.snapshotDeletionLedger()
	fB.snapshotDeletionLedger()

	idsA, okA := readPersistedLedger(t, fA)
	idsB, okB := readPersistedLedger(t, fB)
	if !okA || !okB {
		t.Fatalf("both filers must have their own ledger, got %v %v", idsA, idsB)
	}
	if !contains(idsA, "1,01") || contains(idsA, "2,02") {
		t.Fatalf("filer A ledger wrong: %v", idsA)
	}
	if !contains(idsB, "2,02") || contains(idsB, "1,01") {
		t.Fatalf("filer B ledger wrong: %v", idsB)
	}

	// A restarted filer on B's address recovers only B's pending deletions.
	fB2 := newLedgerTestFiler(store)
	fB2.Dlm = lock_manager.NewDistributedLockManager("filer-b:8888")
	if !fB2.reloadDeletionLedger() {
		t.Fatalf("reload should succeed")
	}
	if fB2.pendingDeletionCount() != 1 {
		t.Fatalf("B's restart should recover exactly its own id, got %d", fB2.pendingDeletionCount())
	}
}

// A ledger bigger than one store value must still persist: it splits into part
// keys under a manifest, and shrinking below the part size removes the parts.
func TestDeletionLedgerChunked(t *testing.T) {
	withPersistedConfig(t, true)
	store := newStubFilerStore()
	f := newLedgerTestFiler(store)

	var ids []string
	for i := 0; i < 4000; i++ {
		ids = append(ids, fmt.Sprintf("7,%08xbeefcafe", i))
	}
	f.queueDeletions(ids...)
	f.snapshotDeletionLedger()

	raw, err := f.Store.KvGet(context.Background(), []byte(f.deletionLedgerKey()))
	if err != nil || len(raw) == 0 || raw[0] != '{' {
		t.Fatalf("expected a chunked manifest, got %v %q", err, raw)
	}

	f2 := newLedgerTestFiler(store)
	if !f2.reloadDeletionLedger() {
		t.Fatalf("chunked ledger should reload")
	}
	if got := f2.pendingDeletionCount(); got != len(ids) {
		t.Fatalf("recovered %d ids, want %d", got, len(ids))
	}

	// Forget everything; the next snapshot is a single value again and the
	// part keys are removed.
	for _, id := range ids {
		f2.forgetDeletion(id)
	}
	f2.snapshotDeletionLedger()
	raw, err = f.Store.KvGet(context.Background(), []byte(f2.deletionLedgerKey()))
	if err != nil || len(raw) == 0 || raw[0] != '[' {
		t.Fatalf("expected single-value ledger after shrink, got %v %q", err, raw)
	}
	if _, err := f.Store.KvGet(context.Background(), deletionLedgerPartKey(f2.deletionLedgerKey(), 0)); err != ErrKvNotFound {
		t.Fatalf("stale part key must be removed, got %v", err)
	}
}

// A startup ledger read that fails for a reason other than not-found must not
// let snapshots overwrite the unread ledger with a partial set.
func TestDeletionLedgerReadFailureBlocksPersistence(t *testing.T) {
	withPersistedConfig(t, true)
	store := newStubFilerStore()
	f := newLedgerTestFiler(store)
	store.kvGetErr = errors.New("kv read down")

	if f.reloadDeletionLedger() {
		t.Fatalf("reload should report the ledger as unusable")
	}
	store.kvGetErr = nil

	f.queueDeletions("1,01")
	f.snapshotDeletionLedger()
	if _, ok := readPersistedLedger(t, f); ok {
		t.Fatalf("snapshot must not overwrite a ledger that was never read")
	}
}

// Recovered ids join the pending set immediately — before the grace delay — so
// an early snapshot rewrites the recovered ids instead of dropping them.
func TestDeletionLedgerRecoveryKeepsIdsPending(t *testing.T) {
	withPersistedConfig(t, true)
	store := newStubFilerStore()
	f := newLedgerTestFiler(store)
	f.queueDeletions("1,01", "1,02")
	f.snapshotDeletionLedger()

	util.GetViper().Set("filer.deleteQueue.recoveryGrace", time.Hour)
	f2 := newLedgerTestFiler(store)
	if !f2.reloadDeletionLedger() {
		t.Fatalf("reload should succeed")
	}
	// Still inside the grace window, but the ids are already pending.
	if f2.pendingDeletionCount() != 2 {
		t.Fatalf("recovered ids must be pending immediately, got %d", f2.pendingDeletionCount())
	}
	// And an early shutdown snapshot keeps them.
	f2.snapshotDeletionLedger()
	persisted, ok := readPersistedLedger(t, f2)
	if !ok || len(persisted) != 2 {
		t.Fatalf("snapshot during grace must carry recovered ids, got %v", persisted)
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
