package filer

import (
	"context"
	"testing"
)

// An entry/KV transaction can be conflict checked even when bulk folder
// deletion escapes it, as on FoundationDB. Keep these capabilities distinct.
type entryConflictTestStore struct {
	FilerStore
	enabled bool
}

func (s *entryConflictTestStore) RefusesConflictingCommits() bool { return s.enabled }

func TestEntryConflictCapabilityFollowsStoreRouting(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		store := &entryConflictTestStore{FilerStore: newStubFilerStore(), enabled: enabled}
		wrapper := NewFilerStoreWrapper(store)
		wrapper.AddPathSpecificStore("/other/", "other", &entryConflictTestStore{FilerStore: newStubFilerStore(), enabled: true})
		f := &Filer{Store: wrapper}
		if got := f.RefusesConflictingCommits("/default/file"); got != enabled {
			t.Fatalf("default store capability = %v, want %v", got, enabled)
		}
		if f.RefusesConflictingCommits("/other/file") {
			t.Fatal("a path-specific store is not covered by the default store's transaction")
		}
		ctx := context.WithValue(context.Background(), storeTransactionKey{}, true)
		if f.TransactionCovers(ctx, "/default/file") {
			t.Fatal("entry conflict detection must not imply isolation of bulk operations")
		}
	}
	if NewFilerStoreWrapper(newStubFilerStore()).refusesConflictingCommits("/file") {
		t.Fatal("a store without the optional capability must remain unwrapped")
	}
}
