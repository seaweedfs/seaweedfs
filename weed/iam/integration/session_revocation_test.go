package integration

import (
	"context"
	"testing"
	"time"
)

func TestMemoryRevocationStoreCRUD(t *testing.T) {
	ctx := context.Background()
	store := NewMemorySessionRevocationStore()

	if revoked, _ := store.IsRevoked(ctx, "", "abc"); revoked {
		t.Fatal("empty store should report not revoked")
	}

	if err := store.Revoke(ctx, "", &RevocationEntry{JTI: "abc", ExpiresAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if revoked, _ := store.IsRevoked(ctx, "", "abc"); !revoked {
		t.Fatal("expected revoked")
	}
	// Different JTIs are independent.
	if revoked, _ := store.IsRevoked(ctx, "", "xyz"); revoked {
		t.Fatal("xyz should not be revoked")
	}
}

func TestMemoryRevocationStoreRejectsBadInput(t *testing.T) {
	store := NewMemorySessionRevocationStore()
	if err := store.Revoke(context.Background(), "", nil); err == nil {
		t.Fatal("expected error for nil entry")
	}
	if err := store.Revoke(context.Background(), "", &RevocationEntry{}); err == nil {
		t.Fatal("expected error for empty JTI")
	}
}

func TestMemoryRevocationStorePurge(t *testing.T) {
	ctx := context.Background()
	store := NewMemorySessionRevocationStore()
	now := time.Now()

	must := func(jti string, expiresAt time.Time) {
		if err := store.Revoke(ctx, "", &RevocationEntry{JTI: jti, ExpiresAt: expiresAt}); err != nil {
			t.Fatalf("revoke %s: %v", jti, err)
		}
	}
	must("expired-1", now.Add(-2*time.Hour))
	must("expired-2", now.Add(-time.Minute))
	must("future-1", now.Add(time.Hour))

	count, err := store.Purge(ctx, "", now)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 purged, got %d", count)
	}
	if revoked, _ := store.IsRevoked(ctx, "", "expired-1"); revoked {
		t.Fatal("expired-1 should be gone")
	}
	if revoked, _ := store.IsRevoked(ctx, "", "future-1"); !revoked {
		t.Fatal("future-1 should still be revoked")
	}
}

func TestIAMManagerRevocationDefaultIsNoop(t *testing.T) {
	mgr := NewIAMManager()
	// Without SetSessionRevocationStore, revocation is a no-op.
	if got, err := mgr.IsSessionRevoked(context.Background(), "anything"); err != nil || got {
		t.Fatalf("expected (false, nil); got (%v, %v)", got, err)
	}
	if err := mgr.RevokeSession(context.Background(), "abc", time.Now().Add(time.Hour), "test"); err == nil {
		t.Fatal("expected error when no store configured")
	}
}

func TestIAMManagerRevocationFlow(t *testing.T) {
	mgr := NewIAMManager()
	mgr.SetSessionRevocationStore(NewMemorySessionRevocationStore())

	jti := "session-1"
	if err := mgr.RevokeSession(context.Background(), jti, time.Now().Add(time.Hour), "logout"); err != nil {
		t.Fatalf("RevokeSession: %v", err)
	}
	revoked, err := mgr.IsSessionRevoked(context.Background(), jti)
	if err != nil {
		t.Fatalf("IsSessionRevoked: %v", err)
	}
	if !revoked {
		t.Fatal("expected revoked")
	}
}
