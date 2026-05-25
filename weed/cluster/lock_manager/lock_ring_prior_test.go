package lock_manager

import (
	"fmt"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

func TestLockRing_PriorOwner(t *testing.T) {
	r := NewLockRing(5 * time.Second)
	t.Cleanup(r.WaitForCleanup)

	setA := []pb.ServerAddress{"s1:1", "s2:1", "s3:1"}
	r.SetSnapshot(setA, 1)

	// Only one snapshot: nothing to fall back to.
	if got := r.PriorOwner("any"); got != "" {
		t.Fatalf("single snapshot should have no prior owner, got %q", got)
	}

	// Add a server so some keys' ownership moves.
	setB := []pb.ServerAddress{"s1:1", "s2:1", "s3:1", "s4:1"}
	r.SetSnapshot(setB, 2)

	var moved, stable string
	for i := 0; i < 2000 && (moved == "" || stable == ""); i++ {
		key := fmt.Sprintf("key-%d", i)
		if r.GetPrimary(key) != hashKeyToServer(key, setA) {
			if moved == "" {
				moved = key
			}
		} else if stable == "" {
			stable = key
		}
	}
	if moved == "" || stable == "" {
		t.Skip("could not find both a moved and a stable key")
	}

	if got, want := r.PriorOwner(moved), hashKeyToServer(moved, setA); got != want {
		t.Fatalf("PriorOwner(moved)=%q, want %q", got, want)
	}
	if got := r.PriorOwner(stable); got != "" {
		t.Fatalf("unmoved key should have no prior owner, got %q", got)
	}
}

func TestLockRing_PriorOwnerExpires(t *testing.T) {
	r := NewLockRing(20 * time.Millisecond)
	t.Cleanup(r.WaitForCleanup)
	r.SetSnapshot([]pb.ServerAddress{"s1:1", "s2:1", "s3:1"}, 1)
	r.SetSnapshot([]pb.ServerAddress{"s1:1", "s2:1", "s3:1", "s4:1"}, 2)

	// Past the cooling interval, the prior owner is no longer offered.
	time.Sleep(40 * time.Millisecond)
	for i := 0; i < 2000; i++ {
		if got := r.PriorOwner(fmt.Sprintf("key-%d", i)); got != "" {
			t.Fatalf("prior owner should expire after the cooling interval, got %q", got)
		}
	}
}
