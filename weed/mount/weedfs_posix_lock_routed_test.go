package mount

import (
	"syscall"
	"testing"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer/posixlock"
)

func TestPosixLockTypeMapping(t *testing.T) {
	cases := []struct {
		sys  uint32
		wire uint32
	}{
		{syscall.F_RDLCK, posixlock.Read},
		{syscall.F_WRLCK, posixlock.Write},
		{syscall.F_UNLCK, posixlock.Unlock},
	}
	for _, c := range cases {
		if got := posixLockTypeToWire(c.sys); got != c.wire {
			t.Errorf("toWire(%d) = %d, want %d", c.sys, got, c.wire)
		}
		if got := posixLockTypeFromWire(c.wire); got != c.sys {
			t.Errorf("fromWire(%d) = %d, want %d", c.wire, got, c.sys)
		}
	}
}

func TestPosixPollAcquireGrantedImmediately(t *testing.T) {
	calls := 0
	st := posixPollAcquire(nil, func() (bool, error) { calls++; return true, nil })
	if st != fuse.OK || calls != 1 {
		t.Fatalf("immediate grant: status=%v calls=%d", st, calls)
	}
}

func TestPosixPollAcquireRetriesThenGrants(t *testing.T) {
	calls := 0
	st := posixPollAcquire(nil, func() (bool, error) {
		calls++
		return calls >= 3, nil
	})
	if st != fuse.OK || calls != 3 {
		t.Fatalf("retry then grant: status=%v calls=%d", st, calls)
	}
}

func TestPosixPollAcquireError(t *testing.T) {
	if st := posixPollAcquire(nil, func() (bool, error) { return false, syscall.EIO }); st != fuse.EIO {
		t.Fatalf("error should map to EIO, got %v", st)
	}
}

func TestPosixPollAcquireCancel(t *testing.T) {
	cancel := make(chan struct{})
	close(cancel)
	done := make(chan fuse.Status, 1)
	go func() {
		done <- posixPollAcquire(cancel, func() (bool, error) { return false, nil })
	}()
	select {
	case st := <-done:
		if st != fuse.EINTR {
			t.Fatalf("cancel should map to EINTR, got %v", st)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("poll did not return on cancel")
	}
}

func TestPosixLockHint(t *testing.T) {
	h := newPosixLockHint()
	if h.has(1, 2) {
		t.Fatal("empty hint should not report a lock")
	}
	h.add(1, 2)
	h.add(1, 3)
	if !h.has(1, 2) || !h.has(1, 3) {
		t.Fatal("added owners should be reported")
	}
	h.drop(1, 2)
	if h.has(1, 2) {
		t.Fatal("dropped owner should be gone")
	}
	if !h.has(1, 3) {
		t.Fatal("sibling owner should remain")
	}
	h.drop(1, 3)
	if _, ok := h.m[1]; ok {
		t.Fatal("inode entry should be removed when its last owner drops")
	}
}

func TestPosixKeepaliveTracksHeldKeys(t *testing.T) {
	k := newPosixKeepalive()
	if len(k.snapshot()) != 0 {
		t.Fatal("new tracker should be empty")
	}
	// Two holds on the same key (fcntl + flock for one owner) and one on another.
	k.add("a", 1, false)
	k.add("a", 1, true)
	k.add("b", 2, false)
	if got := len(k.snapshot()); got != 2 {
		t.Fatalf("expected 2 distinct keys, got %d", got)
	}
	// Dropping one namespace keeps the key while the other hold remains.
	k.remove("a", 1, false)
	if got := len(k.snapshot()); got != 2 {
		t.Fatalf("key a still has a flock hold; expected 2 keys, got %d", got)
	}
	k.remove("a", 1, true)
	keys := k.snapshot()
	if len(keys) != 1 || keys[0] != "b" {
		t.Fatalf("only key b should remain, got %v", keys)
	}
}
