package shell

import (
	"errors"
	"testing"
	"time"
)

func TestSaveMountSyncOffset(t *testing.T) {
	mountedAt := time.Unix(1700000000, 123)

	var written []int64
	write := func(ts int64) error { written = append(written, ts); return nil }

	if err := saveMountSyncOffsetWith(func() (int64, error) { return 0, nil }, write, "/buckets/b", mountedAt); err != nil {
		t.Fatal(err)
	}
	if len(written) != 1 || written[0] != mountedAt.UnixNano() {
		t.Fatalf("a new mount saved %v, want [%d]", written, mountedAt.UnixNano())
	}

	written = nil
	if err := saveMountSyncOffsetWith(func() (int64, error) { return 42, nil }, write, "/buckets/b", mountedAt); err != nil {
		t.Fatal(err)
	}
	if len(written) != 0 {
		t.Fatalf("a remount overwrote the offset remote.unmount saved: %v", written)
	}

	if err := saveMountSyncOffsetWith(func() (int64, error) { return 0, errors.New("kv down") }, write, "/buckets/b", mountedAt); err == nil || len(written) != 0 {
		t.Fatalf("an unread offset must fail without a write: err=%v written=%v", err, written)
	}

	if err := saveMountSyncOffsetWith(func() (int64, error) { return 0, nil }, func(int64) error { return errors.New("kv down") }, "/buckets/b", mountedAt); err == nil {
		t.Fatal("a failed write must be reported")
	}
}
