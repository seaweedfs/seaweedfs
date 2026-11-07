package shell

import (
	"errors"
	"testing"
	"time"
)

func TestSaveMountSyncOffset(t *testing.T) {
	const crtime = int64(1700000000)
	noOffset := func() (int64, error) { return 0, nil }
	created := func() (int64, error) { return crtime, nil }
	failed := errors.New("kv down")

	var written []int64
	write := func(ts int64) error { written = append(written, ts); return nil }

	// a new mount starts where a write-back without an offset would: at the
	// directory's creation, so local files already there are written back
	if err := saveMountSyncOffsetWith(noOffset, created, write, "/buckets/b"); err != nil {
		t.Fatal(err)
	}
	if want := time.Unix(crtime, 0).UnixNano(); len(written) != 1 || written[0] != want {
		t.Fatalf("a new mount saved %v, want [%d]", written, want)
	}

	written = nil
	if err := saveMountSyncOffsetWith(func() (int64, error) { return 42, nil }, created, write, "/buckets/b"); err != nil {
		t.Fatal(err)
	}
	if len(written) != 0 {
		t.Fatalf("a remount overwrote the offset remote.unmount saved: %v", written)
	}

	if err := saveMountSyncOffsetWith(noOffset, func() (int64, error) { return 0, nil }, write, "/buckets/b"); err != nil || len(written) != 0 {
		t.Fatalf("no creation time must save nothing: err=%v written=%v", err, written)
	}

	if err := saveMountSyncOffsetWith(func() (int64, error) { return 0, failed }, created, write, "/buckets/b"); err == nil || len(written) != 0 {
		t.Fatalf("an unread offset must fail without a write: err=%v written=%v", err, written)
	}
	if err := saveMountSyncOffsetWith(noOffset, func() (int64, error) { return 0, failed }, write, "/buckets/b"); err == nil || len(written) != 0 {
		t.Fatalf("a failed lookup must fail without a write: err=%v written=%v", err, written)
	}
	if err := saveMountSyncOffsetWith(noOffset, created, func(int64) error { return failed }, "/buckets/b"); err == nil {
		t.Fatal("a failed write must be reported")
	}
}
