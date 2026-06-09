package filersink

import (
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// entry builds a file entry with the given mtime and size for the update-action
// decision, which only looks at those two attributes.
func entry(mtime int64, size uint64) *filer_pb.Entry {
	return &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{Mtime: mtime, FileSize: size}}
}

// entryNs adds a sub-second component to check same-second ordering.
func entryNs(mtime int64, ns int32, size uint64) *filer_pb.Entry {
	return &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{Mtime: mtime, MtimeNs: ns, FileSize: size}}
}

// getEntryMtimeNs must order versions written within the same second, which
// plain second-grained mtime cannot. It also must be nil-safe.
func TestGetEntryMtimeNs(t *testing.T) {
	if got := getEntryMtimeNs(nil); got != 0 {
		t.Fatalf("nil entry: got %d, want 0", got)
	}
	if got := getEntryMtimeNs(&filer_pb.Entry{}); got != 0 {
		t.Fatalf("nil attributes: got %d, want 0", got)
	}

	mk := func(sec int64, ns int32) *filer_pb.Entry {
		return &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{Mtime: sec, MtimeNs: ns}}
	}
	if got, want := getEntryMtimeNs(mk(5, 200)), int64(5_000_000_200); got != want {
		t.Fatalf("full-ns: got %d, want %d", got, want)
	}
	// same second, later sub-second component must compare strictly greater
	if !(getEntryMtimeNs(mk(5, 200)) > getEntryMtimeNs(mk(5, 100))) {
		t.Fatalf("same-second ordering failed: %d not > %d",
			getEntryMtimeNs(mk(5, 200)), getEntryMtimeNs(mk(5, 100)))
	}
}

// onCorruptChunk must surface the error, not skip, when it can't confirm
// supersession (no source filer here) — otherwise it could drop a live file.
func TestOnCorruptChunkRefusesWhenSupersessionUnconfirmed(t *testing.T) {
	fs := &FilerSink{} // filerSource nil => hasSourceNewerVersion is always false
	sentinel := errors.New("corrupt chunk: read 0 bytes, source says 107")

	got := fs.onCorruptChunk("/buckets/x/config",
		&filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{Mtime: 5, MtimeNs: 200}},
		sentinel)

	if !errors.Is(got, sentinel) {
		t.Fatalf("expected the error to be returned (refuse), got %v", got)
	}
}

// chooseUpdateAction decides skip vs repair vs normal. A destination left
// truncated by an earlier failed replication can carry a newer mtime (the
// source preserved an old mtime while the partial copy was written recently),
// so a pure mtime guard would strand the corruption. A shorter destination with
// a newer mtime must be repaired, not skipped.
func TestChooseUpdateAction(t *testing.T) {
	tests := []struct {
		name     string
		existing *filer_pb.Entry
		incoming *filer_pb.Entry
		want     updateAction
	}{
		{"nil existing entry", nil, entry(200, 100), updateNormal},
		{"destination older, catching up", entry(100, 50), entry(200, 100), updateNormal},
		{"same mtime", entry(200, 50), entry(200, 100), updateNormal},
		{"destination newer and complete", entry(300, 100), entry(200, 100), updateSkip},
		{"destination newer and larger", entry(300, 200), entry(200, 100), updateSkip},
		{"destination newer but truncated", entry(300, 90), entry(200, 100), updateRepair},
		// same second: sub-second ordering must still pick the winner
		{"same second, destination newer and complete", entryNs(5, 200, 100), entryNs(5, 100, 100), updateSkip},
		{"same second, destination older", entryNs(5, 100, 100), entryNs(5, 200, 100), updateNormal},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := chooseUpdateAction(tc.existing, tc.incoming); got != tc.want {
				t.Fatalf("chooseUpdateAction = %v, want %v", got, tc.want)
			}
		})
	}
}
