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

// entryChunks builds an entry with nChunks contiguous 1-byte chunks at a given
// mtime, so coveredBytes == nChunks. Different counts give different coverage,
// which is what the truncation-repair routing keys on.
func entryChunks(mtime int64, nChunks int) *filer_pb.Entry {
	e := &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{Mtime: mtime, FileSize: uint64(nChunks)}}
	for i := range nChunks {
		e.Chunks = append(e.Chunks, &filer_pb.FileChunk{
			FileId: "3," + string(rune('a'+i)), Offset: int64(i), Size: 1,
		})
	}
	return e
}

// entryMd5 is like entryChunks but also sets attr.Md5, so filer.ETag returns the
// stored Md5 (the S3 PutObject case) instead of a chunk-derived ETag. Used to show
// that coverage — not the Md5-backed ETag — drives the truncation decision.
func entryMd5(mtime int64, nChunks int, md5 []byte) *filer_pb.Entry {
	e := entryChunks(mtime, nChunks)
	e.Attributes.Md5 = md5
	return e
}

// srcEntry builds a source-side entry at mtime whose chunks carry the given
// FileIds — the identities a backup destination's SourceFileId is matched against.
func srcEntry(mtime int64, fids ...string) *filer_pb.Entry {
	e := &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{Mtime: mtime}}
	for i, f := range fids {
		e.Chunks = append(e.Chunks, &filer_pb.FileChunk{FileId: f, Offset: int64(i), Size: 1})
	}
	return e
}

// dstEntry builds a destination entry at mtime. An empty src marks a chunk
// written out-of-band (no SourceFileId, e.g. rsync/direct); otherwise it is a
// backup chunk copied from that source FileId.
func dstEntry(mtime int64, srcFids ...string) *filer_pb.Entry {
	e := &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{Mtime: mtime}}
	for i, s := range srcFids {
		e.Chunks = append(e.Chunks, &filer_pb.FileChunk{FileId: "dst", SourceFileId: s, Offset: int64(i), Size: 1})
	}
	return e
}

// ivEntry builds an entry at mtime with chunks at the given [offset,end) byte
// ranges — for coverage/containment tests where exact ranges matter.
func ivEntry(mtime int64, ivs ...[2]int64) *filer_pb.Entry {
	e := &filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{Mtime: mtime}}
	for _, iv := range ivs {
		e.Chunks = append(e.Chunks, &filer_pb.FileChunk{Offset: iv[0], Size: uint64(iv[1] - iv[0])})
	}
	return e
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
		oldEntry *filer_pb.Entry
		incoming *filer_pb.Entry
		want     updateAction
	}{
		{"nil existing entry", nil, entry(100, 100), entry(200, 100), updateNormal},
		{"destination older, catching up", entry(100, 50), entry(100, 50), entry(200, 100), updateNormal},
		{"same mtime", entry(200, 50), entry(200, 50), entry(200, 100), updateNormal},
		{"destination newer and complete", entry(300, 100), entry(100, 100), entry(200, 100), updateSkip},
		{"destination newer and larger", entry(300, 200), entry(100, 100), entry(200, 100), updateSkip},
		// out-of-order: a newer (smaller) destination must NOT be rolled back by an
		// older, larger replayed event — even though it does not cover oldEntry.
		{"newer smaller dest, older larger event -> skip", ivEntry(300, [2]int64{0, 5}), ivEntry(100, [2]int64{0, 10}), ivEntry(100, [2]int64{0, 10}), updateSkip},
		// same second: sub-second ordering must still pick the winner
		{"same second, destination newer and complete", entryNs(5, 200, 100), entryNs(5, 100, 100), entryNs(5, 100, 100), updateSkip},
		{"same second, destination older", entryNs(5, 100, 100), entryNs(5, 100, 100), entryNs(5, 200, 100), updateNormal},
		// chunk-incompleteness (the truncation bug): destination is at/behind the
		// incoming mtime but covers fewer bytes than oldEntry, so the incremental
		// diff cannot heal it — must repair in full, not append. entryChunks(n) has
		// n contiguous 1-byte chunks → coverage n.
		{"behind, covers less than oldEntry -> repair", entryChunks(200, 2), entryChunks(200, 4), entryChunks(200, 4), updateRepair},
		{"behind, matches oldEntry coverage -> normal", entryChunks(200, 3), entryChunks(200, 3), entryChunks(200, 5), updateNormal},
		{"behind, exceeds oldEntry coverage -> normal", entryChunks(200, 5), entryChunks(200, 3), entryChunks(200, 5), updateNormal},
		// range containment, not scalar coverage: equal total bytes but a missing
		// range (the destination has an extra range at another offset) → repair.
		{"behind, equal total but range gap -> repair", ivEntry(200, [2]int64{0, 10}, [2]int64{30, 40}), ivEntry(200, [2]int64{0, 20}), ivEntry(200, [2]int64{0, 20}), updateRepair},
		// same-range stale: full coverage but a backup-written chunk whose source
		// oldEntry no longer references → repair only when the destination is
		// provably older. Equal-mtime is deferred (cannot order same-second
		// versions). Out-of-band (rsync) chunks are never stale-checked.
		{"older, stale backup chunk -> repair", dstEntry(100, "A", "X"), srcEntry(100, "A", "B"), srcEntry(200, "A", "B"), updateRepair},
		{"equal-mtime stale -> deferred (normal)", dstEntry(200, "A", "X"), srcEntry(200, "A", "B"), srcEntry(200, "A", "B"), updateNormal},
		{"older, rsync chunks -> normal", dstEntry(100, "", ""), srcEntry(100, "A", "B"), srcEntry(200, "A", "B"), updateNormal},
		// in-sync fast path: destination holds exactly oldEntry's chunks → normal
		{"in-sync (fast path) -> normal", dstEntry(100, "A", "B"), srcEntry(100, "A", "B"), srcEntry(200, "A", "B"), updateNormal},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := chooseUpdateAction(tc.existing, tc.oldEntry, tc.incoming); got != tc.want {
				t.Fatalf("chooseUpdateAction = %v, want %v", got, tc.want)
			}
		})
	}
}

// coversReference must verify range containment, not just equal total bytes: a
// destination with the same byte count but a missing range (extra chunk elsewhere)
// does not cover the reference.
func TestCoversReference(t *testing.T) {
	cases := []struct {
		name                string
		existing, reference *filer_pb.Entry
		want                bool
	}{
		{"empty reference", ivEntry(0), ivEntry(0), true},
		{"reference no chunks", ivEntry(0, [2]int64{0, 10}), ivEntry(0), true},
		{"exact", ivEntry(0, [2]int64{0, 10}), ivEntry(0, [2]int64{0, 10}), true},
		{"superset", ivEntry(0, [2]int64{0, 20}), ivEntry(0, [2]int64{0, 10}), true},
		{"truncated tail", ivEntry(0, [2]int64{0, 5}), ivEntry(0, [2]int64{0, 10}), false},
		{"equal total, range gap", ivEntry(0, [2]int64{0, 10}, [2]int64{30, 40}), ivEntry(0, [2]int64{0, 20}), false},
		{"contiguous parts cover", ivEntry(0, [2]int64{0, 10}, [2]int64{10, 20}), ivEntry(0, [2]int64{0, 20}), true},
		{"overlapping parts cover", ivEntry(0, [2]int64{0, 12}, [2]int64{8, 20}), ivEntry(0, [2]int64{0, 20}), true},
		{"existing empty, reference has bytes", ivEntry(0), ivEntry(0, [2]int64{0, 1}), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := coversReference(tc.existing, tc.reference); got != tc.want {
				t.Fatalf("coversReference = %v, want %v", got, tc.want)
			}
		})
	}
}

// destinationSatisfiesSource gates CreateEntry's skip. Coverage/identity must be
// checked before ETag: a truncated or stale destination whose attr.Md5 still
// matches the source (so filer.ETag is equal) must NOT be skipped, or
// -initialSnapshot would leave it permanently diverged.
func TestDestinationSatisfiesSource(t *testing.T) {
	md5A := []byte("0123456789abcdef")
	md5B := []byte("fedcba9876543210")
	cases := []struct {
		name               string
		existing, incoming *filer_pb.Entry
		want               bool
	}{
		{"nil existing", nil, entryChunks(200, 4), false},
		{"identical complete -> skip", entryChunks(200, 4), entryChunks(200, 4), true},
		{"truncated (no md5) -> replicate", entryChunks(200, 2), entryChunks(200, 4), false},
		// regression: same attr.Md5 (equal filer.ETag) but fewer chunks must repair
		{"md5-backed truncated -> replicate", entryMd5(200, 2, md5A), entryMd5(200, 4, md5A), false},
		{"md5 equal and complete -> skip", entryMd5(200, 4, md5A), entryMd5(200, 4, md5A), true},
		// complete but different content: keep newer, replace older
		{"complete, dest newer -> skip", entryMd5(300, 4, md5A), entryMd5(200, 4, md5B), true},
		{"complete, dest older -> replicate", entryMd5(200, 4, md5A), entryMd5(300, 4, md5B), false},
		// same-range stale backup chunk: replicate only when provably older; an
		// equal-mtime stale dest is deferred (skip), and rsync chunks are not
		// stale-checked.
		{"older, stale backup chunk -> replicate", dstEntry(100, "A", "X"), srcEntry(200, "A", "B"), false},
		{"equal-mtime stale -> skip (deferred)", dstEntry(200, "A", "X"), srcEntry(200, "A", "B"), true},
		{"rsync complete -> skip", dstEntry(200, "", ""), srcEntry(200, "A", "B"), true},
		{"in-sync (fast path) -> skip", dstEntry(200, "A", "B"), srcEntry(200, "A", "B"), true},
		// out-of-order: newer (smaller) destination must not be rolled back by an
		// older, larger source replay.
		{"newer smaller dest, older larger source -> skip", ivEntry(300, [2]int64{0, 5}), ivEntry(100, [2]int64{0, 10}), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := destinationSatisfiesSource(tc.existing, tc.incoming); got != tc.want {
				t.Fatalf("destinationSatisfiesSource = %v, want %v", got, tc.want)
			}
		})
	}
}

// destinationMatchesReference is the allocation-free fast path: true only when the
// destination holds exactly the reference's chunks in order (by SourceFileId). It
// must be conservative — never a false positive — so reorders, count mismatches,
// and out-of-band (no SourceFileId) chunks all return false.
func TestDestinationMatchesReference(t *testing.T) {
	cases := []struct {
		name                string
		existing, reference *filer_pb.Entry
		want                bool
	}{
		{"both empty", dstEntry(0), srcEntry(0), true},
		{"exact in order", dstEntry(0, "A", "B"), srcEntry(0, "A", "B"), true},
		{"count mismatch", dstEntry(0, "A"), srcEntry(0, "A", "B"), false},
		{"reordered", dstEntry(0, "B", "A"), srcEntry(0, "A", "B"), false},
		{"stale identity", dstEntry(0, "A", "X"), srcEntry(0, "A", "B"), false},
		{"out-of-band chunk", dstEntry(0, "A", ""), srcEntry(0, "A", "B"), false},
		// Same identity but a different byte range must not fast-path as a match,
		// so the precise checks still run if replication ever re-chunks.
		{"shifted offset, same identity",
			&filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{Mtime: 0}, Chunks: []*filer_pb.FileChunk{
				{FileId: "dst", SourceFileId: "A", Offset: 0, Size: 1},
				{FileId: "dst", SourceFileId: "B", Offset: 5, Size: 1},
			}}, srcEntry(0, "A", "B"), false},
		{"larger size, same identity",
			&filer_pb.Entry{Attributes: &filer_pb.FuseAttributes{Mtime: 0}, Chunks: []*filer_pb.FileChunk{
				{FileId: "dst", SourceFileId: "A", Offset: 0, Size: 2},
			}}, srcEntry(0, "A"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := destinationMatchesReference(tc.existing, tc.reference); got != tc.want {
				t.Fatalf("destinationMatchesReference = %v, want %v", got, tc.want)
			}
		})
	}
}

// hasStaleBackupChunk flags a destination that still holds a backup-written chunk
// (SourceFileId set) the source reference no longer lists — a missed
// overwrite/deletion. Out-of-band chunks (no SourceFileId, e.g. rsync) are ignored.
func TestHasStaleBackupChunk(t *testing.T) {
	cases := []struct {
		name                string
		existing, reference *filer_pb.Entry
		want                bool
	}{
		{"nil existing", nil, srcEntry(200, "A"), false},
		{"in sync", dstEntry(200, "A", "B"), srcEntry(200, "A", "B"), false},
		{"stale backup chunk", dstEntry(200, "A", "X"), srcEntry(200, "A", "B"), true},
		{"rsync chunks ignored", dstEntry(200, "", ""), srcEntry(200, "A", "B"), false},
		{"synced backup + rsync", dstEntry(200, "A", ""), srcEntry(200, "A", "B"), false},
		{"stale backup among rsync", dstEntry(200, "X", ""), srcEntry(200, "A", "B"), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasStaleBackupChunk(tc.existing, tc.reference); got != tc.want {
				t.Fatalf("hasStaleBackupChunk = %v, want %v", got, tc.want)
			}
		})
	}
}

// updatedEntryKey must resolve the incoming entry's new path. For a rename the
// supersession check has to target newParentPath/newEntry.Name, not the old key,
// or the renamed-away old path looks deleted on the source and skips a live event.
func TestUpdatedEntryKey(t *testing.T) {
	named := func(name string) *filer_pb.Entry { return &filer_pb.Entry{Name: name} }
	tests := []struct {
		name          string
		key           string
		newParentPath string
		entry         *filer_pb.Entry
		want          string
	}{
		{"content update, no new parent", "/dst/a.txt", "", named("a.txt"), "/dst/a.txt"},
		{"rename same dir", "/dst/old.txt", "/dst", named("new.txt"), "/dst/new.txt"},
		{"rename to subdir", "/dst/old.txt", "/dst/sub", named("new.txt"), "/dst/sub/new.txt"},
		{"root parent", "/old.txt", "/", named("new.txt"), "/new.txt"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := updatedEntryKey(tc.key, tc.newParentPath, tc.entry); got != tc.want {
				t.Fatalf("updatedEntryKey = %q, want %q", got, tc.want)
			}
		})
	}
}
