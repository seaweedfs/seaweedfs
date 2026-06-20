package erasure_coding

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// readEcxSizeField returns the size field of the .ecx entry for needleId, or
// (false) if the id is absent. Entries are fixed-width (id, offset, size).
func readEcxSizeField(t *testing.T, ecxPath string, needleId types.NeedleId) (types.Size, bool) {
	t.Helper()
	data, err := os.ReadFile(ecxPath)
	if err != nil {
		t.Fatalf("read ecx: %v", err)
	}
	entry := types.NeedleIdSize + types.OffsetSize + types.SizeSize
	for off := 0; off+entry <= len(data); off += entry {
		id := types.BytesToNeedleId(data[off : off+types.NeedleIdSize])
		if id == needleId {
			return types.BytesToSize(data[off+types.NeedleIdSize+types.OffsetSize : off+entry]), true
		}
	}
	return 0, false
}

func writeEcxEntries(t *testing.T, ecxPath string, ids []types.NeedleId) {
	t.Helper()
	var buf []byte
	for i, id := range ids {
		b := make([]byte, types.NeedleIdSize+types.OffsetSize+types.SizeSize)
		types.NeedleIdToBytes(b[0:types.NeedleIdSize], id)
		// A plausible non-zero offset/size; the fold only rewrites the size field.
		types.OffsetToBytes(b[types.NeedleIdSize:types.NeedleIdSize+types.OffsetSize], types.ToOffset(int64((i+1)*1024)))
		types.SizeToBytes(b[types.NeedleIdSize+types.OffsetSize:], types.Size(100))
		buf = append(buf, b...)
	}
	if err := os.WriteFile(ecxPath, buf, 0644); err != nil {
		t.Fatalf("write ecx: %v", err)
	}
}

func packEcjIds(ids []types.NeedleId) []byte {
	var buf []byte
	for _, id := range ids {
		b := make([]byte, types.NeedleIdSize)
		types.NeedleIdToBytes(b, id)
		buf = append(buf, b...)
	}
	return buf
}

// TestRebuildEcxFile_AppliesTombstonesAndRemovesJournal: a clean fold marks the
// journaled needle deleted in .ecx and removes the journal.
func TestRebuildEcxFile_AppliesTombstonesAndRemovesJournal(t *testing.T) {
	base := filepath.Join(t.TempDir(), "vol")
	writeEcxEntries(t, base+".ecx", []types.NeedleId{1, 2, 3})
	if err := os.WriteFile(base+".ecj", packEcjIds([]types.NeedleId{2}), 0644); err != nil {
		t.Fatalf("write ecj: %v", err)
	}

	if err := RebuildEcxFile(base); err != nil {
		t.Fatalf("RebuildEcxFile: %v", err)
	}

	if _, err := os.Stat(base + ".ecj"); !os.IsNotExist(err) {
		t.Errorf(".ecj should be removed after a clean fold, stat err=%v", err)
	}
	if sz, ok := readEcxSizeField(t, base+".ecx", 2); !ok || sz != types.TombstoneFileSize {
		t.Errorf("needle 2 should be tombstoned in .ecx, got size=%d ok=%v", sz, ok)
	}
	for _, live := range []types.NeedleId{1, 3} {
		if sz, ok := readEcxSizeField(t, base+".ecx", live); !ok || sz == types.TombstoneFileSize {
			t.Errorf("needle %d should remain live, got size=%d ok=%v", live, sz, ok)
		}
	}
}

// TestRebuildEcxFile_TornJournalAborts: a journal whose tail is a partial record
// (e.g. interrupted append) must abort the fold and leave .ecj in place for a
// retry, rather than silently dropping the remaining tombstones before unlink.
func TestRebuildEcxFile_TornJournalAborts(t *testing.T) {
	base := filepath.Join(t.TempDir(), "vol")
	writeEcxEntries(t, base+".ecx", []types.NeedleId{1, 2, 3})
	// One full id followed by a truncated trailing record.
	torn := append(packEcjIds([]types.NeedleId{2}), []byte{0x01, 0x02, 0x03}...)
	if err := os.WriteFile(base+".ecj", torn, 0644); err != nil {
		t.Fatalf("write ecj: %v", err)
	}

	if err := RebuildEcxFile(base); err == nil {
		t.Fatal("expected an error folding a torn journal, got nil")
	}
	if _, err := os.Stat(base + ".ecj"); err != nil {
		t.Errorf(".ecj must be retained after a torn-journal abort, stat err=%v", err)
	}
}

// TestRebuildEcxFile_NoJournalNoop: with no .ecj the fold is a no-op and does
// not error (idempotent re-run after a prior successful fold).
func TestRebuildEcxFile_NoJournalNoop(t *testing.T) {
	base := filepath.Join(t.TempDir(), "vol")
	writeEcxEntries(t, base+".ecx", []types.NeedleId{1, 2, 3})
	before, _ := os.ReadFile(base + ".ecx")

	if err := RebuildEcxFile(base); err != nil {
		t.Fatalf("RebuildEcxFile with no journal: %v", err)
	}
	after, _ := os.ReadFile(base + ".ecx")
	if len(before) != len(after) {
		t.Errorf(".ecx changed despite no journal: %d -> %d bytes", len(before), len(after))
	}
}

// makeRebuildInputs creates `present` equal-size input shard files of `size`
// bytes and one missing output file, returning slices sized to ctx.Total().
func makeRebuildInputs(t *testing.T, dir string, present, missingIdx int, size int) ([]bool, []*os.File, []*os.File) {
	t.Helper()
	ctx := NewDefaultECContext("", 0)
	shardHasData := make([]bool, ctx.Total())
	inputFiles := make([]*os.File, ctx.Total())
	outputFiles := make([]*os.File, ctx.Total())
	for i := 0; i < present; i++ {
		p := filepath.Join(dir, "in"+ToExt(i))
		if err := os.WriteFile(p, make([]byte, size), 0644); err != nil {
			t.Fatalf("write input %d: %v", i, err)
		}
		f, err := os.Open(p)
		if err != nil {
			t.Fatalf("open input %d: %v", i, err)
		}
		t.Cleanup(func() { f.Close() })
		inputFiles[i] = f
		shardHasData[i] = true
	}
	outPath := filepath.Join(dir, "out"+ToExt(missingIdx))
	of, err := os.OpenFile(outPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("open output: %v", err)
	}
	t.Cleanup(func() { of.Close() })
	outputFiles[missingIdx] = of
	shardHasData[missingIdx] = false
	return shardHasData, inputFiles, outputFiles
}

// TestRebuildEcFiles_TruncatedInputErrorsWithoutPublishing: a truncated input
// shard must abort the rebuild before any output is written, rather than
// reconstructing over a zero-padded tail and publishing a corrupt shard.
func TestRebuildEcFiles_TruncatedInputErrorsWithoutPublishing(t *testing.T) {
	dir := t.TempDir()
	ctx := NewDefaultECContext("", 0)
	const size = 4096
	shardHasData, inputFiles, outputFiles := makeRebuildInputs(t, dir, ctx.Total()-1, ctx.Total()-1, size)

	// Truncate one present input to half.
	if err := os.Truncate(filepath.Join(dir, "in"+ToExt(5)), size/2); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	err := rebuildEcFiles(shardHasData, inputFiles, outputFiles, ctx)
	if err == nil {
		t.Fatal("expected an error on a truncated input shard, got nil")
	}
	if fi, _ := os.Stat(filepath.Join(dir, "out"+ToExt(ctx.Total()-1))); fi != nil && fi.Size() != 0 {
		t.Errorf("output shard must not be published on a truncated input, got %d bytes", fi.Size())
	}
}

// TestRebuildEcFiles_ZeroSizeInputsErrors: all-empty input shards must error
// rather than producing and publishing zero-length output shards.
func TestRebuildEcFiles_ZeroSizeInputsErrors(t *testing.T) {
	dir := t.TempDir()
	ctx := NewDefaultECContext("", 0)
	shardHasData, inputFiles, outputFiles := makeRebuildInputs(t, dir, ctx.Total()-1, ctx.Total()-1, 0)

	if err := rebuildEcFiles(shardHasData, inputFiles, outputFiles, ctx); err == nil {
		t.Fatal("expected an error on zero-size input shards, got nil")
	}
}

// TestRebuildEcFiles_HappyPathRebuildsByteIdentical: a real encode, drop one
// shard, rebuild, and confirm the regenerated shard is byte-identical. The tiny
// .dat exercises the sub-1MB final-block path (the guard must not false-positive
// on a legitimate short tail).
func TestRebuildEcFiles_HappyPathRebuildsByteIdentical(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "vol")
	ctx := NewDefaultECContext("", 0)
	writeRandomDat(t, base, 7000)

	if _, err := generateEcFiles(base, 256*1024, ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize, ctx); err != nil {
		t.Fatalf("generateEcFiles: %v", err)
	}
	const dropped = 5
	want, err := os.ReadFile(base + ToExt(dropped))
	if err != nil {
		t.Fatalf("read shard %d: %v", dropped, err)
	}
	if err := os.Remove(base + ToExt(dropped)); err != nil {
		t.Fatalf("remove shard: %v", err)
	}

	if _, err := RebuildEcFiles(base, ctx, true); err != nil {
		t.Fatalf("RebuildEcFiles: %v", err)
	}
	got, err := os.ReadFile(base + ToExt(dropped))
	if err != nil {
		t.Fatalf("read rebuilt shard: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("rebuilt shard %d differs from original (%d vs %d bytes)", dropped, len(got), len(want))
	}
}

// TestRebuildEcFiles_CustomRatioRebuildsByteIdentical: the same for a 9+3 ratio,
// confirming no 10+4 assumption leaks into the destructive rebuild path.
func TestRebuildEcFiles_CustomRatioRebuildsByteIdentical(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "vol")
	ctx := &ECContext{DataShards: 9, ParityShards: 3}
	writeRandomDat(t, base, 9000)

	if _, err := generateEcFiles(base, 256*1024, ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize, ctx); err != nil {
		t.Fatalf("generateEcFiles: %v", err)
	}
	const dropped = 7
	want, err := os.ReadFile(base + ToExt(dropped))
	if err != nil {
		t.Fatalf("read shard %d: %v", dropped, err)
	}
	if err := os.Remove(base + ToExt(dropped)); err != nil {
		t.Fatalf("remove shard: %v", err)
	}

	if _, err := RebuildEcFiles(base, ctx, true); err != nil {
		t.Fatalf("RebuildEcFiles: %v", err)
	}
	got, err := os.ReadFile(base + ToExt(dropped))
	if err != nil {
		t.Fatalf("read rebuilt shard: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("rebuilt 9+3 shard %d differs (%d vs %d bytes)", dropped, len(got), len(want))
	}
	// No shard position beyond the 9+3 layout must have been created.
	if _, err := os.Stat(base + ToExt(12)); err == nil {
		t.Errorf("unexpected shard .ec12 for a 9+3 volume")
	}
}
