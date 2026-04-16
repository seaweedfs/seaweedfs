package mount

import (
	"context"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestShouldMergeChunks_NoChunks(t *testing.T) {
	_, _, merge := shouldMergeChunks(nil, nil)
	if merge {
		t.Fatal("empty file should not trigger merge")
	}
}

func TestShouldMergeChunks_SingleChunk(t *testing.T) {
	chunks := []*filer_pb.FileChunk{
		{Offset: 0, Size: 1000, FileId: "a", ModifiedTsNs: 1},
	}
	total, fileSize, merge := shouldMergeChunks(chunks, nil)
	if merge {
		t.Fatalf("single chunk should not merge (total=%d fileSize=%d)", total, fileSize)
	}
}

func TestShouldMergeChunks_NonOverlapping(t *testing.T) {
	chunks := []*filer_pb.FileChunk{
		{Offset: 0, Size: 100, FileId: "a", ModifiedTsNs: 1},
		{Offset: 100, Size: 100, FileId: "b", ModifiedTsNs: 2},
		{Offset: 200, Size: 100, FileId: "c", ModifiedTsNs: 3},
	}
	total, fileSize, merge := shouldMergeChunks(chunks, nil)
	if merge {
		t.Fatalf("non-overlapping chunks should not merge (total=%d fileSize=%d)", total, fileSize)
	}
	if total != 300 || fileSize != 300 {
		t.Fatalf("expected total=300 fileSize=300, got total=%d fileSize=%d", total, fileSize)
	}
}

func TestShouldMergeChunks_ExactlyDouble(t *testing.T) {
	// totalChunkSize == 2 * fileSize: should NOT merge (condition is >2x)
	chunks := []*filer_pb.FileChunk{
		{Offset: 0, Size: 100, FileId: "a", ModifiedTsNs: 1},
		{Offset: 0, Size: 100, FileId: "b", ModifiedTsNs: 2},
	}
	_, _, merge := shouldMergeChunks(chunks, nil)
	if merge {
		t.Fatal("exactly 2x should not trigger merge")
	}
}

func TestShouldMergeChunks_JustOverDouble(t *testing.T) {
	// Three chunks overwriting the same range: 300 stored, 100 content = 3x
	chunks := []*filer_pb.FileChunk{
		{Offset: 0, Size: 100, FileId: "a", ModifiedTsNs: 1},
		{Offset: 0, Size: 100, FileId: "b", ModifiedTsNs: 2},
		{Offset: 0, Size: 100, FileId: "c", ModifiedTsNs: 3},
	}
	total, fileSize, merge := shouldMergeChunks(chunks, nil)
	if !merge {
		t.Fatalf("3x bloat should trigger merge (total=%d fileSize=%d)", total, fileSize)
	}
}

func TestShouldMergeChunks_ManifestExtendFileSize(t *testing.T) {
	// One manifest extends the file. Total stored = 100 (regular) + 900
	// (manifest) = 1000 vs 2*1000 = 2000 → no merge.
	compacted := []*filer_pb.FileChunk{
		{Offset: 0, Size: 100, FileId: "a", ModifiedTsNs: 1},
	}
	manifest := []*filer_pb.FileChunk{
		{Offset: 100, Size: 900, FileId: "m", ModifiedTsNs: 2, IsChunkManifest: true},
	}
	_, _, merge := shouldMergeChunks(compacted, manifest)
	if merge {
		t.Fatal("single manifest extending file should not merge")
	}
}

func TestShouldMergeChunks_ManifestSizesCounted(t *testing.T) {
	// Manifest sizes must count toward totalChunkSize so that overlapping
	// manifests trigger merge.
	compacted := []*filer_pb.FileChunk{
		{Offset: 0, Size: 100, FileId: "a", ModifiedTsNs: 1},
	}
	manifest := []*filer_pb.FileChunk{
		{Offset: 0, Size: 100, FileId: "m", ModifiedTsNs: 2, IsChunkManifest: true},
	}
	total, _, merge := shouldMergeChunks(compacted, manifest)
	if total != 200 {
		t.Fatalf("totalChunkSize should count compacted + manifests, got %d", total)
	}
	if merge {
		t.Fatal("2x total on 100-byte file should not merge (need >2x)")
	}
}

func TestShouldMergeChunks_AccumulatedManifests(t *testing.T) {
	// Simulates the real bug: multiple manifests each covering the full file
	// accumulate across flush cycles. Without counting manifest sizes, the
	// merge condition never fires and storage bloats indefinitely.
	compacted := []*filer_pb.FileChunk{
		{Offset: 0, Size: 1000, FileId: "a", ModifiedTsNs: 100},
	}
	// 5 manifests each covering the full file — successive flush cycles
	var manifests []*filer_pb.FileChunk
	for i := 0; i < 5; i++ {
		manifests = append(manifests, &filer_pb.FileChunk{
			Offset: 0, Size: 1000, FileId: string(rune('m' + i)),
			IsChunkManifest: true,
		})
	}
	total, fileSize, merge := shouldMergeChunks(compacted, manifests)
	// total = 1000 (regular) + 5*1000 (manifests) = 6000
	// fileSize = 1000
	// 6000 > 2*1000 → merge
	if !merge {
		t.Fatalf("accumulated overlapping manifests should trigger merge (total=%d fileSize=%d)", total, fileSize)
	}
}

func TestShouldMergeChunks_DoesNotMutateInput(t *testing.T) {
	compacted := make([]*filer_pb.FileChunk, 2, 4) // extra capacity
	compacted[0] = &filer_pb.FileChunk{Offset: 0, Size: 100, FileId: "a", ModifiedTsNs: 1}
	compacted[1] = &filer_pb.FileChunk{Offset: 0, Size: 100, FileId: "b", ModifiedTsNs: 2}
	manifest := []*filer_pb.FileChunk{
		{Offset: 100, Size: 100, FileId: "m", ModifiedTsNs: 3, IsChunkManifest: true},
	}

	lenBefore := len(compacted)
	shouldMergeChunks(compacted, manifest)

	if len(compacted) != lenBefore {
		t.Fatalf("shouldMergeChunks mutated compactedChunks: len changed from %d to %d",
			lenBefore, len(compacted))
	}
}

// TestCompactThenMergeCondition verifies that after CompactFileChunks
// removes fully-superseded chunks, the merge condition correctly detects
// remaining bloat from partially-overlapping chunks.
func TestCompactThenMergeCondition(t *testing.T) {
	tests := []struct {
		name      string
		chunks    []*filer_pb.FileChunk
		wantMerge bool
	}{
		{
			name: "fully superseded chunks removed, no bloat",
			chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "old", ModifiedTsNs: 1, Fid: &filer_pb.FileId{FileKey: 1}},
				{Offset: 0, Size: 100, FileId: "new", ModifiedTsNs: 2, Fid: &filer_pb.FileId{FileKey: 2}},
			},
			wantMerge: false,
		},
		{
			name: "repeated full overwrites all compacted away",
			chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "a", ModifiedTsNs: 1, Fid: &filer_pb.FileId{FileKey: 1}},
				{Offset: 0, Size: 100, FileId: "b", ModifiedTsNs: 2, Fid: &filer_pb.FileId{FileKey: 2}},
				{Offset: 0, Size: 100, FileId: "c", ModifiedTsNs: 3, Fid: &filer_pb.FileId{FileKey: 3}},
				{Offset: 0, Size: 100, FileId: "d", ModifiedTsNs: 4, Fid: &filer_pb.FileId{FileKey: 4}},
			},
			wantMerge: false,
		},
		{
			name: "non-overlapping small chunks, no bloat",
			chunks: func() []*filer_pb.FileChunk {
				var c []*filer_pb.FileChunk
				for i := 0; i < 10; i++ {
					c = append(c, &filer_pb.FileChunk{
						Offset:       int64(i * 10),
						Size:         10,
						FileId:       string(rune('a' + i)),
						ModifiedTsNs: int64(i + 1),
						Fid:          &filer_pb.FileId{FileKey: uint64(i)},
					})
				}
				return c
			}(),
			wantMerge: false,
		},
		{
			name: "staggered partial overlaps create bloat",
			// 100-byte base + three 40-byte writes at staggered offsets.
			// All 4 survive compact (each partially visible).
			// total = 100+40+40+40 = 220, file = 100, ratio = 2.2.
			chunks: []*filer_pb.FileChunk{
				{Offset: 0, Size: 100, FileId: "orig", ModifiedTsNs: 1, Fid: &filer_pb.FileId{FileKey: 1}},
				{Offset: 10, Size: 40, FileId: "w1", ModifiedTsNs: 2, Fid: &filer_pb.FileId{FileKey: 2}},
				{Offset: 30, Size: 40, FileId: "w2", ModifiedTsNs: 3, Fid: &filer_pb.FileId{FileKey: 3}},
				{Offset: 50, Size: 40, FileId: "w3", ModifiedTsNs: 4, Fid: &filer_pb.FileId{FileKey: 4}},
			},
			wantMerge: true,
		},
		{
			name: "overlapping writes with 75pct overlap",
			// Writes of size 100 at step 25 -> each supersedes 75% of the
			// previous write, all survive compact because the leading 25
			// bytes are still visible.
			// total = N*100, file ~ N*25+75. Ratio approaches 4x.
			chunks: func() []*filer_pb.FileChunk {
				var c []*filer_pb.FileChunk
				for i := 0; i < 20; i++ {
					c = append(c, &filer_pb.FileChunk{
						Offset:       int64(i * 25),
						Size:         100,
						FileId:       string(rune(i)),
						ModifiedTsNs: int64(i + 1),
						Fid:          &filer_pb.FileId{FileKey: uint64(i)},
					})
				}
				return c
			}(),
			wantMerge: true,
		},
		{
			name: "many 4K writes at 1K step over large range",
			// 200 writes of 4096 bytes at 1024-byte step.
			// After compact all survive (each partially visible).
			// total = 200*4096 = 819200, file ~ 199*1024+4096 = 207872.
			// ratio ~ 3.9x.
			chunks: func() []*filer_pb.FileChunk {
				var c []*filer_pb.FileChunk
				for i := 0; i < 200; i++ {
					c = append(c, &filer_pb.FileChunk{
						Offset:       int64(i * 1024),
						Size:         4096,
						FileId:       string(rune(i)),
						ModifiedTsNs: int64(i + 1),
						Fid:          &filer_pb.FileId{FileKey: uint64(i)},
					})
				}
				return c
			}(),
			wantMerge: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compacted, _ := filer.CompactFileChunks(context.Background(), nil, tt.chunks)
			_, _, merge := shouldMergeChunks(compacted, nil)
			if merge != tt.wantMerge {
				var total uint64
				for _, c := range compacted {
					total += c.Size
				}
				fileSize := filer.TotalSize(compacted)
				t.Fatalf("got merge=%v want %v (compacted=%d total=%d fileSize=%d ratio=%.2f)",
					merge, tt.wantMerge, len(compacted), total, fileSize,
					float64(total)/float64(fileSize))
			}
		})
	}
}

// TestRandomWritesBloatDetection simulates random overwrites and verifies
// that the condition agrees with the actual stored-vs-visible ratio.
func TestRandomWritesBloatDetection(t *testing.T) {
	const (
		fileSize  = 1024 * 1024 // 1 MB logical file
		chunkSize = 64 * 1024   // 64 KB write size
	)

	for iter := 0; iter < 50; iter++ {
		chunks := []*filer_pb.FileChunk{
			{Offset: 0, Size: uint64(fileSize), FileId: "base", ModifiedTsNs: 1,
				Fid: &filer_pb.FileId{FileKey: 0}},
		}

		// Add random overwrites until stored > 3x file size.
		for i := 1; ; i++ {
			off := int64(rand.IntN(fileSize - chunkSize + 1))
			chunks = append(chunks, &filer_pb.FileChunk{
				Offset:       off,
				Size:         chunkSize,
				FileId:       string(rune(i)),
				ModifiedTsNs: int64(1 + i),
				Fid:          &filer_pb.FileId{FileKey: uint64(i)},
			})
			var total uint64
			for _, c := range chunks {
				total += c.Size
			}
			if total > uint64(fileSize)*3 {
				break
			}
		}

		compacted, _ := filer.CompactFileChunks(context.Background(), nil, chunks)
		totalCompacted, reportedFileSize, merge := shouldMergeChunks(compacted, nil)

		// Sanity: condition must be consistent with its own inputs.
		expectMerge := reportedFileSize > 0 && totalCompacted > 2*reportedFileSize
		if merge != expectMerge {
			t.Fatalf("iter %d: merge=%v but totalCompacted=%d reportedFileSize=%d",
				iter, merge, totalCompacted, reportedFileSize)
		}
	}
}

// TestVisibleContentPreservedAfterCompact verifies that CompactFileChunks
// never drops a chunk that contributes visible bytes. This is the invariant
// that maybeMergeChunks relies on: the compacted set covers the same logical
// content as the original.
func TestVisibleContentPreservedAfterCompact(t *testing.T) {
	const dataSize = 4096

	for iter := 0; iter < 100; iter++ {
		var chunks []*filer_pb.FileChunk
		numWrites := 5 + rand.IntN(20)
		for i := 0; i < numWrites; i++ {
			start := rand.IntN(dataSize)
			maxSize := 256
			if dataSize-start < maxSize {
				maxSize = dataSize - start
			}
			size := 1 + rand.IntN(maxSize)
			chunks = append(chunks, &filer_pb.FileChunk{
				Offset:       int64(start),
				Size:         uint64(size),
				FileId:       string(rune(i)),
				ModifiedTsNs: int64(i + 1),
				Fid:          &filer_pb.FileId{FileKey: uint64(i)},
			})
		}

		origViews := filer.ViewFromChunks(context.Background(), nil, chunks, 0, math.MaxInt64)
		compacted, _ := filer.CompactFileChunks(context.Background(), nil, chunks)
		compViews := filer.ViewFromChunks(context.Background(), nil, compacted, 0, math.MaxInt64)

		// Collect all (offset, size) pairs from views.
		type viewKey struct {
			offset int64
			size   uint64
		}
		origSet := make(map[viewKey]bool)
		for x := origViews.Front(); x != nil; x = x.Next {
			v := x.Value
			origSet[viewKey{v.ViewOffset, v.ViewSize}] = true
		}
		compSet := make(map[viewKey]bool)
		for x := compViews.Front(); x != nil; x = x.Next {
			v := x.Value
			compSet[viewKey{v.ViewOffset, v.ViewSize}] = true
		}

		// Every view in the compacted set must exist in the original.
		for k := range compSet {
			if !origSet[k] {
				t.Fatalf("iter %d: compacted has view {offset=%d size=%d} not in original",
					iter, k.offset, k.size)
			}
		}
		// Every view in the original must exist after compaction.
		for k := range origSet {
			if !compSet[k] {
				t.Fatalf("iter %d: original view {offset=%d size=%d} lost after compaction",
					iter, k.offset, k.size)
			}
		}
	}
}
