package page_writer

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestUploadPipeline(t *testing.T) {

	uploadPipeline := NewUploadPipeline(nil, 2*1024*1024, nil, 16, "", nil)

	writeRange(uploadPipeline, 0, 131072)
	writeRange(uploadPipeline, 131072, 262144)
	writeRange(uploadPipeline, 262144, 1025536)

	confirmRange(t, uploadPipeline, 0, 1025536)

	writeRange(uploadPipeline, 1025536, 1296896)

	confirmRange(t, uploadPipeline, 1025536, 1296896)

	writeRange(uploadPipeline, 1296896, 2162688)

	confirmRange(t, uploadPipeline, 1296896, 2162688)

	confirmRange(t, uploadPipeline, 1296896, 2162688)
}

// startOff and stopOff must be divided by 4
func writeRange(uploadPipeline *UploadPipeline, startOff, stopOff int64) {
	p := make([]byte, 4)
	for i := startOff / 4; i < stopOff/4; i += 4 {
		util.Uint32toBytes(p, uint32(i))
		uploadPipeline.SaveDataAt(p, i, false, 0)
	}
}

func confirmRange(t *testing.T, uploadPipeline *UploadPipeline, startOff, stopOff int64) {
	p := make([]byte, 4)
	for i := startOff; i < stopOff/4; i += 4 {
		uploadPipeline.MaybeReadDataAt(p, i, 0)
		x := util.BytesToUint32(p)
		if x != uint32(i) {
			t.Errorf("expecting %d found %d at offset [%d,%d)", i, x, i, i+4)
		}
	}
}

// TestEvictOneWritableChunk_SkipsGappyChunks pins the issue #9330 fix:
// pressure-driven eviction must not seal a chunk whose written intervals
// have a hole (leading or internal), because SaveContent would emit
// multiple volume chunks with no coverage for the hole and reads would
// silently zero-fill it (filer/stream.go writeZero on gap).
func TestEvictOneWritableChunk_SkipsGappyChunks(t *testing.T) {
	const cs int64 = 2 * 1024 * 1024
	// saveToStorage = nil so that the async upload triggered by
	// moveToSealed is a no-op (SaveContent short-circuits on nil saveFn).
	up := NewUploadPipeline(util.NewLimitedConcurrentExecutor(2), cs, nil, 16, "", nil)

	block := make([]byte, cs/4) // 512 KiB

	// Chunk 0: internal gap — first quarter and last quarter written.
	// Mirrors FUSE writeback mid-flight on sequential cp.
	if _, err := up.SaveDataAt(block, 0, true, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := up.SaveDataAt(block, 3*cs/4, true, 2); err != nil {
		t.Fatal(err)
	}
	// Chunk 1: leading gap — second and third quarters written, no byte 0.
	// Mirrors FUSE writeback dispatching middle pages first.
	if _, err := up.SaveDataAt(block, cs+cs/4, true, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := up.SaveDataAt(block, cs+cs/2, true, 4); err != nil {
		t.Fatal(err)
	}
	// Chunk 2: contiguous from offset 0 — first half written, no hole.
	if _, err := up.SaveDataAt(block, 2*cs, true, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := up.SaveDataAt(block, 2*cs+cs/4, true, 6); err != nil {
		t.Fatal(err)
	}

	// Eviction must pick chunk 2 (contiguous from 0). Never chunks 0
	// (internal gap) or 1 (leading gap), even though all three have
	// the same WrittenSize.
	if !up.EvictOneWritableChunk() {
		t.Fatalf("EvictOneWritableChunk returned false; expected contiguous chunk 2 to be evictable")
	}
	if _, stillWritable := up.writableChunks[LogicChunkIndex(2)]; stillWritable {
		t.Errorf("chunk 2 should have moved to sealed")
	}
	if _, stillWritable := up.writableChunks[LogicChunkIndex(0)]; !stillWritable {
		t.Errorf("chunk 0 (internal gap) must remain writable so its hole can still be filled")
	}
	if _, stillWritable := up.writableChunks[LogicChunkIndex(1)]; !stillWritable {
		t.Errorf("chunk 1 (leading gap) must remain writable so its leading range can still be filled")
	}

	// With chunk 2 sealed, EvictOneWritableChunk has nothing else to seal —
	// the remaining chunks are both gappy and must wait for FUSE writeback
	// to fill them or for FlushAll at file close.
	if up.EvictOneWritableChunk() {
		t.Errorf("EvictOneWritableChunk returned true with only gappy chunks left")
	}

	// Fill chunk 0's middle and chunk 1's leading + trailing ranges.
	// Both now cover [0, full) within their logicChunkIndex;
	// SaveDataAt's maybeMoveToSealed auto-seals on IsComplete, so they
	// leave writableChunks on their own.
	if _, err := up.SaveDataAt(block, cs/4, true, 7); err != nil { // chunk 0 middle a
		t.Fatal(err)
	}
	if _, err := up.SaveDataAt(block, cs/2, true, 8); err != nil { // chunk 0 middle b
		t.Fatal(err)
	}
	if _, err := up.SaveDataAt(block, cs, true, 9); err != nil { // chunk 1 leading
		t.Fatal(err)
	}
	if _, err := up.SaveDataAt(block, cs+3*cs/4, true, 10); err != nil { // chunk 1 trailing
		t.Fatal(err)
	}
	if _, stillWritable := up.writableChunks[LogicChunkIndex(0)]; stillWritable {
		t.Errorf("chunk 0 should have auto-sealed on IsComplete after gap was filled")
	}
	if _, stillWritable := up.writableChunks[LogicChunkIndex(1)]; stillWritable {
		t.Errorf("chunk 1 should have auto-sealed on IsComplete after leading range was filled")
	}
}
