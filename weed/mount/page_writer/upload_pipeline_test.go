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
// have an internal hole, because SaveContent would emit multiple volume
// chunks with no coverage for the hole and reads would silently zero-fill
// it (filer/stream.go writeZero on gap).
func TestEvictOneWritableChunk_SkipsGappyChunks(t *testing.T) {
	const cs int64 = 2 * 1024 * 1024
	// saveToStorage = nil so that the async upload triggered by
	// moveToSealed is a no-op (SaveContent short-circuits on nil saveFn).
	up := NewUploadPipeline(util.NewLimitedConcurrentExecutor(2), cs, nil, 16, "", nil)

	block := make([]byte, cs/4) // 512 KiB

	// Chunk 0: gappy — first quarter and last quarter written, middle hole.
	// Mirrors FUSE writeback mid-flight on sequential cp.
	if _, err := up.SaveDataAt(block, 0, true, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := up.SaveDataAt(block, 3*cs/4, true, 2); err != nil {
		t.Fatal(err)
	}
	// Chunk 1: contiguous — first half written, no hole.
	if _, err := up.SaveDataAt(block, cs, true, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := up.SaveDataAt(block, cs+cs/4, true, 4); err != nil {
		t.Fatal(err)
	}

	// Eviction must pick chunk 1 (contiguous), never chunk 0 (gappy),
	// even though both have the same WrittenSize.
	if !up.EvictOneWritableChunk() {
		t.Fatalf("EvictOneWritableChunk returned false; expected contiguous chunk 1 to be evictable")
	}
	if _, stillWritable := up.writableChunks[LogicChunkIndex(1)]; stillWritable {
		t.Errorf("chunk 1 should have moved to sealed")
	}
	if _, stillWritable := up.writableChunks[LogicChunkIndex(0)]; !stillWritable {
		t.Errorf("chunk 0 (gappy) must remain writable so its hole can still be filled")
	}

	// Fill chunk 0's middle. The chunk now covers [0, cs) and SaveDataAt's
	// maybeMoveToSealed auto-seals on completion, so it leaves writableChunks
	// without our needing to call EvictOneWritableChunk.
	if _, err := up.SaveDataAt(block, cs/4, true, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := up.SaveDataAt(block, cs/2, true, 6); err != nil {
		t.Fatal(err)
	}
	if _, stillWritable := up.writableChunks[LogicChunkIndex(0)]; stillWritable {
		t.Errorf("chunk 0 should have left writableChunks after gap was filled (auto-seal on IsComplete)")
	}
}
