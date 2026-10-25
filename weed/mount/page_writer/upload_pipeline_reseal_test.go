package page_writer

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// gatedChunk parks in SaveContent until the test opens its gate: the state
// moveToSealed leaves an upload in, submitted but not yet reading its buffer.
type gatedChunk struct {
	gate        chan struct{}
	saved       chan struct{}
	freedBefore bool // FreeResource had already run when SaveContent began
	freeCount   atomic.Int32
}

func newGatedChunk() *gatedChunk {
	return &gatedChunk{
		gate:  make(chan struct{}),
		saved: make(chan struct{}),
	}
}

func (c *gatedChunk) FreeResource()                         { c.freeCount.Add(1) }
func (c *gatedChunk) WriteDataAt([]byte, int64, int64) int  { return 0 }
func (c *gatedChunk) ReadDataAt([]byte, int64, int64) int64 { return 0 }
func (c *gatedChunk) IsComplete() bool                      { return false }
func (c *gatedChunk) IsContiguouslyWritten() bool           { return true }
func (c *gatedChunk) ActivityScore() int64                  { return 0 }
func (c *gatedChunk) WrittenSize() int64                    { return 0 }
func (c *gatedChunk) LastWriteTsNs() int64                  { return 0 }
func (c *gatedChunk) SaveContent(saveFn SaveToStorageFunc) {
	<-c.gate
	c.freedBefore = c.freeCount.Load() > 0
	close(c.saved)
}

// Sealing the same index twice must not free the first chunk: freeing it
// returns a live buffer to the slot pool and the in-flight upload then ships
// whatever the next NewMemChunk writes there.
func TestMoveToSealed_ReplaceKeepsUploadReference(t *testing.T) {
	up := NewUploadPipeline(util.NewLimitedConcurrentExecutor(2), 2*1024*1024, nil, 16, "", nil)

	first, second := newGatedChunk(), newGatedChunk()

	up.chunksLock.Lock()
	up.moveToSealed(first, 0)
	up.moveToSealed(second, 0) // takes over index 0 while first is uploading
	up.chunksLock.Unlock()

	// both seals are done, so any premature free has already happened
	close(first.gate)
	select {
	case <-first.saved:
	case <-time.After(5 * time.Second):
		t.Fatal("first chunk's SaveContent never ran")
	}
	if first.freedBefore {
		t.Error("first chunk was freed before its upload read it")
	}

	// a finished upload must leave the newer chunk indexed for readers
	waitForFree(t, first)
	up.chunksLock.Lock()
	indexed := up.sealedChunks[0]
	up.chunksLock.Unlock()
	if indexed == nil || indexed.chunk != second {
		t.Errorf("index 0 should still hold the second chunk, got %v", indexed)
	}

	close(second.gate)
	waitForFree(t, second)
	if got := first.freeCount.Load(); got != 1 {
		t.Errorf("first chunk freed %d times, want 1", got)
	}
	if got := second.freeCount.Load(); got != 1 {
		t.Errorf("second chunk freed %d times, want 1", got)
	}
}

// Shutdown drops only the slot reference; the in-flight upload keeps the
// chunk alive until it is done, then frees it exactly once.
func TestShutdown_LeavesInFlightUploadItsReference(t *testing.T) {
	up := NewUploadPipeline(util.NewLimitedConcurrentExecutor(2), 2*1024*1024, nil, 16, t.TempDir(), nil)

	chunk := newGatedChunk()
	up.chunksLock.Lock()
	up.moveToSealed(chunk, 0)
	up.chunksLock.Unlock()

	up.Shutdown()

	close(chunk.gate)
	select {
	case <-chunk.saved:
	case <-time.After(5 * time.Second):
		t.Fatal("SaveContent never ran")
	}
	if chunk.freedBefore {
		t.Error("Shutdown freed the chunk before its upload read it")
	}
	waitForFree(t, chunk)
	if got := chunk.freeCount.Load(); got != 1 {
		t.Errorf("chunk freed %d times, want 1", got)
	}
}

func waitForFree(t *testing.T, c *gatedChunk) {
	t.Helper()
	<-c.saved
	deadline := time.Now().Add(5 * time.Second)
	for c.freeCount.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("chunk was never freed after its upload finished")
		}
		time.Sleep(time.Millisecond)
	}
}
