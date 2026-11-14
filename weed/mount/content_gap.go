package mount

import (
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// A publication owner carries provider refusal evidence through every retry.
// Helpers and the periodic flusher never retire content that is still owned.
type contentPublication struct {
	refused bool
	failure *atomic.Bool
	// writes is the handle's pageWrites before its pages were flushed. A
	// later write is not in the published chunks, so it keeps the handle dirty.
	writes uint64
}

func (p *contentPublication) retire(err error) {
	if err != nil && p.refused {
		p.abandon()
	}
}

// An abandoned writeback drain leaves the kernel EOF past the stored one,
// whatever the filer answered.
func (p *contentPublication) abandon() {
	if p.failure != nil {
		p.failure.Store(true)
	}
}

func (wfs *WFS) checkContentPublication(fh *FileHandle, base *filer_pb.Entry) error {
	if wfs.option.WritebackCache && (fh.writebackContentError == nil || fh.writebackContentError.Load()) {
		return fmt.Errorf("writeback inode awaits Forget after content refusal: %w", filer_pb.ErrPreconditionFailed)
	}
	// This guard shares the CAS version with CreateEntry. A rewritten or
	// manifest bounding range cannot conceal the original kernel WRITE frontier.
	var eof uint64
	if base != nil {
		eof = filer.FileSize(base)
	}
	if !wfs.option.WritebackCache && fh.pendingAppend && fh.pendingAppendOffset > eof {
		return fmt.Errorf("append offset %d exceeds committed EOF %d: %w", fh.pendingAppendOffset, eof, filer_pb.ErrPreconditionFailed)
	}
	return nil
}

// Only used when a peer shrank the file: a merge must not acknowledge a gap
// between that stored EOF and local pending content. Ordinary sparse pwrite
// without a concurrent truncate is unaffected. Compare copies, never reorder
// a handle's chunks. A manifest's bounding range is not proof of coverage.
func contentGapAfter(entry *filer_pb.Entry, start uint64) bool {
	end := filer.FileSize(entry)
	if start >= end {
		return false
	}
	chunks := append([]*filer_pb.FileChunk(nil), entry.Chunks...)
	sort.SliceStable(chunks, func(i, j int) bool { return chunks[i].Offset < chunks[j].Offset })
	covered := start
	if uint64(len(entry.Content)) > covered {
		covered = uint64(len(entry.Content))
	}
	for _, c := range chunks {
		if covered >= end {
			return false
		}
		if c.Offset < 0 || c.Size > ^uint64(0)-uint64(c.Offset) {
			return true
		}
		left, right := uint64(c.Offset), uint64(c.Offset)+c.Size
		if right <= covered {
			continue
		}
		if left > covered || c.IsChunkManifest {
			return true
		}
		covered = right
	}
	return covered < end
}
