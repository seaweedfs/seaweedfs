package filer

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"sync"
)

const SectionSize = 2 * 1024 * 1024 * 128 // 256MiB
type SectionIndex int64
type FileChunkSection struct {
	sectionIndex   SectionIndex
	chunks         []*filer_pb.FileChunk
	entryViewCache []VisibleInterval
	chunkViews     []*ChunkView
	reader         *ChunkReadAt
	lock           sync.Mutex
}

func (section *FileChunkSection) addChunk(chunk *filer_pb.FileChunk) error {
	section.lock.Lock()
	defer section.lock.Unlock()
	section.chunks = append(section.chunks, chunk)
	// FIXME: this can be improved to an incremental change
	section.entryViewCache = nil
	return nil
}

func (section *FileChunkSection) readDataAt(group *ChunkGroup, fileSize int64, buff []byte, offset int64) (n int, tsNs int64, err error) {
	section.lock.Lock()
	defer section.lock.Unlock()

	section.setupForRead(group, fileSize)

	return section.reader.ReadAtWithTime(buff, offset)
}

func (section *FileChunkSection) setupForRead(group *ChunkGroup, fileSize int64) {
	if section.entryViewCache == nil {
		section.entryViewCache = readResolvedChunks(section.chunks)
		section.chunks, _ = SeparateGarbageChunks(section.entryViewCache, section.chunks)
		if section.reader != nil {
			_ = section.reader.Close()
			section.reader = nil
		}
	}

	if section.reader == nil {
		chunkViews := ViewFromVisibleIntervals(section.entryViewCache, int64(section.sectionIndex)*SectionSize, (int64(section.sectionIndex)+1)*SectionSize)
		section.reader = NewChunkReaderAtFromClient(group.lookupFn, chunkViews, group.chunkCache, min(int64(section.sectionIndex+1)*SectionSize, fileSize))
	}
}

func (section *FileChunkSection) DataStartOffset(group *ChunkGroup, offset int64, fileSize int64) int64 {
	section.lock.Lock()
	defer section.lock.Unlock()

	section.setupForRead(group, fileSize)

	for _, visible := range section.entryViewCache {
		if visible.stop <= offset {
			continue
		}
		if offset < visible.start {
			return offset
		}
		return offset
	}
	return -1
}

func (section *FileChunkSection) NextStopOffset(group *ChunkGroup, offset int64, fileSize int64) int64 {
	section.lock.Lock()
	defer section.lock.Unlock()

	section.setupForRead(group, fileSize)

	isAfterOffset := false
	for _, visible := range section.entryViewCache {
		if !isAfterOffset {
			if visible.stop <= offset {
				continue
			}
			isAfterOffset = true
		}
		if offset < visible.start {
			return offset
		}
		// now visible.start <= offset
		if offset < visible.stop {
			offset = visible.stop
		}
	}
	return offset
}