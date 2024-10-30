package filer

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

const SectionSize = 2 * 1024 * 1024 * 32 // 64MiB
type SectionIndex int64
type FileChunkSection struct {
	sectionIndex     SectionIndex
	chunks           []*filer_pb.FileChunk
	visibleIntervals *IntervalList[*VisibleInterval]
	chunkViews       *IntervalList[*ChunkView]
	reader           *ChunkReadAt
	lock             sync.RWMutex
	isPrepared       bool
}

func NewFileChunkSection(si SectionIndex) *FileChunkSection {
	return &FileChunkSection{
		sectionIndex: si,
	}
}

func (section *FileChunkSection) addChunk(chunk *filer_pb.FileChunk) error {
	section.lock.Lock()
	defer section.lock.Unlock()

	start, stop := max(int64(section.sectionIndex)*SectionSize, chunk.Offset), min(((int64(section.sectionIndex)+1)*SectionSize), chunk.Offset+int64(chunk.Size))

	section.chunks = append(section.chunks, chunk)

	if section.visibleIntervals == nil {
		section.visibleIntervals = readResolvedChunks(section.chunks, int64(section.sectionIndex)*SectionSize, (int64(section.sectionIndex)+1)*SectionSize)
	} else {
		MergeIntoVisibles(section.visibleIntervals, start, stop, chunk)
		garbageFileIds := FindGarbageChunks(section.visibleIntervals, start, stop)
		removeGarbageChunks(section, garbageFileIds)
	}

	if section.chunkViews != nil {
		MergeIntoChunkViews(section.chunkViews, start, stop, chunk)
	}

	return nil
}

func removeGarbageChunks(section *FileChunkSection, garbageFileIds map[string]struct{}) {
	for i := 0; i < len(section.chunks); {
		t := section.chunks[i]
		length := len(section.chunks)
		if _, found := garbageFileIds[t.FileId]; found {
			if i < length-1 {
				section.chunks[i] = section.chunks[length-1]
			}
			section.chunks = section.chunks[:length-1]
		} else {
			i++
		}
	}
}

func (section *FileChunkSection) setupForRead(group *ChunkGroup, fileSize int64) {
	section.lock.Lock()
	defer section.lock.Unlock()

	if section.isPrepared {
		section.reader.fileSize = fileSize
		return
	}

	if section.visibleIntervals == nil {
		section.visibleIntervals = readResolvedChunks(section.chunks, int64(section.sectionIndex)*SectionSize, (int64(section.sectionIndex)+1)*SectionSize)
		section.chunks, _ = SeparateGarbageChunks(section.visibleIntervals, section.chunks)
		if section.reader != nil {
			_ = section.reader.Close()
			section.reader = nil
		}
	}
	if section.chunkViews == nil {
		section.chunkViews = ViewFromVisibleIntervals(section.visibleIntervals, int64(section.sectionIndex)*SectionSize, (int64(section.sectionIndex)+1)*SectionSize)
	}

	if section.reader == nil {
		section.reader = NewChunkReaderAtFromClient(group.readerCache, section.chunkViews, min(int64(section.sectionIndex+1)*SectionSize, fileSize))
	}

	section.isPrepared = true
	section.reader.fileSize = fileSize
}

func (section *FileChunkSection) readDataAt(group *ChunkGroup, fileSize int64, buff []byte, offset int64) (n int, tsNs int64, err error) {

	section.setupForRead(group, fileSize)
	section.lock.RLock()
	defer section.lock.RUnlock()

	return section.reader.ReadAtWithTime(buff, offset)
}

func (section *FileChunkSection) DataStartOffset(group *ChunkGroup, offset int64, fileSize int64) int64 {

	section.setupForRead(group, fileSize)
	section.lock.RLock()
	defer section.lock.RUnlock()

	for x := section.visibleIntervals.Front(); x != nil; x = x.Next {
		visible := x.Value
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

	section.setupForRead(group, fileSize)
	section.lock.RLock()
	defer section.lock.RUnlock()

	isAfterOffset := false
	for x := section.visibleIntervals.Front(); x != nil; x = x.Next {
		visible := x.Value
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
