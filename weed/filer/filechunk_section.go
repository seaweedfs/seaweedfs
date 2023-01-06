package filer

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"sync"
)

const SectionSize = 2 * 1024 * 1024 * 128 // 256MiB
type SectionIndex int64
type FileChunkSection struct {
	sectionIndex     SectionIndex
	chunks           []*filer_pb.FileChunk
	visibleIntervals *IntervalList[*VisibleInterval]
	chunkViews       *IntervalList[*ChunkView]
	reader           *ChunkReadAt
	lock             sync.Mutex
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

	if section.visibleIntervals != nil {
		MergeIntoVisibles(section.visibleIntervals, start, stop, chunk)
		garbageFileIds := FindGarbageChunks(section.visibleIntervals, start, stop)
		for _, garbageFileId := range garbageFileIds {
			length := len(section.chunks)
			for i, t := range section.chunks {
				if t.FileId == garbageFileId {
					section.chunks[i] = section.chunks[length-1]
					section.chunks = section.chunks[:length-1]
					break
				}
			}
		}
	}

	if section.chunkViews != nil {
		MergeIntoChunkViews(section.chunkViews, start, stop, chunk)
	}

	return nil
}

func (section *FileChunkSection) setupForRead(group *ChunkGroup, fileSize int64) {
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
		section.reader = NewChunkReaderAtFromClient(group.lookupFn, section.chunkViews, group.chunkCache, min(int64(section.sectionIndex+1)*SectionSize, fileSize))
	}
	section.reader.fileSize = fileSize
}

func (section *FileChunkSection) readDataAt(group *ChunkGroup, fileSize int64, buff []byte, offset int64) (n int, tsNs int64, err error) {
	section.lock.Lock()
	defer section.lock.Unlock()

	section.setupForRead(group, fileSize)

	return section.reader.ReadAtWithTime(buff, offset)
}

func (section *FileChunkSection) DataStartOffset(group *ChunkGroup, offset int64, fileSize int64) int64 {
	section.lock.Lock()
	defer section.lock.Unlock()

	section.setupForRead(group, fileSize)

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
	section.lock.Lock()
	defer section.lock.Unlock()

	section.setupForRead(group, fileSize)

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
