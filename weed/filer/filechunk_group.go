package filer

import (
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

type ChunkGroup struct {
	lookupFn     wdclient.LookupFileIdFunctionType
	chunkCache   chunk_cache.ChunkCache
	sections     map[SectionIndex]*FileChunkSection
	sectionsLock sync.RWMutex
	readerCache  *ReaderCache
}

func NewChunkGroup(lookupFn wdclient.LookupFileIdFunctionType, chunkCache chunk_cache.ChunkCache, chunks []*filer_pb.FileChunk) (*ChunkGroup, error) {
	group := &ChunkGroup{
		lookupFn:    lookupFn,
		chunkCache:  chunkCache,
		sections:    make(map[SectionIndex]*FileChunkSection),
		readerCache: NewReaderCache(32, chunkCache, lookupFn),
	}

	err := group.SetChunks(chunks)
	return group, err
}

func (group *ChunkGroup) AddChunk(chunk *filer_pb.FileChunk) error {

	group.sectionsLock.Lock()
	defer group.sectionsLock.Unlock()

	sectionIndexStart, sectionIndexStop := SectionIndex(chunk.Offset/SectionSize), SectionIndex((chunk.Offset+int64(chunk.Size))/SectionSize)
	for si := sectionIndexStart; si < sectionIndexStop+1; si++ {
		section, found := group.sections[si]
		if !found {
			section = NewFileChunkSection(si)
			group.sections[si] = section
		}
		section.addChunk(chunk)
	}
	return nil
}

func (group *ChunkGroup) ReadDataAt(fileSize int64, buff []byte, offset int64) (n int, tsNs int64, err error) {

	group.sectionsLock.RLock()
	defer group.sectionsLock.RUnlock()

	sectionIndexStart, sectionIndexStop := SectionIndex(offset/SectionSize), SectionIndex((offset+int64(len(buff)))/SectionSize)
	for si := sectionIndexStart; si < sectionIndexStop+1; si++ {
		section, found := group.sections[si]
		rangeStart, rangeStop := max(offset, int64(si*SectionSize)), min(offset+int64(len(buff)), int64((si+1)*SectionSize))
		if !found {
			rangeStop = min(rangeStop, fileSize)
			for i := rangeStart; i < rangeStop; i++ {
				buff[i-offset] = 0
			}
			n = int(int64(n) + rangeStop - rangeStart)
			continue
		}
		xn, xTsNs, xErr := section.readDataAt(group, fileSize, buff[rangeStart-offset:rangeStop-offset], rangeStart)
		if xErr != nil {
			err = xErr
		}
		n += xn
		tsNs = max(tsNs, xTsNs)
	}
	return
}

func (group *ChunkGroup) SetChunks(chunks []*filer_pb.FileChunk) error {
	group.sectionsLock.RLock()
	defer group.sectionsLock.RUnlock()

	var dataChunks []*filer_pb.FileChunk
	for _, chunk := range chunks {

		if !chunk.IsChunkManifest {
			dataChunks = append(dataChunks, chunk)
			continue
		}

		resolvedChunks, err := ResolveOneChunkManifest(group.lookupFn, chunk)
		if err != nil {
			return err
		}

		dataChunks = append(dataChunks, resolvedChunks...)
	}

	sections := make(map[SectionIndex]*FileChunkSection)

	for _, chunk := range dataChunks {
		sectionIndexStart, sectionIndexStop := SectionIndex(chunk.Offset/SectionSize), SectionIndex((chunk.Offset+int64(chunk.Size))/SectionSize)
		for si := sectionIndexStart; si < sectionIndexStop+1; si++ {
			section, found := sections[si]
			if !found {
				section = NewFileChunkSection(si)
				sections[si] = section
			}
			section.chunks = append(section.chunks, chunk)
		}
	}

	group.sections = sections
	return nil
}

const (
	// see weedfs_file_lseek.go
	SEEK_DATA uint32 = 3 // seek to next data after the offset
	// SEEK_HOLE uint32 = 4 // seek to next hole after the offset
)

// FIXME: needa tests
func (group *ChunkGroup) SearchChunks(offset, fileSize int64, whence uint32) (found bool, out int64) {
	group.sectionsLock.RLock()
	defer group.sectionsLock.RUnlock()

	return group.doSearchChunks(offset, fileSize, whence)
}

func (group *ChunkGroup) doSearchChunks(offset, fileSize int64, whence uint32) (found bool, out int64) {

	sectionIndex, maxSectionIndex := SectionIndex(offset/SectionSize), SectionIndex(fileSize/SectionSize)
	if whence == SEEK_DATA {
		for si := sectionIndex; si < maxSectionIndex+1; si++ {
			section, foundSection := group.sections[si]
			if !foundSection {
				continue
			}
			sectionStart := section.DataStartOffset(group, offset, fileSize)
			if sectionStart == -1 {
				continue
			}
			return true, sectionStart
		}
		return false, 0
	} else {
		// whence == SEEK_HOLE
		for si := sectionIndex; si < maxSectionIndex; si++ {
			section, foundSection := group.sections[si]
			if !foundSection {
				return true, offset
			}
			holeStart := section.NextStopOffset(group, offset, fileSize)
			if holeStart%SectionSize == 0 {
				continue
			}
			return true, holeStart
		}
		return true, fileSize
	}
}
