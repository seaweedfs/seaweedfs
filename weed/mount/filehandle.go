package mount

import (
	"os"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type FileHandleId uint64

var IsDebugFileReadWrite = false

type FileHandle struct {
	fh              FileHandleId
	counter         int64
	entry           *LockedEntry
	entryLock       sync.RWMutex
	entryChunkGroup *filer.ChunkGroup
	inode           uint64
	wfs             *WFS

	// cache file has been written to
	dirtyMetadata bool
	dirtyPages    *PageWriter
	reader        *filer.ChunkReadAt
	contentType   string

	isDeleted bool

	// RDMA chunk offset cache for performance optimization
	chunkOffsetCache []int64
	chunkCacheValid  bool
	chunkCacheLock   sync.RWMutex

	// for debugging
	mirrorFile *os.File
}

func newFileHandle(wfs *WFS, handleId FileHandleId, inode uint64, entry *filer_pb.Entry) *FileHandle {
	fh := &FileHandle{
		fh:      handleId,
		counter: 1,
		inode:   inode,
		wfs:     wfs,
	}
	// dirtyPages: newContinuousDirtyPages(file, writeOnly),
	fh.dirtyPages = newPageWriter(fh, wfs.option.ChunkSizeLimit)
	fh.entry = &LockedEntry{
		Entry: entry,
	}
	if entry != nil {
		fh.SetEntry(entry)
	}

	if IsDebugFileReadWrite {
		var err error
		fh.mirrorFile, err = os.OpenFile("/tmp/sw/"+entry.Name, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			println("failed to create mirror:", err.Error())
		}
	}

	return fh
}

func (fh *FileHandle) FullPath() util.FullPath {
	fp, _ := fh.wfs.inodeToPath.GetPath(fh.inode)
	return fp
}

func (fh *FileHandle) GetEntry() *LockedEntry {
	return fh.entry
}

func (fh *FileHandle) SetEntry(entry *filer_pb.Entry) {
	if entry != nil {
		fileSize := filer.FileSize(entry)
		entry.Attributes.FileSize = fileSize
		var resolveManifestErr error
		fh.entryChunkGroup, resolveManifestErr = filer.NewChunkGroup(fh.wfs.LookupFn(), fh.wfs.chunkCache, entry.Chunks)
		if resolveManifestErr != nil {
			glog.Warningf("failed to resolve manifest chunks in %+v", entry)
		}
	} else {
		glog.Fatalf("setting file handle entry to nil")
	}
	fh.entry.SetEntry(entry)
	
	// Invalidate chunk offset cache since chunks may have changed
	fh.invalidateChunkCache()
}

func (fh *FileHandle) UpdateEntry(fn func(entry *filer_pb.Entry)) *filer_pb.Entry {
	result := fh.entry.UpdateEntry(fn)
	
	// Invalidate chunk offset cache since entry may have been modified
	fh.invalidateChunkCache()
	
	return result
}

func (fh *FileHandle) AddChunks(chunks []*filer_pb.FileChunk) {
	fh.entry.AppendChunks(chunks)
	
	// Invalidate chunk offset cache since new chunks were added
	fh.invalidateChunkCache()
}

func (fh *FileHandle) ReleaseHandle() {

	fhActiveLock := fh.wfs.fhLockTable.AcquireLock("ReleaseHandle", fh.fh, util.ExclusiveLock)
	defer fh.wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)

	fh.dirtyPages.Destroy()
	if IsDebugFileReadWrite {
		fh.mirrorFile.Close()
	}
}

func lessThan(a, b *filer_pb.FileChunk) bool {
	if a.ModifiedTsNs == b.ModifiedTsNs {
		return a.Fid.FileKey < b.Fid.FileKey
	}
	return a.ModifiedTsNs < b.ModifiedTsNs
}

// getCumulativeOffsets returns cached cumulative offsets for chunks, computing them if necessary
func (fh *FileHandle) getCumulativeOffsets(chunks []*filer_pb.FileChunk) []int64 {
	fh.chunkCacheLock.RLock()
	if fh.chunkCacheValid && len(fh.chunkOffsetCache) == len(chunks)+1 {
		// Cache is valid and matches current chunk count
		result := make([]int64, len(fh.chunkOffsetCache))
		copy(result, fh.chunkOffsetCache)
		fh.chunkCacheLock.RUnlock()
		return result
	}
	fh.chunkCacheLock.RUnlock()

	// Need to compute/recompute cache
	fh.chunkCacheLock.Lock()
	defer fh.chunkCacheLock.Unlock()

	// Double-check in case another goroutine computed it while we waited for the lock
	if fh.chunkCacheValid && len(fh.chunkOffsetCache) == len(chunks)+1 {
		result := make([]int64, len(fh.chunkOffsetCache))
		copy(result, fh.chunkOffsetCache)
		return result
	}

	// Compute cumulative offsets
	cumulativeOffsets := make([]int64, len(chunks)+1)
	for i, chunk := range chunks {
		cumulativeOffsets[i+1] = cumulativeOffsets[i] + int64(chunk.Size)
	}

	// Cache the result
	fh.chunkOffsetCache = make([]int64, len(cumulativeOffsets))
	copy(fh.chunkOffsetCache, cumulativeOffsets)
	fh.chunkCacheValid = true

	return cumulativeOffsets
}

// invalidateChunkCache invalidates the chunk offset cache when chunks are modified
func (fh *FileHandle) invalidateChunkCache() {
	fh.chunkCacheLock.Lock()
	fh.chunkCacheValid = false
	fh.chunkOffsetCache = nil
	fh.chunkCacheLock.Unlock()
}
