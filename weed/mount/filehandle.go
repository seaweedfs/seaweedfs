package mount

import (
	"os"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
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
	dirtyMetadata     bool
	dirtyPages        *PageWriter
	reader            *filer.ChunkReadAt
	contentType       string
	asyncFlushPending bool   // set in writebackCache mode to defer flush to Release
	asyncFlushUid     uint32 // saved uid for deferred metadata flush
	asyncFlushGid     uint32 // saved gid for deferred metadata flush
	savedDir          string // last known parent path if inode-to-path state is forgotten
	savedName         string // last known file name if inode-to-path state is forgotten

	isDeleted bool
	isRenamed bool // set by Rename before waiting for async flush; skips old-path metadata flush

	// entryVersionTsNs is the filer log position the handle's entry reflects.
	// State at or below it must not replace the entry — that rolls it back.
	entryVersionTsNs atomic.Int64

	// adoptNextEventBase marks a committed mutation whose readback failed:
	// baseEntry is approximate and the mutation's own event is en route.
	adoptNextEventBase atomic.Bool

	// baseEntry snapshots the filer state last installed or acknowledged.
	// Local writes move the live entry away from it, so "is this event new"
	// must be judged here, not against the live entry. Always store a clone.
	baseEntry atomic.Pointer[filer_pb.Entry]

	// dlmLock holds the distributed lock for cross-mount write coordination.
	// Non-nil only when -dlm is enabled and the file was opened for writing.
	// Acquired in AcquireHandle, released in ReleaseHandle.
	dlmLock *cluster.LiveLock

	// remoteInstallMu serializes downloadRemoteEntry's install, which holds
	// only the handle's shared lock and so races a second concurrent read.
	remoteInstallMu sync.Mutex

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
		fh.baseEntry.Store(proto.Clone(entry).(*filer_pb.Entry))
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
	if fp, status := fh.wfs.inodeToPath.GetPath(fh.inode); status == fuse.OK {
		return fp
	}
	if fh.savedName != "" {
		return util.FullPath(fh.savedDir).Child(fh.savedName)
	}
	return ""
}

func (fh *FileHandle) RememberPath(fullPath util.FullPath) {
	if fullPath == "" {
		return
	}
	fh.savedDir, fh.savedName = fullPath.DirAndName()
}

func (fh *FileHandle) GetEntry() *LockedEntry {
	return fh.entry
}

func (fh *FileHandle) SetEntry(entry *filer_pb.Entry) {
	if entry != nil {
		fileSize := filer.FileSize(entry)
		entry.Attributes.FileSize = fileSize
		var resolveManifestErr error
		fh.entryChunkGroup, resolveManifestErr = filer.NewChunkGroup(fh.wfs.LookupFn(), fh.wfs.chunkCache, entry.Chunks, fh.wfs.option.ConcurrentReaders)
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

// installAckedEntry installs filer-acknowledged state under the handle lock
// when it outranks the handle. A version never advances without its value:
// stamping alone would fence out the events carrying what the handle lacks.
// Dirty handles are skipped — local writes supersede the ack.
func (fh *FileHandle) installAckedEntry(entry *filer_pb.Entry, versionTsNs int64) {
	fhActiveLock := fh.wfs.fhLockTable.AcquireLock("installAckedEntry", fh.fh, util.ExclusiveLock)
	defer fh.wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)
	if versionTsNs == 0 || versionTsNs <= fh.entryVersionTsNs.Load() ||
		fh.dirtyMetadata || entry == fh.GetEntry().GetEntry() {
		return
	}
	fh.SetEntry(entry)
	fh.setAuthoritativeBase(proto.Clone(entry).(*filer_pb.Entry))
	fh.advanceEntryVersionTsNs(versionTsNs)
}

// setAuthoritativeBase installs a base from a local ack and cancels any
// pending adoption: the ack supersedes the mutation it was waiting for, and a
// surviving flag would misfire on a later foreign event.
func (fh *FileHandle) setAuthoritativeBase(base *filer_pb.Entry) {
	fh.baseEntry.Store(base)
	fh.adoptNextEventBase.Store(false)
}

// advanceEntryVersionTsNs raises the entry version, never regresses it. Zero
// (an unversioned old filer) is a no-op, leaving the handle open to refreshes.
func (fh *FileHandle) advanceEntryVersionTsNs(tsNs int64) {
	if tsNs == 0 {
		return
	}
	for {
		current := fh.entryVersionTsNs.Load()
		if tsNs <= current || fh.entryVersionTsNs.CompareAndSwap(current, tsNs) {
			return
		}
	}
}

func (fh *FileHandle) ResetDirtyPages() {
	fh.dirtyPages.Destroy()
	fh.dirtyPages = newPageWriter(fh, fh.wfs.option.ChunkSizeLimit)
	fh.dirtyMetadata = false
	fh.contentType = ""
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
	// Release distributed lock before cleaning up, so other mounts can
	// proceed as soon as this handle is done flushing.
	if fh.dlmLock != nil {
		fh.dlmLock.Stop()
		fh.dlmLock = nil
		glog.V(1).Infof("DLM lock released for inode %d", fh.inode)
	}

	fhActiveLock := fh.wfs.fhLockTable.AcquireLock("ReleaseHandle", fh.fh, util.ExclusiveLock)
	defer fh.wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)

	fh.dirtyPages.Destroy()
	if IsDebugFileReadWrite {
		fh.mirrorFile.Close()
	}
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
