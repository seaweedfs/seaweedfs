package mount

import (
	"bytes"
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
	fh      FileHandleId
	counter int64
	fhs     map[FileHandleId]bool // live fh values, one per open; guarded by fhMap's lock
	entry   *LockedEntry
	// Nested handle lock order: wfs.fhLockTable[fh.fh] -> entryLock ->
	// LockedEntry.RWMutex. Paths may use a subset, but must never acquire an
	// earlier lock while holding a later one. Release in reverse order.
	entryLock       sync.RWMutex
	entryChunkGroup *filer.ChunkGroup
	inode           uint64
	wfs             *WFS

	// cache file has been written to
	pendingAppend         bool
	pendingAppendOffset   uint64
	writebackContentError *atomic.Bool
	dirtyMetadata         bool
	dirtyContent          bool
	dirtyPages            *PageWriter
	reader                *filer.ChunkReadAt
	contentType           string
	asyncFlushPending     bool   // set in writebackCache mode to defer flush to Release
	asyncFlushUid         uint32 // saved uid for deferred metadata flush
	asyncFlushGid         uint32 // saved gid for deferred metadata flush
	savedDir              string // last known parent path if inode-to-path state is forgotten
	savedName             string // last known file name if inode-to-path state is forgotten

	isDeleted bool
	// deleteEpoch counts the times isDeleted was raised, all of them under the
	// handle's flush lock. A caller that raised it and then found it had
	// nothing to delete after all can tell its own mark from a later one.
	deleteEpoch uint64
	isRenamed   bool // set by Rename before waiting for async flush; skips old-path metadata flush
	// linkRefresh is the hard link id of a link change whose stored entry
	// could not be read; the next flush reads it before publishing.
	linkRefresh []byte

	// entryVersionTsNs is the filer log position the handle's entry reflects.
	// State at or below it must not replace the entry — that rolls it back.
	entryVersionTsNs atomic.Int64

	// entryVersionSignature identifies the filer whose clock stamped
	// entryVersionTsNs, when it came from an RPC fence. Positions from a
	// different filer are not comparable, so an event that filer did not log
	// is applied rather than fenced out. Zero when the version came from an
	// event, whose ordering the subscription already provides.
	entryVersionSignature atomic.Int32

	// baseEntry snapshots the filer state last installed or acknowledged.
	// Local writes move the live entry away from it, so "is this event new"
	// must be judged here, not against the live entry. Always store a clone.
	baseEntry atomic.Pointer[filer_pb.Entry]

	// sinceBase marks a truncate or chunk rewrite over baseEntry that a
	// merge must honor: later writes can grow the file past a truncate, and
	// rewritten chunks carry the handle's view of the base.
	sinceBase atomic.Pointer[baseMark]

	// chunkViewStale marks a read view whose manifest resolution failed; the
	// next read or lseek resolves the entry's chunks again.
	chunkViewStale atomic.Bool

	// dlmLock holds the distributed lock for cross-mount write coordination.
	// Non-nil only when -dlm is enabled and the file was opened for writing.
	// Acquired in AcquireHandle, released in ReleaseHandle.
	dlmLock *cluster.LiveLock

	// remoteInstallMu serializes downloadRemoteEntry's install, which holds
	// only the handle's shared lock and so races a second concurrent read.
	remoteInstallMu sync.Mutex

	// for debugging
	mirrorFile *os.File
}

func newFileHandle(wfs *WFS, handleId FileHandleId, inode uint64, entry *filer_pb.Entry) *FileHandle {
	fh := &FileHandle{
		fh:      handleId,
		counter: 1,
		fhs:     map[FileHandleId]bool{handleId: false},
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
		if fh.entryChunkGroup != nil {
			_ = fh.entryChunkGroup.Close()
		}
		var resolveManifestErr error
		fh.entryChunkGroup, resolveManifestErr = filer.NewChunkGroup(fh.wfs.LookupFn(), fh.wfs.chunkCache, entry.Chunks, fh.wfs.option.ConcurrentReaders, fh.wfs.CacheInvalidator(), fh.wfs.manifestCache, fh.wfs.readerCacheBudget)
		if resolveManifestErr != nil {
			glog.Warningf("failed to resolve manifest chunks in %+v", filer_pb.ScrubCipherKeys(entry))
		}
		fh.chunkViewStale.Store(resolveManifestErr != nil)
	} else {
		glog.Fatalf("setting file handle entry to nil")
	}
	fh.entry.SetEntry(entry)
}

// installAckedEntry installs filer-acknowledged state under the handle lock
// when it outranks the handle. A version never advances without its value:
// stamping alone would fence out the events carrying what the handle lacks.
// Dirty handles are skipped — local writes supersede the ack. Deleted ones
// too: their path may already name a different file, and no acknowledged
// state belongs to this handle anymore.
func (fh *FileHandle) installAckedEntry(entry *filer_pb.Entry, versionTsNs int64, signature int32) {
	fhActiveLock := fh.wfs.fhLockTable.AcquireLock("installAckedEntry", fh.fh, util.ExclusiveLock)
	defer fh.wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)
	if versionTsNs == 0 || fh.dirtyMetadata || fh.isDeleted || entry == fh.GetEntry().GetEntry() {
		return
	}
	// Refuse only what is provably older. Two known, differing filer
	// signatures mean the positions come from unrelated clocks and say
	// nothing about each other; dropping the acknowledgment there would leave
	// the handle holding the very state this mutation replaced. Unknown
	// signatures still compare, as they did before.
	handleSignature := fh.entryVersionSignature.Load()
	provablyOtherClock := signature != 0 && handleSignature != 0 && signature != handleSignature
	if !provablyOtherClock && versionTsNs <= fh.entryVersionTsNs.Load() {
		return
	}
	fh.SetEntry(entry)
	fh.setAuthoritativeBase(proto.Clone(entry).(*filer_pb.Entry))
	fh.advanceEntryVersion(versionTsNs, signature)
}

// rebaseEntry installs an entry merged with a peer's changes, keeping the
// chunks and growth writers added after sent was taken, and refreshes the
// read view.
func (fh *FileHandle) rebaseEntry(merged, sent *filer_pb.Entry) {
	inSent := make(map[string]bool, len(sent.Chunks))
	for _, c := range sent.Chunks {
		inSent[contentChunkKey(c)] = true
	}
	merged = proto.Clone(merged).(*filer_pb.Entry)
	fh.UpdateEntry(func(e *filer_pb.Entry) {
		chunks := merged.Chunks
		for _, c := range e.Chunks {
			if !inSent[contentChunkKey(c)] {
				chunks = append(chunks, c)
			}
		}
		attrs := merged.Attributes
		if size := e.GetAttributes().GetFileSize(); size > sent.GetAttributes().GetFileSize() && size > attrs.FileSize {
			attrs.FileSize = size
		}
		e.Attributes, e.Extended, e.Content, e.Chunks = attrs, merged.Extended, merged.Content, chunks
		err := fh.entryChunkGroup.SetChunks(chunks)
		if err != nil {
			glog.Warningf("rebase %s: resolve chunks: %v", fh.FullPath(), err)
		}
		fh.chunkViewStale.Store(err != nil)
	})
}

// refreshChunkView retries a read view whose manifest resolution failed.
func (fh *FileHandle) refreshChunkView() {
	if !fh.chunkViewStale.Load() {
		return
	}
	fh.UpdateEntry(func(e *filer_pb.Entry) {
		fh.chunkViewStale.Store(fh.entryChunkGroup.SetChunks(e.Chunks) != nil)
	})
}

// mergePeerContent folds a peer's change into a dirty handle, so its own
// writes stay on top of the peer's instead of being discarded.
func (fh *FileHandle) mergePeerContent(peer, base *filer_pb.Entry) bool {
	var local *filer_pb.Entry
	fh.UpdateEntry(func(e *filer_pb.Entry) {
		local = proto.Clone(e).(*filer_pb.Entry)
	})
	mark := fh.markSince(base)
	merged, ok := mergeContentEntry(peer, base, local, local, mark.truncatedTo, mark.rewrote)
	ok = ok && mergeKeepsBuffered(merged, local)
	if ok {
		fh.rebaseEntry(merged, local)
	}
	return ok
}

// mergeKeepsBuffered reports whether merged leaves room for writes still
// buffered over local. They are not chunks yet, so a merge that shrinks the
// file or brings in inline content (read ahead of chunks) waits for the flush.
func mergeKeepsBuffered(merged, local *filer_pb.Entry) bool {
	return merged.Attributes.FileSize >= filer.FileSize(local) && bytes.Equal(merged.Content, local.Content)
}

// baseMark records what the handle did over base that its entry does not
// show: the lowest size a truncate set (-1 for none), and a whole-file
// rewrite of its chunks.
type baseMark struct {
	base        *filer_pb.Entry
	truncatedTo int64
	rewrote     bool
}

func (fh *FileHandle) markBase(update func(m *baseMark)) {
	for {
		base, old := fh.baseEntry.Load(), fh.sinceBase.Load()
		next := baseMark{base: base, truncatedTo: -1}
		if old != nil && old.base == base {
			next = *old
		}
		update(&next)
		if fh.sinceBase.CompareAndSwap(old, &next) {
			return
		}
	}
}

// noteTruncate records a truncate to size over the current base.
func (fh *FileHandle) noteTruncate(size uint64) {
	fh.markBase(func(m *baseMark) {
		if m.truncatedTo < 0 || int64(size) < m.truncatedTo {
			m.truncatedTo = int64(size)
		}
	})
}

// noteRewrite records that the handle's chunks were rewritten from its view
// of the current base.
func (fh *FileHandle) noteRewrite() {
	fh.markBase(func(m *baseMark) { m.rewrote = true })
}

// adoptPeerBase moves a dirty handle's base to a peer's entry. What the
// handle has not published yet still applies over the new base.
func (fh *FileHandle) adoptPeerBase(base *filer_pb.Entry) {
	old := fh.baseEntry.Swap(base)
	if m := fh.sinceBase.Load(); m != nil && m.base == old {
		moved := *m
		moved.base = base
		fh.sinceBase.CompareAndSwap(m, &moved)
	}
}

// markSince is what the handle did over base.
func (fh *FileHandle) markSince(base *filer_pb.Entry) baseMark {
	if m := fh.sinceBase.Load(); m != nil && m.base == base {
		return *m
	}
	return baseMark{base: base, truncatedTo: -1}
}

// setAuthoritativeBase installs the base snapshot a local ack acknowledged.
func (fh *FileHandle) setAuthoritativeBase(base *filer_pb.Entry) {
	fh.baseEntry.Store(base)
}

// advanceEntryVersion raises the entry version, never regresses it, and
// records the clock domain the new position belongs to: a filer signature for
// an RPC fence, zero for an event. The signature travels with the timestamp so
// the two never disagree. A zero position (an unversioned old filer) is a
// no-op, leaving the handle open to refreshes.
func (fh *FileHandle) advanceEntryVersion(tsNs int64, signature int32) {
	if tsNs == 0 {
		return
	}
	for {
		current := fh.entryVersionTsNs.Load()
		if tsNs <= current {
			return
		}
		if fh.entryVersionTsNs.CompareAndSwap(current, tsNs) {
			fh.entryVersionSignature.Store(signature)
			return
		}
	}
}

func (fh *FileHandle) ResetDirtyPages() {
	fh.dirtyPages.Destroy()
	fh.dirtyPages = newPageWriter(fh, fh.wfs.option.ChunkSizeLimit)
	fh.dirtyMetadata = false
	fh.dirtyContent = false
	fh.pendingAppend = false
	fh.contentType = ""
}

func (fh *FileHandle) UpdateEntry(fn func(entry *filer_pb.Entry)) *filer_pb.Entry {
	result := fh.entry.UpdateEntry(fn)

	return result
}

func (fh *FileHandle) AddChunks(chunks []*filer_pb.FileChunk) {
	fh.entry.AppendChunks(chunks)
}

func (fh *FileHandle) ReleaseHandle() {
	fhActiveLock := fh.wfs.fhLockTable.AcquireLock("ReleaseHandle", fh.fh, util.ExclusiveLock)
	defer fh.wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)

	// Release distributed lock before cleaning up, so other mounts can
	// proceed as soon as this handle is done flushing.
	if fh.dlmLock != nil {
		fh.dlmLock.Stop()
		fh.dlmLock = nil
		glog.V(1).Infof("DLM lock released for inode %d", fh.inode)
	}

	if fh.entryChunkGroup != nil {
		_ = fh.entryChunkGroup.Close()
	}
	fh.dirtyPages.Destroy()
	if IsDebugFileReadWrite {
		fh.mirrorFile.Close()
	}
}
