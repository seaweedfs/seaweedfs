package mount

import (
	"context"
	"sync"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type DirectoryHandleId uint64

const (
	directoryStreamBaseOffset = 2 // . & ..
	batchSize                 = 1000
)

// readdirContext marks the meta cache listing as reading attributes only. A
// readdir never looks at a chunk list, and building one per child is most of
// the cost of decoding a wide directory.
var readdirContext = filer_pb.WithChunksOmitted(context.Background())

// DirectoryHandle represents an open directory handle.
// It maintains state for directory listing pagination and is protected by a mutex
// to handle concurrent readdir operations from NFS-Ganesha and other multi-threaded clients.
type DirectoryHandle struct {
	sync.Mutex
	isFinished        bool
	entryStream       []*filer.Entry
	entryStreamOffset uint64
	// lastListedName is how far the store itself reached, which runs ahead of
	// the last visible entry whenever children are dropped as expired.
	lastListedName string
	snapshotTsNs   int64 // snapshot timestamp for consistent readdir in direct mode
}

func (dh *DirectoryHandle) reset() {
	dh.isFinished = false
	dh.lastListedName = ""
	dh.snapshotTsNs = 0
	// Nil out pointers to allow garbage collection of old entries,
	// then reuse the slice's capacity to avoid re-allocations.
	for i := range dh.entryStream {
		dh.entryStream[i] = nil
	}
	dh.entryStream = dh.entryStream[:0]
	dh.entryStreamOffset = directoryStreamBaseOffset
}

type DirectoryHandleToInode struct {
	sync.Mutex
	dir2inode map[DirectoryHandleId]*DirectoryHandle
}

func NewDirectoryHandleToInode() *DirectoryHandleToInode {
	return &DirectoryHandleToInode{
		dir2inode: make(map[DirectoryHandleId]*DirectoryHandle),
	}
}

func (wfs *WFS) AcquireDirectoryHandle() (DirectoryHandleId, *DirectoryHandle) {
	fh := DirectoryHandleId(util.RandomUint64())

	wfs.dhMap.Lock()
	defer wfs.dhMap.Unlock()
	dh := &DirectoryHandle{}
	dh.reset()
	wfs.dhMap.dir2inode[fh] = dh
	return fh, dh
}

func (wfs *WFS) GetDirectoryHandle(dhid DirectoryHandleId) *DirectoryHandle {
	wfs.dhMap.Lock()
	defer wfs.dhMap.Unlock()
	if dh, found := wfs.dhMap.dir2inode[dhid]; found {
		return dh
	}
	dh := &DirectoryHandle{}
	dh.reset()
	wfs.dhMap.dir2inode[dhid] = dh
	return dh
}

func (wfs *WFS) ReleaseDirectoryHandle(dhid DirectoryHandleId) {
	wfs.dhMap.Lock()
	defer wfs.dhMap.Unlock()
	delete(wfs.dhMap.dir2inode, dhid)
}

// Directory handling

/** Open directory
 *
 * Unless the 'default_permissions' mount option is given,
 * this method should check if opendir is permitted for this
 * directory. Optionally opendir may also return an arbitrary
 * filehandle in the fuse_file_info structure, which will be
 * passed to readdir, releasedir and fsyncdir.
 */
func (wfs *WFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) (code fuse.Status) {
	if !wfs.inodeToPath.HasInode(input.NodeId) {
		return fuse.ENOENT
	}
	dhid, _ := wfs.AcquireDirectoryHandle()
	out.Fh = uint64(dhid)
	return fuse.OK
}

/** Release directory
 *
 * If the directory has been removed after the call to opendir, the
 * path parameter will be NULL.
 */
func (wfs *WFS) ReleaseDir(input *fuse.ReleaseIn) {
	wfs.ReleaseDirectoryHandle(DirectoryHandleId(input.Fh))
}

/** Synchronize directory contents
 *
 * If the directory has been removed after the call to opendir, the
 * path parameter will be NULL.
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data
 */
func (wfs *WFS) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) (code fuse.Status) {
	return fuse.OK
}

/** Read directory
 *
 * The filesystem may choose between two modes of operation:
 *
 * 1) The readdir implementation ignores the offset parameter, and
 * passes zero to the filler function's offset.  The filler
 * function will not return '1' (unless an error happens), so the
 * whole directory is read in a single readdir operation.
 *
 * 2) The readdir implementation keeps track of the offsets of the
 * directory entries.  It uses the offset parameter and always
 * passes non-zero offset to the filler function.  When the buffer
 * is full (or an error happens) the filler function will return
 * '1'.
 */
func (wfs *WFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) (code fuse.Status) {
	return wfs.doReadDirectory(input, fuseDirEntryList{out}, false)
}

func (wfs *WFS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) (code fuse.Status) {
	return wfs.doReadDirectory(input, fuseDirEntryList{out}, true)
}

func (wfs *WFS) doReadDirectory(input *fuse.ReadIn, out DirEntrySink, isPlusMode bool) fuse.Status {
	// Get the directory handle and lock it for the duration of this operation.
	// This serializes concurrent readdir calls on the same handle, fixing the
	// race condition that caused hangs with NFS-Ganesha.
	dh := wfs.GetDirectoryHandle(DirectoryHandleId(input.Fh))
	dh.Lock()
	defer dh.Unlock()

	if input.Offset == 0 {
		dh.reset()
	} else if input.Offset < dh.entryStreamOffset {
		// Seeking back before what the handle still holds. Start the directory
		// again rather than reporting nothing; the preload below refills up to
		// the requested offset.
		dh.reset()
	} else if dh.isFinished && input.Offset >= dh.entryStreamOffset {
		entryCurrentIndex := input.Offset - dh.entryStreamOffset
		if uint64(len(dh.entryStream)) <= entryCurrentIndex {
			return fuse.OK
		}
	}

	dirPath, code := wfs.inodeToPath.GetPath(input.NodeId)
	if code != fuse.OK {
		return code
	}
	wfs.inodeToPath.TouchDirectory(dirPath)

	var dirEntry fuse.DirEntry
	// Only a reference makes a child worth entering in the inode table: without
	// one nothing ever arrives to take the entry back out again.
	takesLookupRef := isPlusMode && out.TakesLookupRef()

	// index is the position in entryStream, used to calculate the offset for next readdir
	processEachEntryFn := func(entry *filer.Entry, index int64) bool {
		dirEntry.Name = entry.Name()
		dirEntry.Mode = toSyscallMode(entry.Mode)
		childPath := dirPath.Child(dirEntry.Name)
		var inode uint64
		if takesLookupRef {
			inode = wfs.inodeToPath.Lookup(childPath, entry.Crtime.Unix(), entry.IsDirectory(), len(entry.HardLinkId) > 0, entry.Inode, false)
		} else {
			inode = wfs.inodeToPath.InodeForListing(childPath, entry.Crtime.Unix(), entry.Inode)
		}
		dirEntry.Ino = inode

		// Set Off to the next offset so client can resume from correct position
		dirEntry.Off = dh.entryStreamOffset + uint64(index) + 1

		if !isPlusMode {
			if !out.AddEntry(dirEntry) {
				return false
			}
		} else {
			entryOut := out.AddEntryPlus(dirEntry)
			if entryOut == nil {
				return false
			}
			if fh, found := wfs.fhMap.FindFileHandle(inode); found {
				glog.V(4).Infof("readdir opened file %s", childPath)
				entry = filer.FromPbEntry(string(dirPath), fh.GetEntry().GetEntry())
			}
			wfs.outputFilerEntry(entryOut, inode, entry)
			// Taken only once the entry is really in the sink, so one that did not
			// fit leaves no reference behind. The fallback covers a racing Forget.
			if takesLookupRef && !wfs.inodeToPath.IncrementNlookup(inode) {
				wfs.inodeToPath.Lookup(childPath, entry.Crtime.Unix(), entry.IsDirectory(), len(entry.HardLinkId) > 0, entry.Inode, true)
			}
		}
		return true
	}

	if input.Offset < directoryStreamBaseOffset {
		if !isPlusMode {
			if input.Offset == 0 {
				out.AddEntry(fuse.DirEntry{Mode: fuse.S_IFDIR, Name: ".", Off: 1})
			}
			out.AddEntry(fuse.DirEntry{Mode: fuse.S_IFDIR, Name: "..", Off: 2})
		} else {
			if input.Offset == 0 {
				out.AddEntryPlus(fuse.DirEntry{Mode: fuse.S_IFDIR, Name: ".", Off: 1})
			}
			out.AddEntryPlus(fuse.DirEntry{Mode: fuse.S_IFDIR, Name: "..", Off: 2})
		}
		input.Offset = directoryStreamBaseOffset
	}

	var lastEntryName string

	if wfs.inodeToPath.ShouldReadDirectoryDirect(dirPath) {
		return wfs.readDirectoryDirect(input, out, dh, dirPath, processEachEntryFn)
	}

	// Read from cache first, then load next batch if needed
	if input.Offset >= dh.entryStreamOffset {
		// Drop what the client has walked past. Offsets are indexes into the
		// stream from entryStreamOffset, so advancing the two together keeps
		// them lined up; one entry is kept back because the next batch resumes
		// from the name immediately before the offset.
		if trim := int(input.Offset-dh.entryStreamOffset) - 1; trim > 0 && trim <= len(dh.entryStream) {
			copy(dh.entryStream, dh.entryStream[trim:])
			for i := len(dh.entryStream) - trim; i < len(dh.entryStream); i++ {
				dh.entryStream[i] = nil
			}
			dh.entryStream = dh.entryStream[:len(dh.entryStream)-trim]
			dh.entryStreamOffset += uint64(trim)
		}

		// Handle case: new handle with non-zero offset but empty cache
		// This happens when NFS-Ganesha opens multiple directory handles
		if len(dh.entryStream) == 0 && input.Offset > dh.entryStreamOffset {
			skipCount := int64(input.Offset - dh.entryStreamOffset)

			if err := meta_cache.EnsureVisited(wfs.metaCache, wfs, dirPath); err != nil {
				glog.Errorf("dir ReadDirAll %s: %v", dirPath, err)
				return fuse.EIO
			}

			// Load entries from beginning to fill cache up to the requested offset
			storeLastName, loadErr := wfs.metaCache.ListDirectoryEntries(readdirContext, dirPath, "", false, skipCount+int64(batchSize), func(entry *filer.Entry) (bool, error) {
				dh.entryStream = append(dh.entryStream, entry)
				return true, nil
			})
			dh.lastListedName = storeLastName
			if loadErr != nil {
				glog.Errorf("list meta cache: %v", loadErr)
				return fuse.EIO
			}
		}

		if input.Offset > dh.entryStreamOffset {
			entryPreviousIndex := (input.Offset - dh.entryStreamOffset) - 1
			if uint64(len(dh.entryStream)) > entryPreviousIndex {
				lastEntryName = dh.entryStream[entryPreviousIndex].Name()
			} else {
				// The stream runs from the directory's first child, so failing to
				// reach the entry before this offset means the directory has since
				// shrunk past it. Listing on from an empty name would replay the
				// directory from the start and hand the client every name twice.
				dh.isFinished = true
				return fuse.OK
			}
		}

		entryCurrentIndex := int64(input.Offset - dh.entryStreamOffset)
		for int64(len(dh.entryStream)) > entryCurrentIndex {
			entry := dh.entryStream[entryCurrentIndex]
			if processEachEntryFn(entry, entryCurrentIndex) {
				lastEntryName = entry.Name()
				entryCurrentIndex++
			} else {
				return fuse.OK
			}
		}

		// Cache exhausted, load next batch
		if err := meta_cache.EnsureVisited(wfs.metaCache, wfs, dirPath); err != nil {
			glog.Errorf("dir ReadDirAll %s: %v", dirPath, err)
			return fuse.EIO
		}

		// Page from where the store itself reached, not from the last entry the
		// sink saw. An expired child is counted against the batch and then
		// dropped, so resuming from the last visible name would re-read it every
		// round and never get past a batch that was entirely expired.
		if dh.lastListedName > lastEntryName {
			lastEntryName = dh.lastListedName
		}

		bufferFull := false
		storeLastName, loadErr := wfs.metaCache.ListDirectoryEntries(readdirContext, dirPath, lastEntryName, false, int64(batchSize), func(entry *filer.Entry) (bool, error) {
			currentIndex := int64(len(dh.entryStream))
			dh.entryStream = append(dh.entryStream, entry)
			if !processEachEntryFn(entry, currentIndex) {
				bufferFull = true
				return false, nil
			}
			return true, nil
		})
		if loadErr != nil {
			glog.Errorf("list meta cache: %v", loadErr)
			return fuse.EIO
		}
		dh.lastListedName = storeLastName

		// The store reaching nothing is the only sound end-of-directory signal:
		// a batch can come back short because entries expired, not because the
		// directory ran out.
		if !bufferFull && storeLastName == "" {
			dh.isFinished = true
		}
	}

	return fuse.OK
}

func (wfs *WFS) readDirectoryDirect(input *fuse.ReadIn, out DirEntrySink, dh *DirectoryHandle, dirPath util.FullPath, processEachEntryFn func(entry *filer.Entry, index int64) bool) fuse.Status {
	var lastEntryName string

	if input.Offset >= dh.entryStreamOffset {
		if len(dh.entryStream) == 0 && input.Offset > dh.entryStreamOffset {
			skipCount := uint32(input.Offset-dh.entryStreamOffset) + batchSize
			entries, snapshotTs, err := loadDirectoryEntriesDirect(readdirContext, wfs, wfs.option.UidGidMapper, dirPath, "", false, skipCount, dh.snapshotTsNs, wfs.option.IncludeSystemEntries)
			if err != nil {
				glog.Errorf("list filer directory: %v", err)
				return fuse.EIO
			}
			dh.entryStream = append(dh.entryStream, entries...)
			if dh.snapshotTsNs == 0 {
				dh.snapshotTsNs = snapshotTs
			}
		}

		if input.Offset > dh.entryStreamOffset {
			entryPreviousIndex := (input.Offset - dh.entryStreamOffset) - 1
			if uint64(len(dh.entryStream)) > entryPreviousIndex {
				lastEntryName = dh.entryStream[entryPreviousIndex].Name()
			} else {
				// See the cached path: the directory shrank past this offset, and
				// resuming from an empty name would replay it from the start.
				dh.isFinished = true
				return fuse.OK
			}
		}

		entryCurrentIndex := int64(input.Offset - dh.entryStreamOffset)
		for int64(len(dh.entryStream)) > entryCurrentIndex {
			entry := dh.entryStream[entryCurrentIndex]
			if processEachEntryFn(entry, entryCurrentIndex) {
				lastEntryName = entry.Name()
				entryCurrentIndex++
			} else {
				return fuse.OK
			}
		}

		entries, snapshotTs, err := loadDirectoryEntriesDirect(readdirContext, wfs, wfs.option.UidGidMapper, dirPath, lastEntryName, false, batchSize, dh.snapshotTsNs, wfs.option.IncludeSystemEntries)
		if err != nil {
			glog.Errorf("list filer directory: %v", err)
			return fuse.EIO
		}
		if dh.snapshotTsNs == 0 {
			dh.snapshotTsNs = snapshotTs
		}

		bufferFull := false
		for _, entry := range entries {
			currentIndex := int64(len(dh.entryStream))
			dh.entryStream = append(dh.entryStream, entry)
			if !processEachEntryFn(entry, currentIndex) {
				bufferFull = true
				break
			}
		}
		if !bufferFull && len(entries) < int(batchSize) {
			dh.isFinished = true
			// After a full successful read-through listing, exit direct mode
			// so subsequent reads can use the cache instead of hitting the filer.
			wfs.inodeToPath.MarkDirectoryRefreshed(dirPath, time.Now())
		}
	}

	return fuse.OK
}

func loadDirectoryEntriesDirect(ctx context.Context, client filer_pb.FilerClient, uidGidMapper *meta_cache.UidGidMapper, dirPath util.FullPath, startFileName string, includeStart bool, limit uint32, snapshotTsNs int64, includeSystemEntries bool) ([]*filer.Entry, int64, error) {
	// limit can be a client-supplied resume offset rather than a batch size, so
	// preallocating for it would size the slice from where the caller happened to
	// seek. Reserve a batch and let append find the rest.
	prealloc := limit
	if prealloc > batchSize {
		prealloc = batchSize
	}
	entries := make([]*filer.Entry, 0, prealloc)
	var actualSnapshotTsNs int64
	err := client.WithFilerClient(false, func(sc filer_pb.SeaweedFilerClient) error {
		var innerErr error
		actualSnapshotTsNs, innerErr = filer_pb.DoSeaweedListWithSnapshot(ctx, sc, dirPath, "", func(entry *filer_pb.Entry, isLast bool) error {
			if !includeSystemEntries && meta_cache.IsHiddenSystemEntry(string(dirPath), entry.Name) {
				return nil
			}
			if uidGidMapper != nil && entry.Attributes != nil {
				entry.Attributes.Uid, entry.Attributes.Gid = uidGidMapper.FilerToLocal(entry.Attributes.Uid, entry.Attributes.Gid)
			}
			entries = append(entries, filer.FromPbEntry(string(dirPath), entry))
			return nil
		}, startFileName, includeStart, limit, snapshotTsNs)
		return innerErr
	})
	if err != nil {
		return nil, actualSnapshotTsNs, err
	}
	return entries, actualSnapshotTsNs, nil
}
