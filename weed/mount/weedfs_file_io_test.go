package mount

import (
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func TestOpenKeepCache_FirstOpen(t *testing.T) {
	// First open of a file should NOT set FOPEN_KEEP_CACHE because there
	// is no previously cached mtime to compare against.
	wfs := &WFS{}

	var out fuse.OpenOut
	inode := uint64(42)
	currentMtime := int64(1000)

	fh := &FileHandle{
		entry: &LockedEntry{
			Entry: &filer_pb.Entry{
				Attributes: &filer_pb.FuseAttributes{Mtime: currentMtime},
			},
		},
	}

	applyKeepCache(wfs, inode, 0, fh, &out)

	if out.OpenFlags&fuse.FOPEN_KEEP_CACHE != 0 {
		t.Error("first open should not set FOPEN_KEEP_CACHE")
	}
}

func TestOpenKeepCache_SecondOpenSameMtime(t *testing.T) {
	// Second open with an unchanged mtime SHOULD set FOPEN_KEEP_CACHE.
	wfs := &WFS{}

	inode := uint64(42)
	currentMtime := int64(1000)

	fh := &FileHandle{
		entry: &LockedEntry{
			Entry: &filer_pb.Entry{
				Attributes: &filer_pb.FuseAttributes{Mtime: currentMtime},
			},
		},
	}

	// First open -- populate cache.
	var out1 fuse.OpenOut
	applyKeepCache(wfs, inode, 0, fh, &out1)

	// Second open -- mtime unchanged.
	var out2 fuse.OpenOut
	applyKeepCache(wfs, inode, 0, fh, &out2)

	if out2.OpenFlags&fuse.FOPEN_KEEP_CACHE == 0 {
		t.Error("second open with same mtime should set FOPEN_KEEP_CACHE")
	}
}

func TestOpenKeepCache_MtimeChanged(t *testing.T) {
	// If the file's mtime changes between opens, FOPEN_KEEP_CACHE must NOT
	// be set so the kernel invalidates its page cache.
	wfs := &WFS{}

	inode := uint64(42)

	fh1 := &FileHandle{
		entry: &LockedEntry{
			Entry: &filer_pb.Entry{
				Attributes: &filer_pb.FuseAttributes{Mtime: 1000},
			},
		},
	}

	// First open.
	var out1 fuse.OpenOut
	applyKeepCache(wfs, inode, 0, fh1, &out1)

	// File is modified externally -- mtime changes.
	fh2 := &FileHandle{
		entry: &LockedEntry{
			Entry: &filer_pb.Entry{
				Attributes: &filer_pb.FuseAttributes{Mtime: 2000},
			},
		},
	}

	var out2 fuse.OpenOut
	applyKeepCache(wfs, inode, 0, fh2, &out2)

	if out2.OpenFlags&fuse.FOPEN_KEEP_CACHE != 0 {
		t.Error("open after mtime change should not set FOPEN_KEEP_CACHE")
	}
}

func TestOpenKeepCache_WriteInvalidation(t *testing.T) {
	// After a write invalidates the mtime cache, the next open should NOT
	// set FOPEN_KEEP_CACHE.
	wfs := &WFS{}

	inode := uint64(42)
	currentMtime := int64(1000)

	fh := &FileHandle{
		entry: &LockedEntry{
			Entry: &filer_pb.Entry{
				Attributes: &filer_pb.FuseAttributes{Mtime: currentMtime},
			},
		},
	}

	// First open -- populate cache.
	var out1 fuse.OpenOut
	applyKeepCache(wfs, inode, 0, fh, &out1)

	// Simulate write: delete the cached mtime.
	wfs.openMtimeCache.Delete(inode)

	// Next open -- cache was invalidated.
	var out2 fuse.OpenOut
	applyKeepCache(wfs, inode, 0, fh, &out2)

	if out2.OpenFlags&fuse.FOPEN_KEEP_CACHE != 0 {
		t.Error("open after write invalidation should not set FOPEN_KEEP_CACHE")
	}
}

func TestOpenKeepCache_WriteOpenSkipped(t *testing.T) {
	// Write-mode opens should never set FOPEN_KEEP_CACHE, even if the
	// mtime is cached.
	wfs := &WFS{}

	inode := uint64(42)
	currentMtime := int64(1000)

	fh := &FileHandle{
		entry: &LockedEntry{
			Entry: &filer_pb.Entry{
				Attributes: &filer_pb.FuseAttributes{Mtime: currentMtime},
			},
		},
	}

	// First read-only open.
	var out1 fuse.OpenOut
	applyKeepCache(wfs, inode, 0, fh, &out1)

	// Second open with write flag.
	var out2 fuse.OpenOut
	applyKeepCache(wfs, inode, fuse.O_ANYWRITE, fh, &out2)

	if out2.OpenFlags&fuse.FOPEN_KEEP_CACHE != 0 {
		t.Error("write open should not set FOPEN_KEEP_CACHE")
	}
}

// applyKeepCache mirrors the FOPEN_KEEP_CACHE logic from WFS.Open so it
// can be tested without a full FUSE mount.
func applyKeepCache(wfs *WFS, inode uint64, flags uint32, fh *FileHandle, out *fuse.OpenOut) {
	if flags&fuse.O_ANYWRITE == 0 {
		if entry := fh.GetEntry(); entry != nil && entry.Attributes != nil {
			currentMtime := entry.Attributes.Mtime
			if prev, loaded := wfs.openMtimeCache.Load(inode); loaded {
				if prev.(int64) == currentMtime {
					out.OpenFlags |= fuse.FOPEN_KEEP_CACHE
				}
			}
			wfs.openMtimeCache.Store(inode, currentMtime)
		}
	}
}
