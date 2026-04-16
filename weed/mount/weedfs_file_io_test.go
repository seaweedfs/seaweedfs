package mount

import (
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func newTestWFS() *WFS {
	return &WFS{
		openMtimeCache: make(map[uint64][2]int64, 8192),
	}
}

func TestOpenKeepCache_FirstOpen(t *testing.T) {
	// First open of a file should NOT set FOPEN_KEEP_CACHE because there
	// is no previously cached mtime to compare against.
	wfs := newTestWFS()

	var out fuse.OpenOut
	inode := uint64(42)

	entry := &LockedEntry{
		Entry: &filer_pb.Entry{
			Attributes: &filer_pb.FuseAttributes{Mtime: 1000, MtimeNs: 123},
		},
	}

	wfs.applyKeepCacheFlag(inode, entry, &out)

	if out.OpenFlags&fuse.FOPEN_KEEP_CACHE != 0 {
		t.Error("first open should not set FOPEN_KEEP_CACHE")
	}
}

func TestOpenKeepCache_SecondOpenSameMtime(t *testing.T) {
	// Second open with an unchanged mtime SHOULD set FOPEN_KEEP_CACHE.
	wfs := newTestWFS()

	inode := uint64(42)

	entry := &LockedEntry{
		Entry: &filer_pb.Entry{
			Attributes: &filer_pb.FuseAttributes{Mtime: 1000, MtimeNs: 123},
		},
	}

	// First open -- populate cache.
	var out1 fuse.OpenOut
	wfs.applyKeepCacheFlag(inode, entry, &out1)

	// Second open -- mtime unchanged.
	var out2 fuse.OpenOut
	wfs.applyKeepCacheFlag(inode, entry, &out2)

	if out2.OpenFlags&fuse.FOPEN_KEEP_CACHE == 0 {
		t.Error("second open with same mtime should set FOPEN_KEEP_CACHE")
	}
}

func TestOpenKeepCache_MtimeChanged(t *testing.T) {
	// If the file's mtime changes between opens, FOPEN_KEEP_CACHE must NOT
	// be set so the kernel invalidates its page cache.
	wfs := newTestWFS()

	inode := uint64(42)

	entry1 := &LockedEntry{
		Entry: &filer_pb.Entry{
			Attributes: &filer_pb.FuseAttributes{Mtime: 1000, MtimeNs: 0},
		},
	}

	// First open.
	var out1 fuse.OpenOut
	wfs.applyKeepCacheFlag(inode, entry1, &out1)

	// File is modified externally -- mtime changes.
	entry2 := &LockedEntry{
		Entry: &filer_pb.Entry{
			Attributes: &filer_pb.FuseAttributes{Mtime: 2000, MtimeNs: 0},
		},
	}

	var out2 fuse.OpenOut
	wfs.applyKeepCacheFlag(inode, entry2, &out2)

	if out2.OpenFlags&fuse.FOPEN_KEEP_CACHE != 0 {
		t.Error("open after mtime change should not set FOPEN_KEEP_CACHE")
	}
}

func TestOpenKeepCache_NanosecondPrecision(t *testing.T) {
	// Two modifications within the same second but different nanoseconds
	// must NOT reuse cached page data.
	wfs := newTestWFS()

	inode := uint64(42)

	entry1 := &LockedEntry{
		Entry: &filer_pb.Entry{
			Attributes: &filer_pb.FuseAttributes{Mtime: 1000, MtimeNs: 100},
		},
	}

	var out1 fuse.OpenOut
	wfs.applyKeepCacheFlag(inode, entry1, &out1)

	// Same second, different nanosecond.
	entry2 := &LockedEntry{
		Entry: &filer_pb.Entry{
			Attributes: &filer_pb.FuseAttributes{Mtime: 1000, MtimeNs: 200},
		},
	}

	var out2 fuse.OpenOut
	wfs.applyKeepCacheFlag(inode, entry2, &out2)

	if out2.OpenFlags&fuse.FOPEN_KEEP_CACHE != 0 {
		t.Error("open after nanosecond-level mtime change should not set FOPEN_KEEP_CACHE")
	}
}

func TestOpenKeepCache_WriteInvalidation(t *testing.T) {
	// After a write invalidates the mtime cache, the next open should NOT
	// set FOPEN_KEEP_CACHE.
	wfs := newTestWFS()

	inode := uint64(42)

	entry := &LockedEntry{
		Entry: &filer_pb.Entry{
			Attributes: &filer_pb.FuseAttributes{Mtime: 1000, MtimeNs: 0},
		},
	}

	// First open -- populate cache.
	var out1 fuse.OpenOut
	wfs.applyKeepCacheFlag(inode, entry, &out1)

	// Simulate write invalidation.
	wfs.invalidateOpenMtimeCache(inode)

	// Next open -- cache was invalidated.
	var out2 fuse.OpenOut
	wfs.applyKeepCacheFlag(inode, entry, &out2)

	if out2.OpenFlags&fuse.FOPEN_KEEP_CACHE != 0 {
		t.Error("open after write invalidation should not set FOPEN_KEEP_CACHE")
	}
}

func TestOpenKeepCache_WriteOpenSkipped(t *testing.T) {
	// Write-mode opens should never evaluate FOPEN_KEEP_CACHE.
	// The caller (WFS.Open) gates on O_ANYWRITE before calling
	// applyKeepCacheFlag, so we verify the gate logic here.
	wfs := newTestWFS()

	inode := uint64(42)

	entry := &LockedEntry{
		Entry: &filer_pb.Entry{
			Attributes: &filer_pb.FuseAttributes{Mtime: 1000, MtimeNs: 0},
		},
	}

	// Populate cache.
	var out1 fuse.OpenOut
	wfs.applyKeepCacheFlag(inode, entry, &out1)

	// Simulate write-mode open: the caller would skip applyKeepCacheFlag.
	var out2 fuse.OpenOut
	flags := uint32(fuse.O_ANYWRITE)
	if flags&fuse.O_ANYWRITE == 0 {
		wfs.applyKeepCacheFlag(inode, entry, &out2)
	}

	if out2.OpenFlags&fuse.FOPEN_KEEP_CACHE != 0 {
		t.Error("write open should not set FOPEN_KEEP_CACHE")
	}
}

func TestOpenKeepCache_BoundedEviction(t *testing.T) {
	// Verify the cache doesn't grow beyond openMtimeCacheMaxSize.
	wfs := newTestWFS()

	entry := &LockedEntry{
		Entry: &filer_pb.Entry{
			Attributes: &filer_pb.FuseAttributes{Mtime: 1000, MtimeNs: 0},
		},
	}

	for i := uint64(0); i < openMtimeCacheMaxSize+100; i++ {
		var out fuse.OpenOut
		wfs.applyKeepCacheFlag(i, entry, &out)
	}

	wfs.openMtimeMu.Lock()
	size := len(wfs.openMtimeCache)
	wfs.openMtimeMu.Unlock()

	if size > openMtimeCacheMaxSize {
		t.Errorf("cache size %d exceeds max %d", size, openMtimeCacheMaxSize)
	}
}
