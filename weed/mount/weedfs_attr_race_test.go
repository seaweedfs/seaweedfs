package mount

import (
	"sync"
	"testing"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestAttrChunkRace guards the locking around an open handle's chunk slice.
//
// With writebackCache, async upload workers append chunks to an open file
// handle's shared entry under the LockedEntry lock (FileHandle.AddChunks),
// while metadata ops compute the file size by iterating entry.Chunks. SetAttr
// and GetAttr used to read that slice without the LockedEntry lock, so a
// concurrent append that reallocated the backing array produced a torn slice
// read and a nil pointer dereference in filer.TotalSize. Run under -race.
func TestAttrChunkRace(t *testing.T) {
	wfs := &WFS{
		option:         &Option{},
		inodeToPath:    NewInodeToPath(util.FullPath("/"), 0),
		fhMap:          NewFileHandleToInode(),
		openMtimeCache: make(map[uint64][2]int64, 8),
	}

	const inode = uint64(42)
	fullPath := util.FullPath("/dir/sample.txt")
	wfs.inodeToPath.Lookup(fullPath, 1, false, false, inode, true)

	entry := &filer_pb.Entry{
		Name:       "sample.txt",
		Attributes: &filer_pb.FuseAttributes{FileMode: 0644},
	}
	chunkGroup, err := filer.NewChunkGroup(nil, nil, nil, 1)
	if err != nil {
		t.Fatalf("NewChunkGroup: %v", err)
	}
	fh := &FileHandle{
		fh:              FileHandleId(1),
		inode:           inode,
		wfs:             wfs,
		entry:           &LockedEntry{Entry: entry},
		entryChunkGroup: chunkGroup,
	}
	wfs.fhMap.inode2fh[inode] = fh
	wfs.fhMap.fh2inode[fh.fh] = inode

	const iterations = 2000
	var wg sync.WaitGroup
	wg.Add(3)

	// Async uploader: append chunks, reallocating the backing array.
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			fh.AddChunks([]*filer_pb.FileChunk{{FileId: "x", Offset: int64(i), Size: 1}})
		}
	}()

	// SetAttr: mtime-only recomputes FileSize by iterating chunks; a shrinking
	// size takes the truncate path that rewrites entry.Chunks under the lock.
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			in := &fuse.SetAttrIn{}
			in.NodeId = inode
			if i%2 == 0 {
				in.Valid = fuse.FATTR_MTIME
				in.Mtime = uint64(i)
			} else {
				in.Valid = fuse.FATTR_SIZE
				in.Size = uint64(i % 8)
			}
			var out fuse.AttrOut
			wfs.SetAttr(nil, in, &out)
		}
	}()

	// GetAttr also computes FileSize by iterating chunks.
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			in := &fuse.GetAttrIn{}
			in.NodeId = inode
			var out fuse.AttrOut
			wfs.GetAttr(nil, in, &out)
		}
	}()

	wg.Wait()
}
