package mount

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestHandleRenameResponseCachesTargetForUncachedDirectory(t *testing.T) {
	uidGidMapper, err := meta_cache.NewUidGidMapper("", "")
	if err != nil {
		t.Fatalf("create uid/gid mapper: %v", err)
	}

	root := util.FullPath("/")
	inodeToPath := NewInodeToPath(root, 1)

	mc := meta_cache.NewMetaCache(
		filepath.Join(t.TempDir(), "meta"),
		uidGidMapper,
		root,
		func(path util.FullPath) {
			inodeToPath.MarkChildrenCached(path)
		},
		func(path util.FullPath) bool {
			return inodeToPath.IsChildrenCached(path)
		},
		func(util.FullPath, *filer_pb.Entry) {},
		nil,
	)
	defer mc.Shutdown()

	parentPath := util.FullPath("/repo/.git")
	sourcePath := parentPath.Child("config.lock")
	targetPath := parentPath.Child("config")

	inodeToPath.Lookup(parentPath, 1, true, false, 0, true)
	sourceInode := inodeToPath.Lookup(sourcePath, 1, false, false, 0, true)
	inodeToPath.Lookup(targetPath, 1, false, false, 0, true)

	wfs := &WFS{
		metaCache:   mc,
		inodeToPath: inodeToPath,
		fhMap:       NewFileHandleToInode(),
	}

	resp := &filer_pb.StreamRenameEntryResponse{
		Directory: string(parentPath),
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{
				Name: "config.lock",
			},
			NewEntry: &filer_pb.Entry{
				Name: "config",
				Attributes: &filer_pb.FuseAttributes{
					Crtime:   1,
					Mtime:    1,
					FileMode: 0100644,
					FileSize: 53,
					Inode:    sourceInode,
				},
			},
			NewParentPath: string(parentPath),
		},
	}

	if err := wfs.handleRenameResponse(context.Background(), resp); err != nil {
		t.Fatalf("handle rename response: %v", err)
	}

	entry, findErr := mc.FindEntry(context.Background(), targetPath)
	if findErr != nil {
		t.Fatalf("find target entry: %v", findErr)
	}
	if entry == nil {
		t.Fatalf("target entry %s not cached", targetPath)
	}
	if entry.FileSize != 53 {
		t.Fatalf("cached file size = %d, want 53", entry.FileSize)
	}

	updatedInode, found := inodeToPath.GetInode(targetPath)
	if !found {
		t.Fatalf("target path %s missing inode mapping", targetPath)
	}
	if updatedInode != sourceInode {
		t.Fatalf("target inode = %d, want %d", updatedInode, sourceInode)
	}
}
