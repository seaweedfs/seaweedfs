package filesys

import (
	"context"
	"fmt"

	"github.com/HZ89/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

func (dir *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDirectory fs.Node) error {

	newDir := newDirectory.(*Dir)

	return dir.wfs.withFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: dir.Path,
			OldName:      req.OldName,
			NewDirectory: newDir.Path,
			NewName:      req.NewName,
		}

		_, err := client.AtomicRenameEntry(ctx, request)
		if err != nil {
			return fmt.Errorf("renaming %s/%s => %s/%s: %v", dir.Path, req.OldName, newDir.Path, req.NewName, err)
		}

		return nil

	})

}
