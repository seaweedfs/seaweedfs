package weed_server

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"path/filepath"
)

func (fs *FilerServer) AtomicRenameEntry(ctx context.Context, req *filer_pb.AtomicRenameEntryRequest) (*filer_pb.AtomicRenameEntryResponse, error) {

	glog.V(1).Infof("AtomicRenameEntry %v", req)

	if err := fs.Copy(ctx, nil, req.OldDirectory, req.NewDirectory, nil, true); err != nil {
		return nil, err
	} else {
		return &filer_pb.AtomicRenameEntryResponse{}, nil
	}

}

func (fs *FilerServer) StreamRenameEntry(req *filer_pb.StreamRenameEntryRequest, stream filer_pb.SeaweedFiler_StreamRenameEntryServer) (err error) {

	glog.V(1).Infof("StreamRenameEntry %v", req)

	oldParent := util.FullPath(filepath.ToSlash(req.OldDirectory))
	newParent := util.FullPath(filepath.ToSlash(req.NewDirectory))

	if err := fs.filer.CanRename(oldParent, newParent); err != nil {
		return err
	}

	ctx := context.Background()

	ctx, err = fs.filer.BeginTransaction(ctx)
	if err != nil {
		return err
	}

	oldEntry, err := fs.filer.FindEntry(ctx, oldParent.Child(req.OldName))
	if err != nil {
		fs.filer.RollbackTransaction(ctx)
		return fmt.Errorf("%s/%s not found: %v", req.OldDirectory, req.OldName, err)
	}

	if oldEntry.IsDirectory() {
		// follow https://pubs.opengroup.org/onlinepubs/000095399/functions/rename.html
		targetDir := newParent.Child(req.NewName)
		newEntry, err := fs.filer.FindEntry(ctx, targetDir)
		if err == nil {
			if !newEntry.IsDirectory() {
				fs.filer.RollbackTransaction(ctx)
				return fmt.Errorf("%s is not directory", targetDir)
			}
			if entries, _, _ := fs.filer.ListDirectoryEntries(context.Background(), targetDir, "", false, 1, "", "", ""); len(entries) > 0 {
				return fmt.Errorf("%s is not empty", targetDir)
			}
		}
	}

	moveErr := fs.copyEntry(ctx, stream, oldParent, oldEntry, newParent, req.NewName, req.Signatures, true)
	if moveErr != nil {
		fs.filer.RollbackTransaction(ctx)
		return fmt.Errorf("%s/%s move error: %v", req.OldDirectory, req.OldName, moveErr)
	} else {
		if commitError := fs.filer.CommitTransaction(ctx); commitError != nil {
			fs.filer.RollbackTransaction(ctx)
			return fmt.Errorf("%s/%s move commit error: %v", req.OldDirectory, req.OldName, commitError)
		}
	}

	return nil
}
