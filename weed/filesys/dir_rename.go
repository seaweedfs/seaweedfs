package filesys

import (
	"context"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
	"math"
	"path/filepath"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func (dir *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDirectory fs.Node) error {

	newDir := newDirectory.(*Dir)

	return dir.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		// find existing entry
		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir.Path,
			Name:      req.OldName,
		}

		glog.V(4).Infof("find existing directory entry: %v", request)
		resp, err := client.LookupDirectoryEntry(ctx, request)
		if err != nil {
			glog.V(3).Infof("renaming find %s/%s: %v", dir.Path, req.OldName, err)
			return fuse.ENOENT
		}

		entry := resp.Entry

		glog.V(4).Infof("found existing directory entry resp: %+v", resp)

		return moveEntry(ctx, client, dir.Path, entry, newDir.Path, req.NewName)
	})

}

func moveEntry(ctx context.Context, client filer_pb.SeaweedFilerClient, oldParent string, entry *filer_pb.Entry, newParent, newName string) error {
	if entry.IsDirectory {
		currentDirPath := filepath.Join(oldParent, entry.Name)

		lastFileName := ""
		includeLastFile := false
		limit := math.MaxInt32
		for limit > 0 {
			request := &filer_pb.ListEntriesRequest{
				Directory:          currentDirPath,
				StartFromFileName:  lastFileName,
				InclusiveStartFrom: includeLastFile,
				Limit:              1024,
			}
			glog.V(4).Infof("read directory: %v", request)
			resp, err := client.ListEntries(ctx, request)
			if err != nil {
				glog.V(0).Infof("list %s: %v", oldParent, err)
				return fuse.EIO
			}
			if len(resp.Entries) == 0 {
				break
			}

			for _, item := range resp.Entries {
				lastFileName = item.Name
				err := moveEntry(ctx, client, currentDirPath, item, filepath.Join(newParent, newName), item.Name)
				if err != nil {
					return err
				}
				limit--
			}
			if len(resp.Entries) < 1024 {
				break
			}
		}

	}

	// add to new directory
	{
		request := &filer_pb.CreateEntryRequest{
			Directory: newParent,
			Entry: &filer_pb.Entry{
				Name:        newName,
				IsDirectory: entry.IsDirectory,
				Attributes:  entry.Attributes,
				Chunks:      entry.Chunks,
			},
		}

		glog.V(1).Infof("create new entry: %v", request)
		if _, err := client.CreateEntry(ctx, request); err != nil {
			glog.V(0).Infof("renaming create %s/%s: %v", newParent, newName, err)
			return fuse.EIO
		}
	}

	// delete old entry
	{
		request := &filer_pb.DeleteEntryRequest{
			Directory:    oldParent,
			Name:         entry.Name,
			IsDeleteData: false,
		}

		glog.V(1).Infof("remove old entry: %v", request)
		_, err := client.DeleteEntry(ctx, request)
		if err != nil {
			glog.V(0).Infof("renaming delete %s/%s: %v", oldParent, entry.Name, err)
			return fuse.EIO
		}

	}

	return nil

}
