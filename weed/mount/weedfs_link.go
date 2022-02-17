package mount

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/hanwen/go-fuse/v2/fuse"
	"time"
)

const (
	HARD_LINK_MARKER = '\x01'
)

/** Create a hard link to a file */
func (wfs *WFS) Link(cancel <-chan struct{}, in *fuse.LinkIn, name string, out *fuse.EntryOut) (code fuse.Status) {

	if s := checkName(name); s != fuse.OK {
		return s
	}

	newParentPath := wfs.inodeToPath.GetPath(in.NodeId)
	oldEntryPath := wfs.inodeToPath.GetPath(in.Oldnodeid)
	oldParentPath, _ := oldEntryPath.DirAndName()

	oldEntry, status := wfs.maybeLoadEntry(oldEntryPath)
	if status != fuse.OK {
		return status
	}

	// update old file to hardlink mode
	if len(oldEntry.HardLinkId) == 0 {
		oldEntry.HardLinkId = append(util.RandomBytes(16), HARD_LINK_MARKER)
		oldEntry.HardLinkCounter = 1
	}
	oldEntry.HardLinkCounter++
	updateOldEntryRequest := &filer_pb.UpdateEntryRequest{
		Directory:  oldParentPath,
		Entry:      oldEntry,
		Signatures: []int32{wfs.signature},
	}

	// CreateLink 1.2 : update new file to hardlink mode
	oldEntry.Attributes.Mtime = time.Now().Unix()
	request := &filer_pb.CreateEntryRequest{
		Directory: string(newParentPath),
		Entry: &filer_pb.Entry{
			Name:            name,
			IsDirectory:     false,
			Attributes:      oldEntry.Attributes,
			Chunks:          oldEntry.Chunks,
			Extended:        oldEntry.Extended,
			HardLinkId:      oldEntry.HardLinkId,
			HardLinkCounter: oldEntry.HardLinkCounter,
		},
		Signatures: []int32{wfs.signature},
	}

	// apply changes to the filer, and also apply to local metaCache
	err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		wfs.mapPbIdFromLocalToFiler(request.Entry)
		defer wfs.mapPbIdFromFilerToLocal(request.Entry)

		if err := filer_pb.UpdateEntry(client, updateOldEntryRequest); err != nil {
			return err
		}
		wfs.metaCache.UpdateEntry(context.Background(), filer.FromPbEntry(updateOldEntryRequest.Directory, updateOldEntryRequest.Entry))

		if err := filer_pb.CreateEntry(client, request); err != nil {
			return err
		}

		wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(request.Directory, request.Entry))

		return nil
	})

	newEntryPath := newParentPath.Child(name)

	if err != nil {
		glog.V(0).Infof("Link %v -> %s: %v", oldEntryPath, newEntryPath, err)
		return fuse.EIO
	}

	inode := wfs.inodeToPath.Lookup(newEntryPath, false, true)

	wfs.outputPbEntry(out, inode, request.Entry)

	return fuse.OK
}
