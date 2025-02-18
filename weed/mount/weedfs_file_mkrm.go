package mount

import (
	"context"
	"fmt"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

/**
 * Create and open a file
 *
 * If the file does not exist, first create it with the specified
 * mode, and then open it.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the mknod() and open() methods
 * will be called instead.
 */
func (wfs *WFS) Create(cancel <-chan struct{}, in *fuse.CreateIn, name string, out *fuse.CreateOut) (code fuse.Status) {
	// if implemented, need to use
	// 	inode := wfs.inodeToPath.Lookup(entryFullPath)
	// to ensure nlookup counter
	return fuse.ENOSYS
}

/** Create a file node
 *
 * This is called for creation of all non-directory, non-symlink
 * nodes.  If the filesystem defines a create() method, then for
 * regular files that will be called instead.
 */
func (wfs *WFS) Mknod(cancel <-chan struct{}, in *fuse.MknodIn, name string, out *fuse.EntryOut) (code fuse.Status) {

	if wfs.IsOverQuota {
		return fuse.Status(syscall.ENOSPC)
	}

	if s := checkName(name); s != fuse.OK {
		return s
	}

	dirFullPath, code := wfs.inodeToPath.GetPath(in.NodeId)
	if code != fuse.OK {
		return
	}

	entryFullPath := dirFullPath.Child(name)
	fileMode := toOsFileMode(in.Mode)
	now := time.Now().Unix()
	inode := wfs.inodeToPath.AllocateInode(entryFullPath, now)

	newEntry := &filer_pb.Entry{
		Name:        name,
		IsDirectory: false,
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    now,
			Crtime:   now,
			FileMode: uint32(fileMode),
			Uid:      in.Uid,
			Gid:      in.Gid,
			TtlSec:   wfs.option.TtlSec,
			Rdev:     in.Rdev,
			Inode:    inode,
		},
	}

	err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		wfs.mapPbIdFromLocalToFiler(newEntry)
		defer wfs.mapPbIdFromFilerToLocal(newEntry)

		request := &filer_pb.CreateEntryRequest{
			Directory:                string(dirFullPath),
			Entry:                    newEntry,
			Signatures:               []int32{wfs.signature},
			SkipCheckParentDirectory: true,
		}

		glog.V(1).Infof("mknod: %v", request)
		if err := filer_pb.CreateEntry(client, request); err != nil {
			glog.V(0).Infof("mknod %s: %v", entryFullPath, err)
			return err
		}

		if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(request.Directory, request.Entry)); err != nil {
			return fmt.Errorf("local mknod %s: %v", entryFullPath, err)
		}

		return nil
	})

	glog.V(3).Infof("mknod %s: %v", entryFullPath, err)

	if err != nil {
		return fuse.EIO
	}

	// this is to increase nlookup counter
	inode = wfs.inodeToPath.Lookup(entryFullPath, newEntry.Attributes.Crtime, false, false, inode, true)

	wfs.outputPbEntry(out, inode, newEntry)

	return fuse.OK

}

/** Remove a file */
func (wfs *WFS) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) (code fuse.Status) {

	dirFullPath, code := wfs.inodeToPath.GetPath(header.NodeId)
	if code != fuse.OK {
		if code == fuse.ENOENT {
			return fuse.OK
		}
		return code
	}
	entryFullPath := dirFullPath.Child(name)

	entry, code := wfs.maybeLoadEntry(entryFullPath)
	if code != fuse.OK {
		if code == fuse.ENOENT {
			return fuse.OK
		}
		return code
	}

	if wormEnforced, _ := wfs.wormEnforcedForEntry(entryFullPath, entry); wormEnforced {
		return fuse.EPERM
	}

	// first, ensure the filer store can correctly delete
	glog.V(3).Infof("remove file: %v", entryFullPath)
	isDeleteData := entry != nil && entry.HardLinkCounter <= 1
	err := filer_pb.Remove(wfs, string(dirFullPath), name, isDeleteData, false, false, false, []int32{wfs.signature})
	if err != nil {
		glog.V(0).Infof("remove %s: %v", entryFullPath, err)
		return fuse.OK
	}

	// then, delete meta cache
	if err = wfs.metaCache.DeleteEntry(context.Background(), entryFullPath); err != nil {
		glog.V(3).Infof("local DeleteEntry %s: %v", entryFullPath, err)
		return fuse.EIO
	}

	wfs.inodeToPath.RemovePath(entryFullPath)

	return fuse.OK

}
