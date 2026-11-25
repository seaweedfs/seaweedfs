package weed_server

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_objectlock"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (fs *WebDavFileSystem) getEntry(ctx context.Context, p string) (*filer_pb.Entry, error) {
	entry, _, _, err := filer_pb.GetEntry(ctx, fs, util.FullPath(p))
	if errors.Is(err, filer_pb.ErrNotFound) || err == nil && entry == nil {
		return nil, os.ErrNotExist
	}
	return entry, err
}

// checkWritable refuses a write that would replace a protected file; a file
// over a directory is left for the filer to refuse.
func (fs *WebDavFileSystem) checkWritable(ctx context.Context, p string) error {
	entry, err := fs.getEntry(ctx, p)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if entry.IsDirectory {
		return nil
	}
	return fs.checkEntryProtected(ctx, p, entry)
}

// checkEntryProtected refuses to change an entry that the S3 API or the filer
// HTTP API would refuse to change: one under an active Object Lock or WORM.
func (fs *WebDavFileSystem) checkEntryProtected(ctx context.Context, p string, entry *filer_pb.Entry) error {
	if string(entry.Extended[s3_constants.ExtDeleteMarkerKey]) != "true" && s3_objectlock.EntryHasActiveLock(entry, time.Now()) {
		glog.V(0).Infof("webdav: %s is under Object Lock", p)
		return os.ErrPermission
	}
	if entry.WormEnforcedAtTsNs == 0 {
		return nil
	}
	fc, err := filer.ReadFilerConfFromClient(ctx, fs, fs.filerClient)
	if err != nil {
		return err
	}
	if fc.WormEnforced(p, entry.WormEnforcedAtTsNs) {
		glog.V(0).Infof("webdav: %s is write-once", p)
		return os.ErrPermission
	}
	return nil
}

// checkSubtreeProtected refuses to delete or move a directory that a WORM
// rule covers or that holds an object under Object Lock.
func (fs *WebDavFileSystem) checkSubtreeProtected(ctx context.Context, dir string) error {
	fc, err := filer.ReadFilerConfFromClient(ctx, fs, fs.filerClient)
	if err != nil {
		return err
	}
	if fc.IntersectsWorm(dir) {
		glog.V(0).Infof("webdav: %s intersects a WORM rule", dir)
		return os.ErrPermission
	}
	var locked bool
	if err := fs.WithFilerClient(true, func(client filer_pb.SeaweedFilerClient) error {
		locked, err = s3_objectlock.HasObjectsWithActiveLocks(ctx, client, dir)
		return err
	}); err != nil {
		return err
	}
	if locked {
		glog.V(0).Infof("webdav: %s holds objects under Object Lock", dir)
		return os.ErrPermission
	}
	return nil
}

// checkRenameProtected refuses to move a protected entry, a directory holding
// one, or onto a protected target, which the rename would replace.
func (fs *WebDavFileSystem) checkRenameProtected(ctx context.Context, oldName, newName string) error {
	srcEntry, err := fs.getEntry(ctx, oldName)
	if err != nil {
		return err
	}
	if err := fs.checkEntryProtected(ctx, oldName, srcEntry); err != nil {
		return err
	}
	if srcEntry.IsDirectory {
		if err := fs.checkSubtreeProtected(ctx, oldName); err != nil {
			return err
		}
	}
	dstEntry, err := fs.getEntry(ctx, newName)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return fs.checkEntryProtected(ctx, newName, dstEntry)
}
