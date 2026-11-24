package weed_server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
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

// checkEntryProtected refuses to change an entry that the S3 API or the filer
// HTTP API would refuse to change: one under an active Object Lock or WORM.
func (fs *WebDavFileSystem) checkEntryProtected(ctx context.Context, p string, entry *filer_pb.Entry) error {
	if string(entry.Extended[s3_constants.ExtDeleteMarkerKey]) != "true" && s3_objectlock.EntryHasActiveLock(entry, time.Now()) {
		glog.V(0).Infof("webdav: %s is under Object Lock", p)
		return os.ErrPermission
	}
	enforced, err := fs.wormEnforcedForEntry(ctx, p, entry)
	if err != nil {
		return err
	}
	if enforced {
		glog.V(0).Infof("webdav: %s is write-once", p)
		return os.ErrPermission
	}
	return nil
}

// checkSubtreeProtected refuses to delete or move a directory that a WORM
// rule covers or that holds an object under Object Lock.
func (fs *WebDavFileSystem) checkSubtreeProtected(ctx context.Context, dir string) error {
	fc, err := fs.readFilerConf(ctx)
	if err != nil {
		return err
	}
	prefix := strings.TrimRight(dir, "/") + "/"
	worm := fc.MatchStorageRule(prefix).GetWorm()
	for _, rule := range fc.ToProto().Locations {
		worm = worm || rule.GetWorm() && strings.HasPrefix(rule.LocationPrefix, prefix)
	}
	if worm {
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

func (fs *WebDavFileSystem) wormEnforcedForEntry(ctx context.Context, p string, entry *filer_pb.Entry) (bool, error) {
	if entry.WormEnforcedAtTsNs == 0 {
		return false, nil
	}
	fc, err := fs.readFilerConf(ctx)
	if err != nil {
		return false, err
	}
	rule := fc.MatchStorageRule(p)
	if !rule.GetWorm() {
		return false, nil
	}
	if rule.WormRetentionTimeSeconds == 0 {
		return true, nil
	}
	return time.Since(time.Unix(0, entry.WormEnforcedAtTsNs)).Seconds() < float64(rule.WormRetentionTimeSeconds), nil
}

func (fs *WebDavFileSystem) readFilerConf(ctx context.Context) (*filer.FilerConf, error) {
	fc := filer.NewFilerConf()
	confPath := string(util.NewFullPath(filer.DirectoryEtcSeaweedFS, filer.FilerConfName))
	entry, err := fs.getEntry(ctx, confPath)
	if errors.Is(err, os.ErrNotExist) {
		return fc, nil
	}
	if err != nil {
		return nil, err
	}
	content := entry.Content
	if len(content) == 0 && len(entry.GetChunks()) > 0 {
		var buf bytes.Buffer
		if err := filer.StreamContent(fs.filerClient, &buf, entry.GetChunks(), 0, int64(filer.FileSize(entry))); err != nil {
			return nil, fmt.Errorf("read %s: %w", confPath, err)
		}
		content = buf.Bytes()
	}
	if len(content) > 0 {
		if err := fc.LoadFromBytes(content); err != nil {
			return nil, fmt.Errorf("parse %s: %w", confPath, err)
		}
	}
	return fc, nil
}

// objectLockState makes the filer refuse the change if the entry's Object
// Lock state changed after it was checked.
func objectLockState(entry *filer_pb.Entry) map[string][]byte {
	state := make(map[string][]byte, 3)
	for _, key := range []string{s3_constants.ExtLegalHoldKey, s3_constants.ExtObjectLockModeKey, s3_constants.ExtRetentionUntilDateKey} {
		state[key] = entry.Extended[key]
	}
	return state
}
