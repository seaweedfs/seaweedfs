package filer

import (
	"context"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// IsEvictableRemoteEntry reports whether an entry's local chunks are backed by
// a synchronized remote copy, mirroring the checks remote.uncache applies:
// remote-backed, chunks present, and not ahead of the remote version.
func IsEvictableRemoteEntry(entry *Entry) bool {
	if entry.IsDirectory() || entry.Remote == nil {
		return false
	}
	if entry.Remote.LastLocalSyncTsNs <= 0 || len(entry.GetChunks()) == 0 {
		return false
	}
	if entry.Remote.LastLocalSyncTsNs/1e9 < entry.Mtime.Unix() {
		return false
	}
	return true
}

// ListEvictableRemoteEntries walks every mounted directory and returns
// synchronized remote entries holding local chunks, oldest cached first.
func (f *Filer) ListEvictableRemoteEntries(ctx context.Context, mounts []util.FullPath, minCacheAge time.Duration) (out []*Entry) {
	cutoffNs := time.Now().UnixNano() - minCacheAge.Nanoseconds()
	for _, dir := range mounts {
		if err := f.collectEvictableRemoteEntries(ctx, dir, cutoffNs, &out); err != nil {
			glog.WarningfCtx(ctx, "list evictable remote entries under %s: %v", dir, err)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Remote.LastLocalSyncTsNs < out[j].Remote.LastLocalSyncTsNs
	})
	return out
}

func (f *Filer) collectEvictableRemoteEntries(ctx context.Context, dir util.FullPath, cutoffNs int64, out *[]*Entry) error {
	startFileName := ""
	for {
		var subDirs []util.FullPath
		lastFileName, err := f.Store.ListDirectoryEntries(ctx, dir, startFileName, false, 1024, func(entry *Entry) (bool, error) {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			if entry.IsDirectory() {
				subDirs = append(subDirs, entry.FullPath)
				return true, nil
			}
			if entry.Remote != nil && entry.Remote.LastLocalSyncTsNs > 0 && entry.Remote.LastLocalSyncTsNs <= cutoffNs && IsEvictableRemoteEntry(entry) {
				*out = append(*out, entry)
			}
			return true, nil
		})
		if err != nil {
			return err
		}
		for _, subDir := range subDirs {
			if err := f.collectEvictableRemoteEntries(ctx, subDir, cutoffNs, out); err != nil {
				return err
			}
		}
		if lastFileName == "" {
			return nil
		}
		startFileName = lastFileName
	}
}
