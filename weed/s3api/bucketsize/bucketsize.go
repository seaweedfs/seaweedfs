// Package bucketsize sizes an S3 bucket from the filer's metadata: every byte
// the bucket stores. Quota enforcement uses it for remote-mounted buckets,
// whose local volumes are only a cache and so cannot be sized from the
// collection.
package bucketsize

import (
	"context"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// StoredSize is the bytes one entry stores. A file stores filer.FileSize: its
// chunks, its attribute size, and its remote size when the remote copy is
// newer. A remote-only file (remote.mount, uncached or evicted) keeps its
// attribute size, so it counts in full though it holds no local chunk. A
// directory stores nothing unless it is also an object ("<key>/" or a prefix
// object), which keeps its body in chunks or inline content.
func StoredSize(entry *filer_pb.Entry) uint64 {
	if entry == nil {
		return 0
	}
	if !entry.IsDirectory {
		return filer.FileSize(entry)
	}
	if !entry.IsDirectoryKeyObject() {
		return 0
	}
	if size := filer.FileSize(entry); size > 0 {
		return size
	}
	return uint64(len(entry.Content))
}

// Size walks bucketDir one directory at a time and sums StoredSize over every
// entry under it, whatever an S3 listing would show of it: noncurrent
// versions and delete-marked objects under "<key>.versions/", and in-progress
// multipart parts under ".uploads/", all hold stored bytes and all count, as
// they did in the collection this replaces. Any listing error is returned: a
// partial sum is never a bucket's size.
func Size(ctx context.Context, client filer_pb.FilerClient, bucketDir util.FullPath) (uint64, error) {
	var total uint64
	queue := []util.FullPath{util.FullPath(util.NormalizePath(string(bucketDir)))}
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		dir := queue[0]
		queue = queue[1:]
		err := filer_pb.ReadDirAllEntries(ctx, client, dir, "", func(entry *filer_pb.Entry, isLast bool) error {
			total += StoredSize(entry)
			if entry.IsDirectory {
				queue = append(queue, dir.Child(entry.Name))
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
	}
	return total, nil
}

// IsMounted reports whether any part of bucketDir is remote-mounted: the
// bucket directory itself, a directory above it, or one inside it.
func IsMounted(mappings *remote_pb.RemoteStorageMapping, bucketDir string) bool {
	if mappings == nil {
		return false
	}
	bucketDir = strings.TrimSuffix(bucketDir, "/")
	for mountDir := range mappings.Mappings {
		m := strings.TrimSuffix(mountDir, "/")
		if m == bucketDir || strings.HasPrefix(bucketDir, m+"/") || strings.HasPrefix(m, bucketDir+"/") {
			return true
		}
	}
	return false
}
