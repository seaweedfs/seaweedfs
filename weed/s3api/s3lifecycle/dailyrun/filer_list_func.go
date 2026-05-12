package dailyrun

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/bootstrap"
)

// listPageSize is the page size for paginated directory listings. The
// filer caps SeaweedList(..., limit=0) at DirListingLimit (1000 by
// default) per call, so a single-page list would silently truncate
// large directories. Atomic so tests can shrink it without racing.
var listPageSize atomic.Uint32

func init() { listPageSize.Store(1024) }

// FilerListFunc returns a bootstrap.ListFunc that streams entries
// under <bucketsPath>/<bucket> for use by the daily-run walker.
//
// Phase 4b scope: non-versioned, non-MPU entries only. Versioned
// `.versions/` directories and MPU init records at `.uploads/<id>`
// are skipped at this stage; the follow-up commit adds the sibling
// expansion needed for noncurrent retention math and MPU dispatch.
func FilerListFunc(client filer_pb.SeaweedFilerClient, bucketsPath string) bootstrap.ListFunc {
	return func(ctx context.Context, bucket, start string, cb func(*bootstrap.Entry) error) error {
		if client == nil {
			return fmt.Errorf("FilerListFunc: nil client")
		}
		root := strings.TrimSuffix(bucketsPath, "/") + "/" + bucket
		return walkBucketTree(ctx, client, root, root, start, cb)
	}
}

// walkBucketTree recursively lists dir, emitting each file as a
// bootstrap.Entry whose Path is bucket-relative. Subdirectories
// recurse; `.versions/` and `.uploads/` directories are skipped for
// now (Phase 4b follow-up).
func walkBucketTree(ctx context.Context, client filer_pb.SeaweedFilerClient, dir, bucketRoot, start string, cb func(*bootstrap.Entry) error) error {
	return listAll(ctx, client, dir, func(e *filer_pb.Entry) error {
		if e == nil || e.Attributes == nil {
			return nil
		}
		full := dir + "/" + e.Name
		key := strings.TrimPrefix(full, bucketRoot+"/")
		if e.IsDirectory {
			if isVersionsDir(e) {
				// TODO(phase4b): expand into per-version Entries.
				return nil
			}
			if isMPUInitDirShape(key) {
				// TODO(phase4b): emit MPU init as a single Entry.
				return nil
			}
			return walkBucketTree(ctx, client, full, bucketRoot, start, cb)
		}
		// Resume contract: skip entries Path <= start.
		if start != "" && key <= start {
			return nil
		}
		entry := &bootstrap.Entry{
			Path:     key,
			ModTime:  time.Unix(e.Attributes.Mtime, int64(e.Attributes.MtimeNs)),
			Size:     int64(e.Attributes.FileSize),
			IsLatest: true, // Non-versioned default; versioned expansion overrides.
		}
		return cb(entry)
	})
}

// listAll issues paginated SeaweedList calls until exhausted. Ported
// from scheduler/bootstrap.go's same-named helper; Phase 5 deletes
// the scheduler copy when the streaming path is removed.
func listAll(ctx context.Context, client filer_pb.SeaweedFilerClient, dir string, fn func(*filer_pb.Entry) error) error {
	pageSize := listPageSize.Load()
	startFrom := ""
	for {
		var pageCount uint32
		var lastName string
		if err := filer_pb.SeaweedList(ctx, client, dir, "", func(e *filer_pb.Entry, _ bool) error {
			pageCount++
			if e != nil {
				lastName = e.Name
			}
			return fn(e)
		}, startFrom, false, pageSize); err != nil {
			return err
		}
		if pageCount < pageSize {
			return nil
		}
		startFrom = lastName
	}
}

func isVersionsDir(entry *filer_pb.Entry) bool {
	return entry.IsDirectory && strings.HasSuffix(entry.Name, s3_constants.VersionsFolder)
}

// isMPUInitDirShape mirrors scheduler/bootstrap.go's isMPUInitDir but
// checks the path shape only; the Extended-attr verification lives in
// the eventual full-MPU emission path.
func isMPUInitDirShape(key string) bool {
	uploadsPrefix := s3_constants.MultipartUploadsFolder + "/"
	if !strings.HasPrefix(key, uploadsPrefix) {
		return false
	}
	rest := key[len(uploadsPrefix):]
	return rest != "" && !strings.ContainsRune(rest, '/')
}
