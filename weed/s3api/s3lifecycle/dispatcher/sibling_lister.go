package dispatcher

import (
	"context"
	"errors"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/router"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// filerSiblingLister inspects the .versions/<key>/ folder and the bare
// null-version. Listing caps at 2 — callers only distinguish 1 vs >=2.
type filerSiblingLister struct {
	client      filer_pb.SeaweedFilerClient
	bucketsPath string
}

func (l *filerSiblingLister) Survivors(ctx context.Context, bucket, objectKey string) (router.Survivors, error) {
	bucketPath := strings.TrimSuffix(l.bucketsPath, "/") + "/" + bucket
	versionsDir := bucketPath + "/" + objectKey + s3_constants.VersionsFolder

	var s router.Survivors
	err := filer_pb.SeaweedList(ctx, l.client, versionsDir, "", func(entry *filer_pb.Entry, _ bool) error {
		s.Count++
		if s.Count == 1 && entry != nil {
			s.LoneEntry = entry
		} else if s.Count > 1 {
			s.LoneEntry = nil
		}
		return nil
	}, "", false, 2)
	if err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		return router.Survivors{}, err
	}

	// NewFullPath strips a trailing slash from objectKey so directory-key
	// objects (foo/) split the same as regular keys. LookupEntry
	// normalizes the gRPC string-mapped not-found into ErrNotFound; a
	// raw client.LookupDirectoryEntry would return that as a generic
	// error and suppress every otherwise-valid match.
	parent, name := util.NewFullPath(bucketPath, objectKey).DirAndName()
	resp, err := filer_pb.LookupEntry(ctx, l.client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: parent,
		Name:      name,
	})
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return s, nil
		}
		return router.Survivors{}, err
	}
	// Bare regular file or an explicit S3 directory-marker (an empty
	// directory entry with Mime set) both count as the null version.
	if resp.Entry != nil && (!resp.Entry.IsDirectory || resp.Entry.IsDirectoryKeyObject()) {
		s.HasNullVersion = true
	}
	return s, nil
}

// ListVersions paginates every file under .versions/<key>/. Used by
// the pointer-transition expansion path when a NewerNoncurrentVersions
// rule needs accurate per-version ranking. Subdirectories and entries
// without ExtVersionIdKey are filtered out. NotFound returns
// (nil, nil) so a hard-deleted .versions/ container collapses cleanly.
func (l *filerSiblingLister) ListVersions(ctx context.Context, bucket, objectKey string) ([]*filer_pb.Entry, error) {
	bucketPath := strings.TrimSuffix(l.bucketsPath, "/") + "/" + bucket
	dir := bucketPath + "/" + objectKey + s3_constants.VersionsFolder
	const pageSize uint32 = 1024
	startFrom := ""
	var versions []*filer_pb.Entry
	for {
		var pageCount uint32
		var lastName string
		err := filer_pb.SeaweedList(ctx, l.client, dir, "", func(e *filer_pb.Entry, _ bool) error {
			pageCount++
			if e == nil {
				return nil
			}
			lastName = e.Name
			if e.Attributes == nil || e.IsDirectory {
				return nil
			}
			if id, ok := e.Extended[s3_constants.ExtVersionIdKey]; !ok || len(id) == 0 {
				return nil
			}
			versions = append(versions, e)
			return nil
		}, startFrom, false, pageSize)
		if err != nil {
			if errors.Is(err, filer_pb.ErrNotFound) {
				return nil, nil
			}
			return nil, err
		}
		if pageCount < pageSize {
			return versions, nil
		}
		startFrom = lastName
	}
}

// LookupVersion fetches <bucketsPath>/<bucket>/<objectKey>.versions/v_<id>
// for the pointer-transition router branch. NotFound returns (nil, nil)
// — the displaced version may have been hard-deleted between the
// pointer update and the lookup.
func (l *filerSiblingLister) LookupVersion(ctx context.Context, bucket, objectKey, versionID string) (*filer_pb.Entry, error) {
	if versionID == "" {
		return nil, nil
	}
	bucketPath := strings.TrimSuffix(l.bucketsPath, "/") + "/" + bucket
	dir := bucketPath + "/" + objectKey + s3_constants.VersionsFolder
	resp, err := filer_pb.LookupEntry(ctx, l.client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      "v_" + versionID,
	})
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if resp == nil {
		return nil, nil
	}
	return resp.Entry, nil
}
