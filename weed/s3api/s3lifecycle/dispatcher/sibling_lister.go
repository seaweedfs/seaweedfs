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
