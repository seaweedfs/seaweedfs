package dispatcher

import (
	"context"
	"errors"
	"path"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/router"
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

	parent, name := path.Split(bucketPath + "/" + objectKey)
	parent = strings.TrimRight(parent, "/")
	if parent == "" {
		parent = "/"
	}
	resp, err := l.client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
		Directory: parent,
		Name:      name,
	})
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return s, nil
		}
		return router.Survivors{}, err
	}
	if resp.Entry != nil && !resp.Entry.IsDirectory {
		s.HasNullVersion = true
	}
	return s, nil
}
