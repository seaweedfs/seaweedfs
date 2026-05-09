package dispatcher

import (
	"context"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// filerSiblingLister counts entries under .versions/<key>/ via the
// filer's list RPC. Caps at 2 — callers only distinguish 1 vs >=2.
type filerSiblingLister struct {
	client      filer_pb.SeaweedFilerClient
	bucketsPath string
}

func (l *filerSiblingLister) Count(ctx context.Context, bucket, objectKey string) (int, error) {
	dir := strings.TrimSuffix(l.bucketsPath, "/") + "/" + bucket + "/" + objectKey + s3_constants.VersionsFolder
	count := 0
	err := filer_pb.SeaweedList(ctx, l.client, dir, "", func(_ *filer_pb.Entry, _ bool) error {
		count++
		return nil
	}, "", false, 2)
	if err != nil {
		return 0, err
	}
	return count, nil
}
