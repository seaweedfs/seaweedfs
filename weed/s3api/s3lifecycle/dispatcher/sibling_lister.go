package dispatcher

import (
	"context"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// filerSiblingLister counts entries in <bucketsPath>/<bucket>/<key>.versions/
// via the filer's existing list RPC; reused for every sole-survivor check.
type filerSiblingLister struct {
	client      filer_pb.SeaweedFilerClient
	bucketsPath string
}

// Count caps at 2 — the caller only distinguishes "sole survivor" (1)
// from "more than one" (>=2), so listing further siblings is wasted I/O
// on hot keys with many versions.
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
