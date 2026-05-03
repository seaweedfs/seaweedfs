package iceberg

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// The following methods export the internal maintenance operations for use
// by integration tests. They are intentionally thin wrappers.

func (h *Handler) ExpireSnapshots(ctx context.Context, client filer_pb.SeaweedFilerClient, bucketName, tablePath string, config Config) (string, map[string]int64, error) {
	return h.expireSnapshots(ctx, client, bucketName, tablePath, config)
}

func (h *Handler) RemoveOrphans(ctx context.Context, client filer_pb.SeaweedFilerClient, bucketName, tablePath string, config Config) (string, map[string]int64, error) {
	return h.removeOrphans(ctx, client, bucketName, tablePath, config)
}

func (h *Handler) RewriteManifests(ctx context.Context, client filer_pb.SeaweedFilerClient, bucketName, tablePath string, config Config) (string, map[string]int64, error) {
	return h.rewriteManifests(ctx, client, bucketName, tablePath, config)
}

func (h *Handler) CompactDataFiles(ctx context.Context, client filer_pb.SeaweedFilerClient, bucketName, tablePath string, config Config) (string, map[string]int64, error) {
	return h.compactDataFiles(ctx, client, bucketName, tablePath, config, nil)
}
