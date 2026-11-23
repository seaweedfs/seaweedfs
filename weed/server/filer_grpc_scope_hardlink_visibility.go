package weed_server

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// scopeWritesSharedHardLink refuses writes through a shared record: another
// name can be outside the token's grants. Removing this name's link and data
// leaves the other names' data intact and remains allowed.
func scopeWritesSharedHardLink(stored, requested *filer_pb.Entry) error {
	if len(stored.GetHardLinkId()) == 0 || stored.GetHardLinkCounter() <= 1 {
		return nil
	}
	if len(requested.GetHardLinkId()) == 0 && len(requested.GetChunks()) == 0 && len(requested.GetContent()) == 0 {
		return nil
	}
	return fmt.Errorf("writes through a hard link with more than one name are not allowed")
}

// checkScopeHardLinkWrite rechecks the stored count under the handler's path
// lock. It does not change the shared registry's existing concurrency model.
func checkScopeHardLinkWrite(ctx context.Context, stored *filer.Entry, requested *filer_pb.Entry) error {
	if !scopeKeepOnly(ctx) || stored == nil {
		return nil
	}
	if err := scopeWritesSharedHardLink(stored.ToProtoEntry(), requested); err != nil {
		return status.Errorf(codes.PermissionDenied, "filer: %v", err)
	}
	return nil
}
