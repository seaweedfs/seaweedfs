package weed_server

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/util/constants"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// checkScopeWorm applies the same WORM retention decision used by the HTTP
// filer to scoped gRPC mutations. The caller holds the path lock before
// invoking this helper, so the lookup and the mutation cannot be interleaved
// on this filer.
func (fs *FilerServer) checkScopeWorm(ctx context.Context, fullPath string) error {
	if !scopeKeepOnly(ctx) {
		return nil
	}

	enforced, err := fs.wormEnforcedForEntry(ctx, fullPath)
	if err != nil {
		return fmt.Errorf("check WORM %s: %w", fullPath, err)
	}
	if enforced {
		return status.Error(codes.PermissionDenied, constants.ErrMsgOperationNotPermitted)
	}
	return nil
}

// checkScopeWormDirectory refuses a subtree mutation wherever WORM could apply.
// The endpoint path locks do not lock descendants, so checking their timestamps
// before mutation would race with child writes. Check configuration instead,
// before moving or deleting any entries, even if no child is currently retained.
func (fs *FilerServer) checkScopeWormDirectory(ctx context.Context, fullPath string) error {
	if !scopeKeepOnly(ctx) {
		return nil
	}
	if fs.filer.FilerConf.IntersectsWorm(fullPath) {
		return status.Error(codes.PermissionDenied, "scoped directory mutation intersects a WORM rule")
	}
	return nil
}
