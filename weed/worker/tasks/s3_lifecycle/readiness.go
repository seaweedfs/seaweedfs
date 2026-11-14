package s3_lifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var lifecycleReadyTimeout = 10 * time.Second

// checkLifecycleReady authenticates to one S3 front with the credentials
// LifecycleDelete uses, without touching bucket or object metadata.
func checkLifecycleReady(ctx context.Context, c s3_lifecycle_pb.SeaweedS3LifecycleInternalClient) error {
	ctx, cancel := context.WithTimeout(ctx, lifecycleReadyTimeout)
	defer cancel()
	ctx, _ = credential.WithS3InternalAdminAuth(ctx)
	_, err := c.LifecycleReady(ctx, &s3_lifecycle_pb.LifecycleReadyRequest{})
	if status.Code(err) == codes.Unimplemented {
		// Older fronts authorize an empty delete, then reject it before any lookup.
		_, err = c.LifecycleDelete(ctx, &s3_lifecycle_pb.LifecycleDeleteRequest{})
	}
	return err
}

// selectS3Front returns a connection to the first front that passes
// checkLifecycleReady, plus why each earlier front was skipped. A nil
// connection means none passed.
func (h *Handler) selectS3Front(ctx context.Context, endpoints []string) (*grpc.ClientConn, string, []error) {
	var skipped []error
	for _, addr := range endpoints {
		if err := ctx.Err(); err != nil {
			return nil, "", append(skipped, err)
		}
		dialCtx, dialCancel := context.WithTimeout(ctx, 30*time.Second)
		conn, err := pb.GrpcDial(dialCtx, addr, false, h.grpcDialOption)
		dialCancel()
		if err != nil {
			skipped = append(skipped, fmt.Errorf("dial s3 %s: %w", addr, err))
			continue
		}
		if err := checkLifecycleReady(ctx, s3_lifecycle_pb.NewSeaweedS3LifecycleInternalClient(conn)); err != nil {
			conn.Close()
			skipped = append(skipped, readinessError(addr, err))
			continue
		}
		return conn, addr, skipped
	}
	return nil, "", skipped
}

func readinessError(addr string, err error) error {
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied:
		workerKey := "is not set"
		if util.GetViper().GetString("jwt.filer_signing.key") != "" {
			workerKey = "is set"
		}
		return fmt.Errorf("s3 %s rejected lifecycle credentials: %w; this worker's jwt.filer_signing.key %s, and it must match the key in the s3 server's security.toml (an s3 server without the key accepts only local callers)", addr, err, workerKey)
	}
	return fmt.Errorf("s3 %s readiness check: %w", addr, err)
}

func noReadyS3Error(skipped []error) error {
	return fmt.Errorf("no s3 server is ready for lifecycle deletes: %w", errors.Join(skipped...))
}
