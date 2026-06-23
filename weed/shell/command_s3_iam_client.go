package shell

import (
	"context"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// iamRequestTimeout caps every shell-originated IAM gRPC call so the shell
// can't hang on an unresponsive filer.
const iamRequestTimeout = 30 * time.Second

// withIamClient invokes fn against the filer's IAM gRPC service. When
// jwt.filer_signing.key is configured in security.toml, a freshly minted admin
// Bearer token is attached to the outgoing context so the filer's
// IamGrpcServer.checkAdminAuth passes; with no key configured the filer
// accepts unauthenticated calls. The context already has the iamRequestTimeout
// applied — callers can derive child contexts but should not need their own
// timeout boilerplate.
func (ce *CommandEnv) withIamClient(fn func(ctx context.Context, client iam_pb.SeaweedIdentityAccessManagementClient) error) error {
	return pb.WithGrpcClient(context.Background(), false, 0, func(conn *grpc.ClientConn) error {
		ctx, cancel := context.WithTimeout(iamAdminAuthContext(context.Background()), iamRequestTimeout)
		defer cancel()
		return fn(ctx, iam_pb.NewSeaweedIdentityAccessManagementClient(conn))
	}, ce.option.FilerAddress.ToGrpcAddress(), false, ce.option.GrpcDialOption)
}

func iamAdminAuthContext(ctx context.Context) context.Context {
	signingKey := util.GetViper().GetString("jwt.filer_signing.key")
	if signingKey == "" {
		return ctx
	}
	expiresAfterSec := util.GetViper().GetInt("jwt.filer_signing.expires_after_seconds")
	token := security.GenJwtForFilerAdmin(security.SigningKey(signingKey), expiresAfterSec)
	if token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+string(token))
}
