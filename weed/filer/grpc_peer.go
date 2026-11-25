package filer

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
)

func (f *Filer) peerCallContext(ctx context.Context) context.Context {
	if f != nil && f.PeerContext != nil {
		return f.PeerContext(ctx)
	}
	return ctx
}

func (f *Filer) peerSubscriptionContext(ctx context.Context) context.Context {
	if f != nil && f.PeerContext != nil {
		return f.PeerContext(ctx)
	}
	return security.WithMetadataSubscriptionCredentials(ctx, nil)
}

// A metadata stream can fetch many log chunks over its lifetime. Resolve the
// credential for each volume lookup, rather than retaining the stream's token.
type peerLookupClient struct {
	filer_pb.SeaweedFilerClient
	peerContext func(context.Context) context.Context
}

func (c peerLookupClient) LookupVolume(ctx context.Context, req *filer_pb.LookupVolumeRequest, opts ...grpc.CallOption) (*filer_pb.LookupVolumeResponse, error) {
	return c.SeaweedFilerClient.LookupVolume(c.peerContext(ctx), req, opts...)
}
