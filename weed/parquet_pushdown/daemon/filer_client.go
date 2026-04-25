package daemon

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// filerClient is a thin handle around the daemon's filer endpoints.
// M0 only uses it to confirm filer reachability at startup; later
// milestones add read paths for Parquet data and side-index blobs.
type filerClient struct {
	addresses      []pb.ServerAddress
	grpcDialOption grpc.DialOption
}

func newFilerClient(addresses []pb.ServerAddress, grpcDialOption grpc.DialOption) *filerClient {
	return &filerClient{addresses: addresses, grpcDialOption: grpcDialOption}
}

// waitUntilReachable blocks until the daemon can complete a
// GetFilerConfiguration RPC against any of its configured filer
// addresses, or the context is cancelled. Mirrors the iam command's
// startup-time filer probe.
func (c *filerClient) waitUntilReachable(ctx context.Context) error {
	for {
		err := pb.WithOneOfGrpcFilerClients(false, c.addresses, c.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			_, getErr := client.GetFilerConfiguration(ctx, &filer_pb.GetFilerConfigurationRequest{})
			return getErr
		})
		if err == nil {
			glog.V(0).Infof("pushdown daemon connected to filers %v", c.addresses)
			return nil
		}
		glog.V(0).Infof("pushdown daemon waiting for filers %v: %v", c.addresses, err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("filer reachability wait cancelled: %w", ctx.Err())
		case <-time.After(time.Second):
		}
	}
}
