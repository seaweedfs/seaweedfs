package weed_server

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
)

func (fs *FilerServer) Statistics(ctx context.Context, req *filer_pb.StatisticsRequest) (resp *filer_pb.StatisticsResponse, err error) {

	var output *master_pb.StatisticsResponse

	err = fs.filer.MasterClient.WithClient(false, func(masterClient master_pb.SeaweedClient) error {
		grpcResponse, grpcErr := masterClient.Statistics(context.Background(), &master_pb.StatisticsRequest{
			Replication: req.Replication,
			Collection:  req.Collection,
			Ttl:         req.Ttl,
			DiskType:    req.DiskType,
		})
		if grpcErr != nil {
			return grpcErr
		}

		output = grpcResponse
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &filer_pb.StatisticsResponse{
		TotalSize: output.TotalSize,
		UsedSize:  output.UsedSize,
		FileCount: output.FileCount,
	}, nil
}

// isKnownPingTarget reports whether target is a peer the filer has learned
// about from its master subscription (other filers, volume servers) or from
// its own master list. Restricting Ping prevents the RPC from being used as
// an arbitrary outbound dialer. All lookups are O(1) so the gate adds no
// noticeable overhead even in large clusters.
func (fs *FilerServer) isKnownPingTarget(ctx context.Context, target string, targetType string) bool {
	addr := pb.ServerAddress(target)
	switch targetType {
	case cluster.FilerType:
		if fs.filer != nil && fs.filer.MetaAggregator != nil && fs.filer.MetaAggregator.HasPeer(addr) {
			return true
		}
		return false
	case cluster.VolumeServerType:
		if fs.filer != nil && fs.filer.MasterClient != nil {
			return fs.filer.MasterClient.HasVolumeServer(addr)
		}
		return false
	case cluster.MasterType:
		key := addr.ToHttpAddress()
		if fs.option != nil && fs.option.Masters != nil {
			if _, ok := fs.option.Masters.GetInstancesAsMap()[string(addr)]; ok {
				return true
			}
			// Fall back to a port-tolerant compare for callers that supply
			// the http form when masters were registered with grpc suffix.
			for _, master := range fs.option.Masters.GetInstances() {
				if master.ToHttpAddress() == key {
					return true
				}
			}
		}
		if fs.filer != nil && fs.filer.MasterClient != nil {
			if _, ok := fs.filer.MasterClient.ListMasterSet()[key]; ok {
				return true
			}
		}
		return false
	}
	return false
}

func (fs *FilerServer) Ping(ctx context.Context, req *filer_pb.PingRequest) (resp *filer_pb.PingResponse, pingErr error) {
	resp = &filer_pb.PingResponse{
		StartTimeNs: time.Now().UnixNano(),
	}
	// Empty target is a self-liveness probe and stays unauthenticated.
	if req.Target != "" && !fs.isKnownPingTarget(ctx, req.Target, req.TargetType) {
		resp.StopTimeNs = time.Now().UnixNano()
		return resp, status.Errorf(codes.InvalidArgument, "unknown ping target %s of type %s", req.Target, req.TargetType)
	}
	if req.TargetType == cluster.FilerType {
		pingErr = pb.WithFilerClient(false, 0, pb.ServerAddress(req.Target), fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			pingResp, err := client.Ping(ctx, &filer_pb.PingRequest{})
			if pingResp != nil {
				resp.RemoteTimeNs = pingResp.StartTimeNs
			}
			return err
		})
	}
	if req.TargetType == cluster.VolumeServerType {
		pingErr = pb.WithVolumeServerClient(false, pb.ServerAddress(req.Target), fs.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			pingResp, err := client.Ping(ctx, &volume_server_pb.PingRequest{})
			if pingResp != nil {
				resp.RemoteTimeNs = pingResp.StartTimeNs
			}
			return err
		})
	}
	if req.TargetType == cluster.MasterType {
		pingErr = pb.WithMasterClient(context.Background(), false, pb.ServerAddress(req.Target), fs.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
			pingResp, err := client.Ping(ctx, &master_pb.PingRequest{})
			if pingResp != nil {
				resp.RemoteTimeNs = pingResp.StartTimeNs
			}
			return err
		})
	}
	if pingErr != nil {
		pingErr = fmt.Errorf("ping %s %s: %v", req.TargetType, req.Target, pingErr)
	}
	resp.StopTimeNs = time.Now().UnixNano()
	return
}

func (fs *FilerServer) GetFilerConfiguration(ctx context.Context, req *filer_pb.GetFilerConfigurationRequest) (resp *filer_pb.GetFilerConfigurationResponse, err error) {

	t := &filer_pb.GetFilerConfigurationResponse{
		Masters:            fs.option.Masters.GetInstancesAsStrings(),
		Collection:         fs.option.Collection,
		Replication:        fs.option.DefaultReplication,
		MaxMb:              uint32(fs.option.MaxMB),
		DirBuckets:         fs.filer.DirBucketsPath,
		Cipher:             fs.filer.Cipher,
		Signature:          fs.filer.Signature,
		MetricsAddress:     fs.metricsAddress,
		MetricsIntervalSec: int32(fs.metricsIntervalSec),
		Version:            version.Version(),
		FilerGroup:         fs.option.FilerGroup,
		MajorVersion:       version.MAJOR_VERSION,
		MinorVersion:       version.MINOR_VERSION,
	}

	glog.V(4).InfofCtx(ctx, "GetFilerConfiguration: %v", t)

	return t, nil
}
