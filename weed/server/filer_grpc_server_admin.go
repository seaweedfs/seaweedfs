package weed_server

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
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

func (fs *FilerServer) Ping(ctx context.Context, req *filer_pb.PingRequest) (resp *filer_pb.PingResponse, pingErr error) {
	resp = &filer_pb.PingResponse{
		StartTimeNs: time.Now().UnixNano(),
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
		pingErr = pb.WithMasterClient(false, pb.ServerAddress(req.Target), fs.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
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
		Version:            util.Version(),
		FilerGroup:         fs.option.FilerGroup,
		MajorVersion:       util.MAJOR_VERSION,
		MinorVersion:       util.MINOR_VERSION,
	}

	glog.V(4).Infof("GetFilerConfiguration: %v", t)

	return t, nil
}
