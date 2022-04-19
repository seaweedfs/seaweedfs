package weed_server

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/cluster"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"time"
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
		pingErr = pb.WithFilerClient(false, pb.ServerAddress(req.Target), fs.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
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
		pingErr = pb.WithMasterClient(false, pb.ServerAddress(req.Target), fs.grpcDialOption, func(client master_pb.SeaweedClient) error {
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

	clusterId, _ := fs.filer.Store.KvGet(context.Background(), []byte("clusterId"))

	t := &filer_pb.GetFilerConfigurationResponse{
		Masters:            pb.ToAddressStringsFromMap(fs.option.Masters),
		Collection:         fs.option.Collection,
		Replication:        fs.option.DefaultReplication,
		MaxMb:              uint32(fs.option.MaxMB),
		DirBuckets:         fs.filer.DirBucketsPath,
		Cipher:             fs.filer.Cipher,
		Signature:          fs.filer.Signature,
		MetricsAddress:     fs.metricsAddress,
		MetricsIntervalSec: int32(fs.metricsIntervalSec),
		Version:            util.Version(),
		ClusterId:          string(clusterId),
	}

	glog.V(4).Infof("GetFilerConfiguration: %v", t)

	return t, nil
}

func (fs *FilerServer) KeepConnected(stream filer_pb.SeaweedFiler_KeepConnectedServer) error {

	req, err := stream.Recv()
	if err != nil {
		return err
	}

	clientName := util.JoinHostPort(req.Name, int(req.GrpcPort))
	m := make(map[string]bool)
	for _, tp := range req.Resources {
		m[tp] = true
	}
	fs.brokersLock.Lock()
	fs.brokers[clientName] = m
	glog.V(0).Infof("+ broker %v", clientName)
	fs.brokersLock.Unlock()

	defer func() {
		fs.brokersLock.Lock()
		delete(fs.brokers, clientName)
		glog.V(0).Infof("- broker %v: %v", clientName, err)
		fs.brokersLock.Unlock()
	}()

	for {
		if err := stream.Send(&filer_pb.KeepConnectedResponse{}); err != nil {
			glog.V(0).Infof("send broker %v: %+v", clientName, err)
			return err
		}
		// println("replied")

		if _, err := stream.Recv(); err != nil {
			glog.V(0).Infof("recv broker %v: %v", clientName, err)
			return err
		}
		// println("received")
	}

}

func (fs *FilerServer) LocateBroker(ctx context.Context, req *filer_pb.LocateBrokerRequest) (resp *filer_pb.LocateBrokerResponse, err error) {

	resp = &filer_pb.LocateBrokerResponse{}

	fs.brokersLock.Lock()
	defer fs.brokersLock.Unlock()

	var localBrokers []*filer_pb.LocateBrokerResponse_Resource

	for b, m := range fs.brokers {
		if _, found := m[req.Resource]; found {
			resp.Found = true
			resp.Resources = []*filer_pb.LocateBrokerResponse_Resource{
				{
					GrpcAddresses: b,
					ResourceCount: int32(len(m)),
				},
			}
			return
		}
		localBrokers = append(localBrokers, &filer_pb.LocateBrokerResponse_Resource{
			GrpcAddresses: b,
			ResourceCount: int32(len(m)),
		})
	}

	resp.Resources = localBrokers

	return resp, nil

}
