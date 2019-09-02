package weed_server

import (
	"fmt"
	"net"
	"time"

	"github.com/joeslay/seaweedfs/weed/security"
	"github.com/joeslay/seaweedfs/weed/storage/erasure_coding"
	"github.com/spf13/viper"
	"google.golang.org/grpc"

	"github.com/joeslay/seaweedfs/weed/glog"
	"github.com/joeslay/seaweedfs/weed/pb/master_pb"
	"github.com/joeslay/seaweedfs/weed/util"
	"golang.org/x/net/context"
)

func (vs *VolumeServer) GetMaster() string {
	return vs.currentMaster
}
func (vs *VolumeServer) heartbeat() {

	glog.V(0).Infof("Volume server start with seed master nodes: %v", vs.SeedMasterNodes)
	vs.store.SetDataCenter(vs.dataCenter)
	vs.store.SetRack(vs.rack)

	grpcDialOption := security.LoadClientTLS(viper.Sub("grpc"), "volume")

	var err error
	var newLeader string
	for {
		for _, master := range vs.SeedMasterNodes {
			if newLeader != "" {
				master = newLeader
			}
			masterGrpcAddress, parseErr := util.ParseServerToGrpcAddress(master)
			if parseErr != nil {
				glog.V(0).Infof("failed to parse master grpc %v: %v", masterGrpcAddress, parseErr)
				continue
			}
			vs.store.MasterAddress = master
			newLeader, err = vs.doHeartbeat(context.Background(), master, masterGrpcAddress, grpcDialOption, time.Duration(vs.pulseSeconds)*time.Second)
			if err != nil {
				glog.V(0).Infof("heartbeat error: %v", err)
				time.Sleep(time.Duration(vs.pulseSeconds) * time.Second)
				newLeader = ""
				vs.store.MasterAddress = ""
			}
		}
	}
}

func (vs *VolumeServer) doHeartbeat(ctx context.Context, masterNode, masterGrpcAddress string, grpcDialOption grpc.DialOption, sleepInterval time.Duration) (newLeader string, err error) {

	grpcConection, err := util.GrpcDial(ctx, masterGrpcAddress, grpcDialOption)
	if err != nil {
		return "", fmt.Errorf("fail to dial %s : %v", masterNode, err)
	}
	defer grpcConection.Close()

	client := master_pb.NewSeaweedClient(grpcConection)
	stream, err := client.SendHeartbeat(ctx)
	if err != nil {
		glog.V(0).Infof("SendHeartbeat to %s: %v", masterNode, err)
		return "", err
	}
	glog.V(0).Infof("Heartbeat to: %v", masterNode)
	vs.currentMaster = masterNode

	doneChan := make(chan error, 1)

	go func() {
		for {
			in, err := stream.Recv()
			if err != nil {
				doneChan <- err
				return
			}
			if in.GetVolumeSizeLimit() != 0 {
				vs.store.SetVolumeSizeLimit(in.GetVolumeSizeLimit())
			}
			if in.GetLeader() != "" && masterNode != in.GetLeader() && !isSameIP(in.GetLeader(), masterNode) {
				glog.V(0).Infof("Volume Server found a new master newLeader: %v instead of %v", in.GetLeader(), masterNode)
				newLeader = in.GetLeader()
				doneChan <- nil
				return
			}
			if in.GetMetricsAddress() != "" && vs.MetricsAddress != in.GetMetricsAddress() {
				vs.MetricsAddress = in.GetMetricsAddress()
				vs.MetricsIntervalSec = int(in.GetMetricsIntervalSeconds())
			}
		}
	}()

	if err = stream.Send(vs.store.CollectHeartbeat()); err != nil {
		glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterNode, err)
		return "", err
	}

	if err = stream.Send(vs.store.CollectErasureCodingHeartbeat()); err != nil {
		glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterNode, err)
		return "", err
	}

	volumeTickChan := time.Tick(sleepInterval)
	ecShardTickChan := time.Tick(17 * sleepInterval)

	for {
		select {
		case volumeMessage := <-vs.store.NewVolumesChan:
			deltaBeat := &master_pb.Heartbeat{
				NewVolumes: []*master_pb.VolumeShortInformationMessage{
					&volumeMessage,
				},
			}
			glog.V(1).Infof("volume server %s:%d adds volume %d", vs.store.Ip, vs.store.Port, volumeMessage.Id)
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update to master %s: %v", masterNode, err)
				return "", err
			}
		case ecShardMessage := <-vs.store.NewEcShardsChan:
			deltaBeat := &master_pb.Heartbeat{
				NewEcShards: []*master_pb.VolumeEcShardInformationMessage{
					&ecShardMessage,
				},
			}
			glog.V(1).Infof("volume server %s:%d adds ec shard %d:%d", vs.store.Ip, vs.store.Port, ecShardMessage.Id,
				erasure_coding.ShardBits(ecShardMessage.EcIndexBits).ShardIds())
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update to master %s: %v", masterNode, err)
				return "", err
			}
		case volumeMessage := <-vs.store.DeletedVolumesChan:
			deltaBeat := &master_pb.Heartbeat{
				DeletedVolumes: []*master_pb.VolumeShortInformationMessage{
					&volumeMessage,
				},
			}
			glog.V(1).Infof("volume server %s:%d deletes volume %d", vs.store.Ip, vs.store.Port, volumeMessage.Id)
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update to master %s: %v", masterNode, err)
				return "", err
			}
		case ecShardMessage := <-vs.store.DeletedEcShardsChan:
			deltaBeat := &master_pb.Heartbeat{
				DeletedEcShards: []*master_pb.VolumeEcShardInformationMessage{
					&ecShardMessage,
				},
			}
			glog.V(1).Infof("volume server %s:%d deletes ec shard %d:%d", vs.store.Ip, vs.store.Port, ecShardMessage.Id,
				erasure_coding.ShardBits(ecShardMessage.EcIndexBits).ShardIds())
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update to master %s: %v", masterNode, err)
				return "", err
			}
		case <-volumeTickChan:
			glog.V(4).Infof("volume server %s:%d heartbeat", vs.store.Ip, vs.store.Port)
			if err = stream.Send(vs.store.CollectHeartbeat()); err != nil {
				glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterNode, err)
				return "", err
			}
		case <-ecShardTickChan:
			glog.V(4).Infof("volume server %s:%d ec heartbeat", vs.store.Ip, vs.store.Port)
			if err = stream.Send(vs.store.CollectErasureCodingHeartbeat()); err != nil {
				glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterNode, err)
				return "", err
			}
		case err = <-doneChan:
			return
		}
	}
}

func isSameIP(ip string, host string) bool {
	ips, err := net.LookupIP(host)
	if err != nil {
		return false
	}
	for _, t := range ips {
		if ip == t.String() {
			return true
		}
	}
	return false
}
