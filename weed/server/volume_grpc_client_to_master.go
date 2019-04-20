package weed_server

import (
	"fmt"
	"time"

	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/spf13/viper"
	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"golang.org/x/net/context"
)

func (vs *VolumeServer) GetMaster() string {
	return vs.currentMaster
}
func (vs *VolumeServer) heartbeat() {

	glog.V(0).Infof("Volume server start with masters: %v", vs.MasterNodes)
	vs.store.SetDataCenter(vs.dataCenter)
	vs.store.SetRack(vs.rack)

	grpcDialOption := security.LoadClientTLS(viper.Sub("grpc"), "volume")

	var err error
	var newLeader string
	for {
		for _, master := range vs.MasterNodes {
			if newLeader != "" {
				master = newLeader
			}
			masterGrpcAddress, parseErr := util.ParseServerToGrpcAddress(master)
			if parseErr != nil {
				glog.V(0).Infof("failed to parse master grpc %v: %v", masterGrpcAddress, parseErr)
				continue
			}
			newLeader, err = vs.doHeartbeat(context.Background(), master, masterGrpcAddress, grpcDialOption, time.Duration(vs.pulseSeconds)*time.Second)
			if err != nil {
				glog.V(0).Infof("heartbeat error: %v", err)
				time.Sleep(time.Duration(vs.pulseSeconds) * time.Second)
				newLeader = ""
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

	vs.store.Client = stream
	defer func() { vs.store.Client = nil }()

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
			if in.GetLeader() != "" && masterNode != in.GetLeader() {
				glog.V(0).Infof("Volume Server found a new master newLeader: %v instead of %v", in.GetLeader(), masterNode)
				newLeader = in.GetLeader()
				doneChan <- nil
				return
			}
		}
	}()

	if err = stream.Send(vs.store.CollectHeartbeat()); err != nil {
		glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterNode, err)
		return "", err
	}

	tickChan := time.Tick(sleepInterval)

	for {
		select {
		case volumeMessage := <-vs.store.NewVolumesChan:
			deltaBeat := &master_pb.Heartbeat{
				NewVolumes:[]*master_pb.VolumeShortInformationMessage{
					&volumeMessage,
				},
			}
			glog.V(1).Infof("volume server %s:%d adds volume %d", vs.store.Ip, vs.store.Port, volumeMessage.Id)
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update to master %s: %v", masterNode, err)
				return "", err
			}
		case volumeMessage := <-vs.store.DeletedVolumesChan:
			deltaBeat := &master_pb.Heartbeat{
				DeletedVolumes:[]*master_pb.VolumeShortInformationMessage{
					&volumeMessage,
				},
			}
			glog.V(1).Infof("volume server %s:%d deletes volume %d", vs.store.Ip, vs.store.Port, volumeMessage.Id)
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update to master %s: %v", masterNode, err)
				return "", err
			}
		case <-tickChan:
			glog.V(1).Infof("volume server %s:%d heartbeat", vs.store.Ip, vs.store.Port)
			if err = stream.Send(vs.store.CollectHeartbeat()); err != nil {
				glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterNode, err)
				return "", err
			}
		case err = <-doneChan:
			return
		}
	}
}
