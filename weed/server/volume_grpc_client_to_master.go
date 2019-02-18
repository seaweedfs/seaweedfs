package weed_server

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"time"

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
			masterGrpcAddress, parseErr := util.ParseServerToGrpcAddress(master, 0)
			if parseErr != nil {
				glog.V(0).Infof("failed to parse master grpc %v", masterGrpcAddress)
				continue
			}
			newLeader, err = vs.doHeartbeat(master, masterGrpcAddress, grpcDialOption, time.Duration(vs.pulseSeconds)*time.Second)
			if err != nil {
				glog.V(0).Infof("heartbeat error: %v", err)
				time.Sleep(time.Duration(vs.pulseSeconds) * time.Second)
			}
		}
	}
}

func (vs *VolumeServer) doHeartbeat(masterNode, masterGrpcAddress string, grpcDialOption grpc.DialOption, sleepInterval time.Duration) (newLeader string, err error) {

	grpcConection, err := util.GrpcDial(masterGrpcAddress, grpcDialOption)
	if err != nil {
		return "", fmt.Errorf("fail to dial %s : %v", masterNode, err)
	}
	defer grpcConection.Close()

	client := master_pb.NewSeaweedClient(grpcConection)
	stream, err := client.SendHeartbeat(context.Background())
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
				vs.store.VolumeSizeLimit = in.GetVolumeSizeLimit()
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
		case vid := <-vs.store.NewVolumeIdChan:
			deltaBeat := &master_pb.Heartbeat{
				NewVids: []uint32{uint32(vid)},
			}
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update to master %s: %v", masterNode, err)
				return "", err
			}
		case vid := <-vs.store.DeletedVolumeIdChan:
			deltaBeat := &master_pb.Heartbeat{
				DeletedVids: []uint32{uint32(vid)},
			}
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update to master %s: %v", masterNode, err)
				return "", err
			}
		case <-tickChan:
			if err = stream.Send(vs.store.CollectHeartbeat()); err != nil {
				glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterNode, err)
				return "", err
			}
		case err = <-doneChan:
			return
		}
	}
}
