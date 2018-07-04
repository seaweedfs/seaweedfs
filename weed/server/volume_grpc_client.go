package weed_server

import (
	"fmt"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"golang.org/x/net/context"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (vs *VolumeServer) GetMaster() string {
	return vs.currentMaster
}
func (vs *VolumeServer) heartbeat() {

	glog.V(0).Infof("Volume server start with masters: %v", vs.MasterNodes)
	vs.store.SetDataCenter(vs.dataCenter)
	vs.store.SetRack(vs.rack)

	var err error
	var newLeader string
	for {
		for _, master := range vs.MasterNodes {
			if newLeader != "" {
				master = newLeader
			}
			newLeader, err = vs.doHeartbeat(master, time.Duration(vs.pulseSeconds)*time.Second)
			if err != nil {
				glog.V(0).Infof("heartbeat error: %v", err)
				time.Sleep(time.Duration(vs.pulseSeconds) * time.Second)
			}
		}
	}
}

func (vs *VolumeServer) doHeartbeat(masterNode string, sleepInterval time.Duration) (newLeader string, err error) {

	grpcConection, err := util.GrpcDial(masterNode)
	if err != nil {
		return "", fmt.Errorf("fail to dial: %v", err)
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
			if in.GetSecretKey() != "" {
				vs.guard.SecretKey = security.Secret(in.GetSecretKey())
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
		case <-tickChan:
			if err = stream.Send(vs.store.CollectHeartbeat()); err != nil {
				glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterNode, err)
				return "", err
			}
		case <-doneChan:
			return
		}
	}
}
