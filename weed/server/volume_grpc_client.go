package weed_server

import (
	"fmt"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func (vs *VolumeServer) heartbeat() {

	glog.V(0).Infof("Volume server bootstraps with master %s", vs.GetMasterNode())
	vs.masterNodes = storage.NewMasterNodes(vs.masterNode)
	vs.store.SetDataCenter(vs.dataCenter)
	vs.store.SetRack(vs.rack)

	for {
		err := vs.doHeartbeat(time.Duration(vs.pulseSeconds) * time.Second)
		if err != nil {
			glog.V(0).Infof("heartbeat error: %v", err)
			time.Sleep(time.Duration(3*vs.pulseSeconds) * time.Second)
		}
	}
}

func (vs *VolumeServer) doHeartbeat(sleepInterval time.Duration) error {

	masterNode, err := vs.masterNodes.FindMaster()
	if err != nil {
		return fmt.Errorf("No master found: %v", err)
	}

	grpcConection, err := grpc.Dial(masterNode, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial: %v", err)
	}
	defer grpcConection.Close()

	client := pb.NewSeaweedClient(grpcConection)
	stream, err := client.SendHeartbeat(context.Background())
	if err != nil {
		glog.V(0).Infof("%v.SendHeartbeat(_) = _, %v", client, err)
		return err
	}
	vs.SetMasterNode(masterNode)
	glog.V(0).Infof("Heartbeat to %s", masterNode)

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
			vs.store.VolumeSizeLimit = in.GetVolumeSizeLimit()
			vs.guard.SecretKey = security.Secret(in.GetSecretKey())
		}
	}()

	if err = stream.Send(vs.store.CollectHeartbeat()); err != nil {
		glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterNode, err)
		return err
	}

	tickChan := time.NewTimer(sleepInterval).C

	for {
		select {
		case <-tickChan:
			if err = stream.Send(vs.store.CollectHeartbeat()); err != nil {
				glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterNode, err)
				return err
			}
		case err := <-doneChan:
			return err
		}
	}
}
