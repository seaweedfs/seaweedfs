package wdclient

import (
	"context"
	"math/rand"
	"time"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
)

type MasterClient struct {
	clientType     string
	clientHost     string
	grpcPort       uint32
	currentMaster  string
	masters        []string
	grpcDialOption grpc.DialOption

	vidMap
}

func NewMasterClient(grpcDialOption grpc.DialOption, clientType string, clientHost string, clientGrpcPort uint32, masters []string) *MasterClient {
	return &MasterClient{
		clientType:     clientType,
		clientHost:     clientHost,
		grpcPort:       clientGrpcPort,
		masters:        masters,
		grpcDialOption: grpcDialOption,
		vidMap:         newVidMap(),
	}
}

func (mc *MasterClient) GetMaster() string {
	return mc.currentMaster
}

func (mc *MasterClient) WaitUntilConnected() {
	for mc.currentMaster == "" {
		time.Sleep(time.Duration(rand.Int31n(200)) * time.Millisecond)
	}
}

func (mc *MasterClient) KeepConnectedToMaster() {
	glog.V(1).Infof("%s bootstraps with masters %v", mc.clientType, mc.masters)
	for {
		mc.tryAllMasters()
		time.Sleep(time.Second)
	}
}

func (mc *MasterClient) tryAllMasters() {
	nextHintedLeader := ""
	for _, master := range mc.masters {

		nextHintedLeader = mc.tryConnectToMaster(master)
		for nextHintedLeader != "" {
			nextHintedLeader = mc.tryConnectToMaster(nextHintedLeader)
		}

		mc.currentMaster = ""
		mc.vidMap = newVidMap()
	}
}

func (mc *MasterClient) tryConnectToMaster(master string) (nextHintedLeader string) {
	glog.V(1).Infof("%s Connecting to master %v", mc.clientType, master)
	gprcErr := pb.WithMasterClient(master, mc.grpcDialOption, func(client master_pb.SeaweedClient) error {

		stream, err := client.KeepConnected(context.Background())
		if err != nil {
			glog.V(0).Infof("%s failed to keep connected to %s: %v", mc.clientType, master, err)
			return err
		}

		if err = stream.Send(&master_pb.KeepConnectedRequest{Name: mc.clientType, GrpcPort: mc.grpcPort}); err != nil {
			glog.V(0).Infof("%s failed to send to %s: %v", mc.clientType, master, err)
			return err
		}

		glog.V(1).Infof("%s Connected to %v", mc.clientType, master)
		mc.currentMaster = master

		for {
			volumeLocation, err := stream.Recv()
			if err != nil {
				glog.V(0).Infof("%s failed to receive from %s: %v", mc.clientType, master, err)
				return err
			}

			// maybe the leader is changed
			if volumeLocation.Leader != "" {
				glog.V(0).Infof("redirected to leader %v", volumeLocation.Leader)
				nextHintedLeader = volumeLocation.Leader
				return nil
			}

			// process new volume location
			loc := Location{
				Url:       volumeLocation.Url,
				PublicUrl: volumeLocation.PublicUrl,
			}
			for _, newVid := range volumeLocation.NewVids {
				glog.V(1).Infof("%s: %s adds volume %d", mc.clientType, loc.Url, newVid)
				mc.addLocation(newVid, loc)
			}
			for _, deletedVid := range volumeLocation.DeletedVids {
				glog.V(1).Infof("%s: %s removes volume %d", mc.clientType, loc.Url, deletedVid)
				mc.deleteLocation(deletedVid, loc)
			}
		}

	})
	if gprcErr != nil {
		glog.V(0).Infof("%s failed to connect with master %v: %v", mc.clientType, master, gprcErr)
	}
	return
}

func (mc *MasterClient) WithClient(fn func(client master_pb.SeaweedClient) error) error {
	for mc.currentMaster == "" {
		time.Sleep(3 * time.Second)
	}
	return pb.WithMasterClient(mc.currentMaster, mc.grpcDialOption, func(client master_pb.SeaweedClient) error {
		return fn(client)
	})
}
