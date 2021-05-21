package wdclient

import (
	"context"
	"math/rand"
	"time"

	"github.com/chrislusf/seaweedfs/weed/util"
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

func NewMasterClient(grpcDialOption grpc.DialOption, clientType string, clientHost string, clientGrpcPort uint32, clientDataCenter string, masters []string) *MasterClient {
	return &MasterClient{
		clientType:     clientType,
		clientHost:     clientHost,
		grpcPort:       clientGrpcPort,
		masters:        masters,
		grpcDialOption: grpcDialOption,
		vidMap:         newVidMap(clientDataCenter),
	}
}

func (mc *MasterClient) GetMaster() string {
	mc.WaitUntilConnected()
	return mc.currentMaster
}

func (mc *MasterClient) WaitUntilConnected() {
	for mc.currentMaster == "" {
		time.Sleep(time.Duration(rand.Int31n(200)) * time.Millisecond)
	}
}

func (mc *MasterClient) KeepConnectedToMaster() {
	glog.V(1).Infof("%s masterClient bootstraps with masters %v", mc.clientType, mc.masters)
	for {
		mc.tryAllMasters()
		time.Sleep(time.Second)
	}
}

func (mc *MasterClient) FindLeaderFromOtherPeers(myMasterAddress string) (leader string) {
	for _, master := range mc.masters {
		if master == myMasterAddress {
			continue
		}
		if grpcErr := pb.WithMasterClient(master, mc.grpcDialOption, func(client master_pb.SeaweedClient) error {
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
			defer cancel()
			resp, err := client.GetMasterConfiguration(ctx, &master_pb.GetMasterConfigurationRequest{})
			if err != nil {
				return err
			}
			leader = resp.Leader
			return nil
		}); grpcErr != nil {
			glog.V(0).Infof("connect to %s: %v", master, grpcErr)
		}
		if leader != "" {
			glog.V(0).Infof("existing leader is %s", leader)
			return
		}
	}
	glog.V(0).Infof("No existing leader found!")
	return
}

func (mc *MasterClient) tryAllMasters() {
	nextHintedLeader := ""
	for _, master := range mc.masters {

		nextHintedLeader = mc.tryConnectToMaster(master)
		for nextHintedLeader != "" {
			nextHintedLeader = mc.tryConnectToMaster(nextHintedLeader)
		}

		mc.currentMaster = ""
		mc.vidMap = newVidMap("")
	}
}

func (mc *MasterClient) tryConnectToMaster(master string) (nextHintedLeader string) {
	glog.V(1).Infof("%s masterClient Connecting to master %v", mc.clientType, master)
	gprcErr := pb.WithMasterClient(master, mc.grpcDialOption, func(client master_pb.SeaweedClient) error {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stream, err := client.KeepConnected(ctx)
		if err != nil {
			glog.V(1).Infof("%s masterClient failed to keep connected to %s: %v", mc.clientType, master, err)
			return err
		}

		if err = stream.Send(&master_pb.KeepConnectedRequest{Name: mc.clientType, GrpcPort: mc.grpcPort}); err != nil {
			glog.V(0).Infof("%s masterClient failed to send to %s: %v", mc.clientType, master, err)
			return err
		}

		glog.V(1).Infof("%s masterClient Connected to %v", mc.clientType, master)
		mc.currentMaster = master

		for {
			volumeLocation, err := stream.Recv()
			if err != nil {
				glog.V(0).Infof("%s masterClient failed to receive from %s: %v", mc.clientType, master, err)
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
				Url:        volumeLocation.Url,
				PublicUrl:  volumeLocation.PublicUrl,
				DataCenter: volumeLocation.DataCenter,
			}
			for _, newVid := range volumeLocation.NewVids {
				glog.V(1).Infof("%s: %s masterClient adds volume %d", mc.clientType, loc.Url, newVid)
				mc.addLocation(newVid, loc)
			}
			for _, deletedVid := range volumeLocation.DeletedVids {
				glog.V(1).Infof("%s: %s masterClient removes volume %d", mc.clientType, loc.Url, deletedVid)
				mc.deleteLocation(deletedVid, loc)
			}
		}

	})
	if gprcErr != nil {
		glog.V(1).Infof("%s masterClient failed to connect with master %v: %v", mc.clientType, master, gprcErr)
	}
	return
}

func (mc *MasterClient) WithClient(fn func(client master_pb.SeaweedClient) error) error {
	return util.Retry("master grpc", func() error {
		for mc.currentMaster == "" {
			time.Sleep(3 * time.Second)
		}
		return pb.WithMasterClient(mc.currentMaster, mc.grpcDialOption, func(client master_pb.SeaweedClient) error {
			return fn(client)
		})
	})
}
