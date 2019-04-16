package wdclient

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/HZ89/seaweedfs/weed/glog"
	"github.com/HZ89/seaweedfs/weed/pb/master_pb"
	"github.com/HZ89/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

type MasterClient struct {
	ctx            context.Context
	name           string
	currentMaster  string
	masters        []string
	grpcDialOption grpc.DialOption

	vidMap
}

func NewMasterClient(ctx context.Context, grpcDialOption grpc.DialOption, clientName string, masters []string) *MasterClient {
	return &MasterClient{
		ctx:            ctx,
		name:           clientName,
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
	glog.V(1).Infof("%s bootstraps with masters %v", mc.name, mc.masters)
	for {
		mc.tryAllMasters()
		time.Sleep(time.Second)
	}
}

func (mc *MasterClient) tryAllMasters() {
	for _, master := range mc.masters {
		glog.V(1).Infof("Connecting to master %v", master)
		gprcErr := withMasterClient(context.Background(), master, mc.grpcDialOption, func(ctx context.Context, client master_pb.SeaweedClient) error {

			stream, err := client.KeepConnected(ctx)
			if err != nil {
				glog.V(0).Infof("failed to keep connected to %s: %v", master, err)
				return err
			}

			if err = stream.Send(&master_pb.ClientListenRequest{Name: mc.name}); err != nil {
				glog.V(0).Infof("failed to send to %s: %v", master, err)
				return err
			}

			if mc.currentMaster == "" {
				glog.V(1).Infof("Connected to %v", master)
				mc.currentMaster = master
			}

			for {
				if volumeLocation, err := stream.Recv(); err != nil {
					glog.V(0).Infof("failed to receive from %s: %v", master, err)
					return err
				} else {
					loc := Location{
						Url:       volumeLocation.Url,
						PublicUrl: volumeLocation.PublicUrl,
					}
					for _, newVid := range volumeLocation.NewVids {
						mc.addLocation(newVid, loc)
					}
					for _, deletedVid := range volumeLocation.DeletedVids {
						mc.deleteLocation(deletedVid, loc)
					}
				}
			}

		})

		if gprcErr != nil {
			glog.V(0).Infof("%s failed to connect with master %v: %v", mc.name, master, gprcErr)
		}

		mc.currentMaster = ""
	}
}

func withMasterClient(ctx context.Context, master string, grpcDialOption grpc.DialOption, fn func(ctx context.Context, client master_pb.SeaweedClient) error) error {

	masterGrpcAddress, parseErr := util.ParseServerToGrpcAddress(master)
	if parseErr != nil {
		return fmt.Errorf("failed to parse master grpc %v", master)
	}

	return util.WithCachedGrpcClient(ctx, func(grpcConnection *grpc.ClientConn) error {
		client := master_pb.NewSeaweedClient(grpcConnection)
		return fn(ctx, client)
	}, masterGrpcAddress, grpcDialOption)

}

func (mc *MasterClient) WithClient(ctx context.Context, fn func(client master_pb.SeaweedClient) error) error {
	return withMasterClient(ctx, mc.currentMaster, mc.grpcDialOption, func(ctx context.Context, client master_pb.SeaweedClient) error {
		return fn(client)
	})
}
