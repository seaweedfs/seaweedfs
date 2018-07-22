package filer2

import (
	"context"
	"fmt"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (fs *Filer) GetMaster() string {
	return fs.currentMaster
}

func (fs *Filer) KeepConnectedToMaster() {
	glog.V(0).Infof("Filer bootstraps with masters %v", fs.masters)
	for _, master := range fs.masters {
		glog.V(0).Infof("Connecting to %v", master)
		withMasterClient(master, func(client master_pb.SeaweedClient) error {
			stream, err := client.KeepConnected(context.Background())
			if err != nil {
				glog.V(0).Infof("failed to keep connected to %s: %v", master, err)
				return err
			}

			glog.V(0).Infof("Connected to %v", master)
			fs.currentMaster = master

			for {
				time.Sleep(time.Duration(float32(10*1e3)*0.25) * time.Millisecond)

				if err = stream.Send(&master_pb.Empty{}); err != nil {
					glog.V(0).Infof("failed to send to %s: %v", master, err)
					return err
				}

				if _, err = stream.Recv(); err != nil {
					glog.V(0).Infof("failed to receive from %s: %v", master, err)
					return err
				}
			}
		})
		fs.currentMaster = ""
	}
}

func withMasterClient(master string, fn func(client master_pb.SeaweedClient) error) error {

	grpcConnection, err := util.GrpcDial(master)
	if err != nil {
		return fmt.Errorf("fail to dial %s: %v", master, err)
	}
	defer grpcConnection.Close()

	client := master_pb.NewSeaweedClient(grpcConnection)

	return fn(client)
}
