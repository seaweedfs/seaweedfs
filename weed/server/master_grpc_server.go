package weed_server

import (
	"net"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/topology"
	"google.golang.org/grpc/peer"
)

func (ms *MasterServer) SendHeartbeat(stream master_pb.Seaweed_SendHeartbeatServer) error {
	var dn *topology.DataNode
	t := ms.Topo

	defer func() {
		if dn != nil {
			glog.V(0).Infof("unregister disconnected volume server %s:%d", dn.Ip, dn.Port)
			t.UnRegisterDataNode(dn)
		}
	}()

	for {
		heartbeat, err := stream.Recv()
		if err == nil {
			if dn == nil {
				t.Sequence.SetMax(heartbeat.MaxFileKey)
				if heartbeat.Ip == "" {
					if pr, ok := peer.FromContext(stream.Context()); ok {
						if pr.Addr != net.Addr(nil) {
							heartbeat.Ip = pr.Addr.String()[0:strings.LastIndex(pr.Addr.String(), ":")]
							glog.V(0).Infof("remote IP address is detected as %v", heartbeat.Ip)
						}
					}
				}
				dcName, rackName := t.Configuration.Locate(heartbeat.Ip, heartbeat.DataCenter, heartbeat.Rack)
				dc := t.GetOrCreateDataCenter(dcName)
				rack := dc.GetOrCreateRack(rackName)
				dn = rack.GetOrCreateDataNode(heartbeat.Ip,
					int(heartbeat.Port), heartbeat.PublicUrl,
					int(heartbeat.MaxVolumeCount))
				glog.V(0).Infof("added volume server %v:%d", heartbeat.GetIp(), heartbeat.GetPort())
				if err := stream.Send(&master_pb.HeartbeatResponse{
					VolumeSizeLimit: uint64(ms.volumeSizeLimitMB) * 1024 * 1024,
					SecretKey:       string(ms.guard.SecretKey),
				}); err != nil {
					return err
				}
			}

			t.SyncDataNodeRegistration(heartbeat.Volumes, dn)

		} else {
			return err
		}

		// tell the volume servers about the leader
		newLeader, err := t.Leader()
		if err == nil {
			if err := stream.Send(&master_pb.HeartbeatResponse{
				Leader: newLeader,
			}); err != nil {
				return err
			}
		}
	}
}

// KeepConnected keep a stream gRPC call to the master. Used by filer to know the master is up.
func (ms *MasterServer) KeepConnected(stream master_pb.Seaweed_KeepConnectedServer) error {
	for {
		_, err := stream.Recv()
		if err != nil {
			return err
		}
		if err := stream.Send(&master_pb.Empty{}); err != nil {
			return err
		}
	}
}
