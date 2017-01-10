package weed_server

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/topology"
)

func (ms MasterServer) SendHeartbeat(stream pb.Seaweed_SendHeartbeatServer) error {
	var dn *topology.DataNode
	t := ms.Topo
	for {
		heartbeat, err := stream.Recv()
		if err == nil {
			if dn == nil {
				t.Sequence.SetMax(heartbeat.MaxFileKey)
				dcName, rackName := t.Configuration.Locate(heartbeat.Ip, heartbeat.DataCenter, heartbeat.Rack)
				dc := t.GetOrCreateDataCenter(dcName)
				rack := dc.GetOrCreateRack(rackName)
				dn = rack.GetOrCreateDataNode(heartbeat.Ip,
					int(heartbeat.Port), heartbeat.PublicUrl,
					int(heartbeat.MaxVolumeCount))
				glog.V(0).Infof("added volume server %v:%d", heartbeat.GetIp(), heartbeat.GetPort())
				if err := stream.Send(&pb.HeartbeatResponse{
					VolumeSizeLimit: uint64(ms.volumeSizeLimitMB) * 1024 * 1024,
					SecretKey:       string(ms.guard.SecretKey),
				}); err != nil {
					return err
				}
			}

			var volumeInfos []storage.VolumeInfo
			for _, v := range heartbeat.Volumes {
				if vi, err := storage.NewVolumeInfo(v); err == nil {
					volumeInfos = append(volumeInfos, vi)
				} else {
					glog.V(0).Infof("Fail to convert joined volume information: %v", err)
				}
			}
			deletedVolumes := dn.UpdateVolumes(volumeInfos)
			for _, v := range volumeInfos {
				t.RegisterVolumeLayout(v, dn)
			}
			for _, v := range deletedVolumes {
				t.UnRegisterVolumeLayout(v, dn)
			}

		} else {
			glog.V(0).Infof("lost volume server %s:%d", dn.Ip, dn.Port)
			if dn != nil {
				t.UnRegisterDataNode(dn)
			}
			return err
		}
	}
}
