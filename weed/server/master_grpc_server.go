package weed_server

import (
	"net"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/topology"
	"google.golang.org/grpc/peer"
	"fmt"
)

func (ms *MasterServer) SendHeartbeat(stream master_pb.Seaweed_SendHeartbeatServer) error {
	var dn *topology.DataNode
	t := ms.Topo

	defer func() {
		if dn != nil {

			glog.V(0).Infof("unregister disconnected volume server %s:%d", dn.Ip, dn.Port)
			t.UnRegisterDataNode(dn)

			message := &master_pb.VolumeLocation{
				Url:       dn.Url(),
				PublicUrl: dn.PublicUrl,
			}
			for _, v := range dn.GetVolumes() {
				message.DeletedVids = append(message.DeletedVids, uint32(v.Id))
			}

			if len(message.DeletedVids) > 0 {
				ms.clientChansLock.RLock()
				for _, ch := range ms.clientChans {
					ch <- message
				}
				ms.clientChansLock.RUnlock()
			}

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

			newVolumes, deletedVolumes := t.SyncDataNodeRegistration(heartbeat.Volumes, dn)

			message := &master_pb.VolumeLocation{
				Url:       dn.Url(),
				PublicUrl: dn.PublicUrl,
			}
			for _, v := range newVolumes {
				message.NewVids = append(message.NewVids, uint32(v.Id))
			}
			for _, v := range deletedVolumes {
				message.DeletedVids = append(message.DeletedVids, uint32(v.Id))
			}

			if len(message.NewVids) > 0 || len(message.DeletedVids) > 0 {
				ms.clientChansLock.RLock()
				for _, ch := range ms.clientChans {
					ch <- message
				}
				ms.clientChansLock.RUnlock()
			}

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

	req, err := stream.Recv()
	if err != nil {
		return err
	}

	// remember client address
	ctx := stream.Context()
	// fmt.Printf("FromContext %+v\n", ctx)
	pr, ok := peer.FromContext(ctx)
	if !ok {
		glog.Error("failed to get peer from ctx")
		return fmt.Errorf("failed to get peer from ctx")
	}
	if pr.Addr == net.Addr(nil) {
		glog.Error("failed to get peer address")
		return fmt.Errorf("failed to get peer address")
	}

	clientName := req.Name + pr.Addr.String()
	glog.V(0).Infof("+ client %v", clientName)

	messageChan := make(chan *master_pb.VolumeLocation)
	stopChan := make(chan bool)

	ms.clientChansLock.Lock()
	ms.clientChans[clientName] = messageChan
	ms.clientChansLock.Unlock()

	defer func() {
		glog.V(0).Infof("- client %v", clientName)
		ms.clientChansLock.Lock()
		delete(ms.clientChans, clientName)
		ms.clientChansLock.Unlock()
	}()

	for _, message := range ms.Topo.ToVolumeLocations() {
		if err := stream.Send(message); err != nil {
			return err
		}
	}

	go func() {
		for {
			_, err := stream.Recv()
			if err != nil {
				glog.V(2).Infof("- client %v: %v", clientName, err)
				stopChan <- true
				break
			}
		}
	}()

	for {
		select {
		case message := <-messageChan:
			if err := stream.Send(message); err != nil {
				return err
			}
		case <-stopChan:
			return nil
		}
	}

	return nil
}
