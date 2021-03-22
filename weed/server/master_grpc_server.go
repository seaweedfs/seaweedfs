package weed_server

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"net"
	"strings"
	"time"

	"github.com/chrislusf/raft"
	"google.golang.org/grpc/peer"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/topology"
)

func (ms *MasterServer) SendHeartbeat(stream master_pb.Seaweed_SendHeartbeatServer) error {
	var dn *topology.DataNode

	defer func() {
		if dn != nil {

			// if the volume server disconnects and reconnects quickly
			//  the unregister and register can race with each other
			ms.Topo.UnRegisterDataNode(dn)
			glog.V(0).Infof("unregister disconnected volume server %s:%d", dn.Ip, dn.Port)

			message := &master_pb.VolumeLocation{
				Url:       dn.Url(),
				PublicUrl: dn.PublicUrl,
			}
			for _, v := range dn.GetVolumes() {
				message.DeletedVids = append(message.DeletedVids, uint32(v.Id))
			}
			for _, s := range dn.GetEcShards() {
				message.DeletedVids = append(message.DeletedVids, uint32(s.VolumeId))
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
		if err != nil {
			if dn != nil {
				glog.Warningf("SendHeartbeat.Recv server %s:%d : %v", dn.Ip, dn.Port, err)
			} else {
				glog.Warningf("SendHeartbeat.Recv: %v", err)
			}
			return err
		}

		ms.Topo.Sequence.SetMax(heartbeat.MaxFileKey)

		if dn == nil {
			dcName, rackName := ms.Topo.Configuration.Locate(heartbeat.Ip, heartbeat.DataCenter, heartbeat.Rack)
			dc := ms.Topo.GetOrCreateDataCenter(dcName)
			rack := dc.GetOrCreateRack(rackName)
			dn = rack.GetOrCreateDataNode(heartbeat.Ip, int(heartbeat.Port), heartbeat.PublicUrl, heartbeat.MaxVolumeCounts)
			glog.V(0).Infof("added volume server %v:%d", heartbeat.GetIp(), heartbeat.GetPort())
			if err := stream.Send(&master_pb.HeartbeatResponse{
				VolumeSizeLimit: uint64(ms.option.VolumeSizeLimitMB) * 1024 * 1024,
			}); err != nil {
				glog.Warningf("SendHeartbeat.Send volume size to %s:%d %v", dn.Ip, dn.Port, err)
				return err
			}
		}

		dn.AdjustMaxVolumeCounts(heartbeat.MaxVolumeCounts)

		glog.V(4).Infof("master received heartbeat %s", heartbeat.String())
		var dataCenter string
		if dc := dn.GetDataCenter(); dc != nil {
			dataCenter = string(dc.Id())
		}
		message := &master_pb.VolumeLocation{
			Url:        dn.Url(),
			PublicUrl:  dn.PublicUrl,
			DataCenter: dataCenter,
		}
		if len(heartbeat.NewVolumes) > 0 || len(heartbeat.DeletedVolumes) > 0 {
			// process delta volume ids if exists for fast volume id updates
			for _, volInfo := range heartbeat.NewVolumes {
				message.NewVids = append(message.NewVids, volInfo.Id)
			}
			for _, volInfo := range heartbeat.DeletedVolumes {
				message.DeletedVids = append(message.DeletedVids, volInfo.Id)
			}
			// update master internal volume layouts
			ms.Topo.IncrementalSyncDataNodeRegistration(heartbeat.NewVolumes, heartbeat.DeletedVolumes, dn)
		}

		if len(heartbeat.Volumes) > 0 || heartbeat.HasNoVolumes {
			// process heartbeat.Volumes
			newVolumes, deletedVolumes := ms.Topo.SyncDataNodeRegistration(heartbeat.Volumes, dn)

			for _, v := range newVolumes {
				glog.V(0).Infof("master see new volume %d from %s", uint32(v.Id), dn.Url())
				message.NewVids = append(message.NewVids, uint32(v.Id))
			}
			for _, v := range deletedVolumes {
				glog.V(0).Infof("master see deleted volume %d from %s", uint32(v.Id), dn.Url())
				message.DeletedVids = append(message.DeletedVids, uint32(v.Id))
			}
		}

		if len(heartbeat.NewEcShards) > 0 || len(heartbeat.DeletedEcShards) > 0 {

			// update master internal volume layouts
			ms.Topo.IncrementalSyncDataNodeEcShards(heartbeat.NewEcShards, heartbeat.DeletedEcShards, dn)

			for _, s := range heartbeat.NewEcShards {
				message.NewVids = append(message.NewVids, s.Id)
			}
			for _, s := range heartbeat.DeletedEcShards {
				if dn.HasVolumesById(needle.VolumeId(s.Id)) {
					continue
				}
				message.DeletedVids = append(message.DeletedVids, s.Id)
			}

		}

		if len(heartbeat.EcShards) > 0 || heartbeat.HasNoEcShards {
			glog.V(1).Infof("master received ec shards from %s: %+v", dn.Url(), heartbeat.EcShards)
			newShards, deletedShards := ms.Topo.SyncDataNodeEcShards(heartbeat.EcShards, dn)

			// broadcast the ec vid changes to master clients
			for _, s := range newShards {
				message.NewVids = append(message.NewVids, uint32(s.VolumeId))
			}
			for _, s := range deletedShards {
				if dn.HasVolumesById(s.VolumeId) {
					continue
				}
				message.DeletedVids = append(message.DeletedVids, uint32(s.VolumeId))
			}

		}
		if len(message.NewVids) > 0 || len(message.DeletedVids) > 0 {
			ms.clientChansLock.RLock()
			for host, ch := range ms.clientChans {
				glog.V(0).Infof("master send to %s: %s", host, message.String())
				ch <- message
			}
			ms.clientChansLock.RUnlock()
		}

		// tell the volume servers about the leader
		newLeader, err := ms.Topo.Leader()
		if err != nil {
			glog.Warningf("SendHeartbeat find leader: %v", err)
			return err
		}
		if err := stream.Send(&master_pb.HeartbeatResponse{
			Leader: newLeader,
		}); err != nil {
			glog.Warningf("SendHeartbeat.Send response to to %s:%d %v", dn.Ip, dn.Port, err)
			return err
		}
	}
}

// KeepConnected keep a stream gRPC call to the master. Used by clients to know the master is up.
// And clients gets the up-to-date list of volume locations
func (ms *MasterServer) KeepConnected(stream master_pb.Seaweed_KeepConnectedServer) error {

	req, err := stream.Recv()
	if err != nil {
		return err
	}

	if !ms.Topo.IsLeader() {
		return ms.informNewLeader(stream)
	}

	peerAddress := findClientAddress(stream.Context(), req.GrpcPort)

	// buffer by 1 so we don't end up getting stuck writing to stopChan forever
	stopChan := make(chan bool, 1)

	clientName, messageChan := ms.addClient(req.Name, peerAddress)

	defer ms.deleteClient(clientName)

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

	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case message := <-messageChan:
			if err := stream.Send(message); err != nil {
				glog.V(0).Infof("=> client %v: %+v", clientName, message)
				return err
			}
		case <-ticker.C:
			if !ms.Topo.IsLeader() {
				return ms.informNewLeader(stream)
			}
		case <-stopChan:
			return nil
		}
	}

}

func (ms *MasterServer) informNewLeader(stream master_pb.Seaweed_KeepConnectedServer) error {
	leader, err := ms.Topo.Leader()
	if err != nil {
		glog.Errorf("topo leader: %v", err)
		return raft.NotLeaderError
	}
	if err := stream.Send(&master_pb.VolumeLocation{
		Leader: leader,
	}); err != nil {
		return err
	}
	return nil
}

func (ms *MasterServer) addClient(clientType string, clientAddress string) (clientName string, messageChan chan *master_pb.VolumeLocation) {
	clientName = clientType + "@" + clientAddress
	glog.V(0).Infof("+ client %v", clientName)

	// we buffer this because otherwise we end up in a potential deadlock where
	// the KeepConnected loop is no longer listening on this channel but we're
	// trying to send to it in SendHeartbeat and so we can't lock the
	// clientChansLock to remove the channel and we're stuck writing to it
	// 100 is probably overkill
	messageChan = make(chan *master_pb.VolumeLocation, 100)

	ms.clientChansLock.Lock()
	ms.clientChans[clientName] = messageChan
	ms.clientChansLock.Unlock()
	return
}

func (ms *MasterServer) deleteClient(clientName string) {
	glog.V(0).Infof("- client %v", clientName)
	ms.clientChansLock.Lock()
	delete(ms.clientChans, clientName)
	ms.clientChansLock.Unlock()
}

func findClientAddress(ctx context.Context, grpcPort uint32) string {
	// fmt.Printf("FromContext %+v\n", ctx)
	pr, ok := peer.FromContext(ctx)
	if !ok {
		glog.Error("failed to get peer from ctx")
		return ""
	}
	if pr.Addr == net.Addr(nil) {
		glog.Error("failed to get peer address")
		return ""
	}
	if grpcPort == 0 {
		return pr.Addr.String()
	}
	if tcpAddr, ok := pr.Addr.(*net.TCPAddr); ok {
		externalIP := tcpAddr.IP
		return fmt.Sprintf("%s:%d", externalIP, grpcPort)
	}
	return pr.Addr.String()

}

func (ms *MasterServer) ListMasterClients(ctx context.Context, req *master_pb.ListMasterClientsRequest) (*master_pb.ListMasterClientsResponse, error) {
	resp := &master_pb.ListMasterClientsResponse{}
	ms.clientChansLock.RLock()
	defer ms.clientChansLock.RUnlock()

	for k := range ms.clientChans {
		if strings.HasPrefix(k, req.ClientType+"@") {
			resp.GrpcAddresses = append(resp.GrpcAddresses, k[len(req.ClientType)+1:])
		}
	}
	return resp, nil
}

func (ms *MasterServer) GetMasterConfiguration(ctx context.Context, req *master_pb.GetMasterConfigurationRequest) (*master_pb.GetMasterConfigurationResponse, error) {

	// tell the volume servers about the leader
	leader, _ := ms.Topo.Leader()

	resp := &master_pb.GetMasterConfigurationResponse{
		MetricsAddress:         ms.option.MetricsAddress,
		MetricsIntervalSeconds: uint32(ms.option.MetricsIntervalSec),
		StorageBackends:        backend.ToPbStorageBackends(),
		DefaultReplication:     ms.option.DefaultReplicaPlacement,
		Leader:                 leader,
	}

	return resp, nil
}
