package weed_server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/cluster"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/raft"
	"google.golang.org/grpc/peer"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

func (ms *MasterServer) RegisterUuids(heartbeat *master_pb.Heartbeat) (duplicated_uuids []string, err error) {
	ms.Topo.UuidAccessLock.Lock()
	defer ms.Topo.UuidAccessLock.Unlock()
	key := fmt.Sprintf("%s:%d", heartbeat.Ip, heartbeat.Port)
	if ms.Topo.UuidMap == nil {
		ms.Topo.UuidMap = make(map[string][]string)
	}
	// find whether new uuid exists
	for k, v := range ms.Topo.UuidMap {
		sort.Strings(v)
		for _, id := range heartbeat.LocationUuids {
			index := sort.SearchStrings(v, id)
			if index < len(v) && v[index] == id {
				duplicated_uuids = append(duplicated_uuids, id)
				glog.Errorf("directory of %s on %s has been loaded", id, k)
			}
		}
	}
	if len(duplicated_uuids) > 0 {
		return duplicated_uuids, errors.New("volume: Duplicated volume directories were loaded")
	}

	ms.Topo.UuidMap[key] = heartbeat.LocationUuids
	glog.V(0).Infof("found new uuid:%v %v , %v", key, heartbeat.LocationUuids, ms.Topo.UuidMap)
	return nil, nil
}

func (ms *MasterServer) UnRegisterUuids(ip string, port int) {
	ms.Topo.UuidAccessLock.Lock()
	defer ms.Topo.UuidAccessLock.Unlock()
	key := fmt.Sprintf("%s:%d", ip, port)
	delete(ms.Topo.UuidMap, key)
	glog.V(0).Infof("remove volume server %v, online volume server: %v", key, ms.Topo.UuidMap)
}

func (ms *MasterServer) SendHeartbeat(stream master_pb.Seaweed_SendHeartbeatServer) error {
	var dn *topology.DataNode

	defer func() {
		if dn != nil {
			dn.Counter--
			if dn.Counter > 0 {
				glog.V(0).Infof("disconnect phantom volume server %s:%d remaining %d", dn.Ip, dn.Port, dn.Counter)
				return
			}

			message := &master_pb.VolumeLocation{
				DataCenter: dn.GetDataCenterId(),
				Url:        dn.Url(),
				PublicUrl:  dn.PublicUrl,
				GrpcPort:   uint32(dn.GrpcPort),
			}
			for _, v := range dn.GetVolumes() {
				message.DeletedVids = append(message.DeletedVids, uint32(v.Id))
			}
			for _, s := range dn.GetEcShards() {
				message.DeletedEcVids = append(message.DeletedEcVids, uint32(s.VolumeId))
			}

			// if the volume server disconnects and reconnects quickly
			//  the unregister and register can race with each other
			ms.Topo.UnRegisterDataNode(dn)
			glog.V(0).Infof("unregister disconnected volume server %s:%d", dn.Ip, dn.Port)
			ms.UnRegisterUuids(dn.Ip, dn.Port)

			if ms.Topo.IsLeader() && (len(message.DeletedVids) > 0 || len(message.DeletedEcVids) > 0) {
				ms.broadcastToClients(&master_pb.KeepConnectedResponse{VolumeLocation: message})
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
			stats.MasterReceivedHeartbeatCounter.WithLabelValues("error").Inc()
			return err
		}

		if !ms.Topo.IsLeader() {
			// tell the volume servers about the leader
			newLeader, err := ms.Topo.Leader()
			if err != nil {
				glog.Warningf("SendHeartbeat find leader: %v", err)
				return err
			}
			if err := stream.Send(&master_pb.HeartbeatResponse{
				Leader: string(newLeader),
			}); err != nil {
				if dn != nil {
					glog.Warningf("SendHeartbeat.Send response to %s:%d %v", dn.Ip, dn.Port, err)
				} else {
					glog.Warningf("SendHeartbeat.Send response %v", err)
				}
				return err
			}
			continue
		}

		ms.Topo.Sequence.SetMax(heartbeat.MaxFileKey)
		if dn == nil {
			// Skip delta heartbeat for volume server versions  better than 3.28 https://github.com/seaweedfs/seaweedfs/pull/3630
			if heartbeat.Ip == "" {
				continue
			} // ToDo must be removed after update major version
			dcName, rackName := ms.Topo.Configuration.Locate(heartbeat.Ip, heartbeat.DataCenter, heartbeat.Rack)
			dc := ms.Topo.GetOrCreateDataCenter(dcName)
			rack := dc.GetOrCreateRack(rackName)
			dn = rack.GetOrCreateDataNode(heartbeat.Ip, int(heartbeat.Port), int(heartbeat.GrpcPort), heartbeat.PublicUrl, heartbeat.MaxVolumeCounts)
			glog.V(0).Infof("added volume server %d: %v:%d %v", dn.Counter, heartbeat.GetIp(), heartbeat.GetPort(), heartbeat.LocationUuids)
			uuidlist, err := ms.RegisterUuids(heartbeat)
			if err != nil {
				if stream_err := stream.Send(&master_pb.HeartbeatResponse{
					DuplicatedUuids: uuidlist,
				}); stream_err != nil {
					glog.Warningf("SendHeartbeat.Send DuplicatedDirectory response to %s:%d %v", dn.Ip, dn.Port, stream_err)
					return stream_err
				}
				return err
			}

			if err := stream.Send(&master_pb.HeartbeatResponse{
				VolumeSizeLimit: uint64(ms.option.VolumeSizeLimitMB) * 1024 * 1024,
				Preallocate:     ms.preallocateSize > 0,
			}); err != nil {
				glog.Warningf("SendHeartbeat.Send volume size to %s:%d %v", dn.Ip, dn.Port, err)
				return err
			}
			stats.MasterReceivedHeartbeatCounter.WithLabelValues("dataNode").Inc()
			dn.Counter++
		}

		dn.AdjustMaxVolumeCounts(heartbeat.MaxVolumeCounts)

		glog.V(4).Infof("master received heartbeat %s", heartbeat.String())
		stats.MasterReceivedHeartbeatCounter.WithLabelValues("total").Inc()

		message := &master_pb.VolumeLocation{
			Url:        dn.Url(),
			PublicUrl:  dn.PublicUrl,
			DataCenter: dn.GetDataCenterId(),
			GrpcPort:   uint32(dn.GrpcPort),
		}
		if len(heartbeat.NewVolumes) > 0 {
			stats.MasterReceivedHeartbeatCounter.WithLabelValues("newVolumes").Inc()
		}
		if len(heartbeat.DeletedVolumes) > 0 {
			stats.MasterReceivedHeartbeatCounter.WithLabelValues("deletedVolumes").Inc()
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
			if heartbeat.Ip != "" {
				dcName, rackName := ms.Topo.Configuration.Locate(heartbeat.Ip, heartbeat.DataCenter, heartbeat.Rack)
				ms.Topo.DataNodeRegistration(dcName, rackName, dn)
			}

			// process heartbeat.Volumes
			stats.MasterReceivedHeartbeatCounter.WithLabelValues("Volumes").Inc()
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
			stats.MasterReceivedHeartbeatCounter.WithLabelValues("newEcShards").Inc()
			// update master internal volume layouts
			ms.Topo.IncrementalSyncDataNodeEcShards(heartbeat.NewEcShards, heartbeat.DeletedEcShards, dn)

			for _, s := range heartbeat.NewEcShards {
				message.NewEcVids = append(message.NewEcVids, s.Id)
			}
			for _, s := range heartbeat.DeletedEcShards {
				if dn.HasEcShards(needle.VolumeId(s.Id)) {
					continue
				}
				message.DeletedEcVids = append(message.DeletedEcVids, s.Id)
			}

		}

		if len(heartbeat.EcShards) > 0 || heartbeat.HasNoEcShards {
			stats.MasterReceivedHeartbeatCounter.WithLabelValues("ecShards").Inc()
			glog.V(4).Infof("master received ec shards from %s: %+v", dn.Url(), heartbeat.EcShards)
			newShards, deletedShards := ms.Topo.SyncDataNodeEcShards(heartbeat.EcShards, dn)

			// broadcast the ec vid changes to master clients
			for _, s := range newShards {
				message.NewEcVids = append(message.NewEcVids, uint32(s.VolumeId))
			}
			for _, s := range deletedShards {
				if dn.HasVolumesById(s.VolumeId) {
					continue
				}
				message.DeletedEcVids = append(message.DeletedEcVids, uint32(s.VolumeId))
			}

		}
		if len(message.NewVids) > 0 || len(message.DeletedVids) > 0 || len(message.NewEcVids) > 0 || len(message.DeletedEcVids) > 0 {
			ms.broadcastToClients(&master_pb.KeepConnectedResponse{VolumeLocation: message})
		}
	}
}

// KeepConnected keep a stream gRPC call to the master. Used by clients to know the master is up.
// And clients gets the up-to-date list of volume locations
func (ms *MasterServer) KeepConnected(stream master_pb.Seaweed_KeepConnectedServer) error {

	req, recvErr := stream.Recv()
	if recvErr != nil {
		return recvErr
	}

	if !ms.Topo.IsLeader() {
		return ms.informNewLeader(stream)
	}

	clientAddress := req.ClientAddress
	// Ensure that the clientAddress is unique.
	if clientAddress == "" {
		clientAddress = uuid.New().String()
	}
	peerAddress := pb.ServerAddress(clientAddress)

	// buffer by 1 so we don't end up getting stuck writing to stopChan forever
	stopChan := make(chan bool, 1)

	clientName, messageChan := ms.addClient(req.FilerGroup, req.ClientType, peerAddress)
	for _, update := range ms.Cluster.AddClusterNode(req.FilerGroup, req.ClientType, cluster.DataCenter(req.DataCenter), cluster.Rack(req.Rack), peerAddress, req.Version) {
		ms.broadcastToClients(update)
	}

	defer func() {
		for _, update := range ms.Cluster.RemoveClusterNode(req.FilerGroup, req.ClientType, peerAddress) {
			ms.broadcastToClients(update)
		}
		ms.deleteClient(clientName)
	}()
	for i, message := range ms.Topo.ToVolumeLocations() {
		if i == 0 {
			if leader, err := ms.Topo.Leader(); err == nil {
				message.Leader = string(leader)
			}
		}
		if sendErr := stream.Send(&master_pb.KeepConnectedResponse{VolumeLocation: message}); sendErr != nil {
			return sendErr
		}
	}

	go func() {
		for {
			_, err := stream.Recv()
			if err != nil {
				glog.V(2).Infof("- client %v: %v", clientName, err)
				go func() {
					// consume message chan to avoid deadlock, go routine exit when message chan is closed
					for range messageChan {
						// no op
					}
				}()
				close(stopChan)
				return
			}
		}
	}()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case message := <-messageChan:
			if err := stream.Send(message); err != nil {
				glog.V(0).Infof("=> client %v: %+v", clientName, message)
				return err
			}
		case <-ticker.C:
			if !ms.Topo.IsLeader() {
				stats.MasterRaftIsleader.Set(0)
				stats.MasterAdminLock.Reset()
				stats.MasterReplicaPlacementMismatch.Reset()
				return ms.informNewLeader(stream)
			} else {
				stats.MasterRaftIsleader.Set(1)
			}
		case <-stopChan:
			return nil
		}
	}

}

func (ms *MasterServer) broadcastToClients(message *master_pb.KeepConnectedResponse) {
	ms.clientChansLock.RLock()
	for client, ch := range ms.clientChans {
		select {
		case ch <- message:
			glog.V(4).Infof("send message to %s", client)
		default:
			stats.MasterBroadcastToFullErrorCounter.Inc()
			glog.Errorf("broadcastToClients %s message full", client)
		}
	}
	ms.clientChansLock.RUnlock()
}

func (ms *MasterServer) informNewLeader(stream master_pb.Seaweed_KeepConnectedServer) error {
	leader, err := ms.Topo.Leader()
	if err != nil {
		glog.Errorf("topo leader: %v", err)
		return raft.NotLeaderError
	}
	if err := stream.Send(&master_pb.KeepConnectedResponse{
		VolumeLocation: &master_pb.VolumeLocation{
			Leader: string(leader),
		},
	}); err != nil {
		return err
	}
	return nil
}

func (ms *MasterServer) addClient(filerGroup, clientType string, clientAddress pb.ServerAddress) (clientName string, messageChan chan *master_pb.KeepConnectedResponse) {
	clientName = filerGroup + "." + clientType + "@" + string(clientAddress)
	glog.V(0).Infof("+ client %v", clientName)

	// we buffer this because otherwise we end up in a potential deadlock where
	// the KeepConnected loop is no longer listening on this channel but we're
	// trying to send to it in SendHeartbeat and so we can't lock the
	// clientChansLock to remove the channel and we're stuck writing to it
	messageChan = make(chan *master_pb.KeepConnectedResponse, 10000)

	ms.clientChansLock.Lock()
	ms.clientChans[clientName] = messageChan
	ms.clientChansLock.Unlock()
	return
}

func (ms *MasterServer) deleteClient(clientName string) {
	glog.V(0).Infof("- client %v", clientName)
	ms.clientChansLock.Lock()
	// close message chan, so that the KeepConnected go routine can exit
	if clientChan, ok := ms.clientChans[clientName]; ok {
		close(clientChan)
		delete(ms.clientChans, clientName)
	}
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
		return util.JoinHostPort(externalIP.String(), int(grpcPort))
	}
	return pr.Addr.String()

}

func (ms *MasterServer) GetMasterConfiguration(ctx context.Context, req *master_pb.GetMasterConfigurationRequest) (*master_pb.GetMasterConfigurationResponse, error) {

	// tell the volume servers about the leader
	leader, _ := ms.Topo.Leader()

	resp := &master_pb.GetMasterConfigurationResponse{
		MetricsAddress:         ms.option.MetricsAddress,
		MetricsIntervalSeconds: uint32(ms.option.MetricsIntervalSec),
		StorageBackends:        backend.ToPbStorageBackends(),
		DefaultReplication:     ms.option.DefaultReplicaPlacement,
		VolumeSizeLimitMB:      uint32(ms.option.VolumeSizeLimitMB),
		VolumePreallocate:      ms.option.VolumePreallocate,
		Leader:                 string(leader),
	}

	return resp, nil
}
