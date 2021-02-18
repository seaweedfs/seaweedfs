package weed_server

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"time"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"

	"golang.org/x/net/context"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (vs *VolumeServer) GetMaster() string {
	return vs.currentMaster
}

func (vs *VolumeServer) checkWithMaster() (err error) {
	isConnected := false
	for !isConnected {
		for _, master := range vs.SeedMasterNodes {
			err = operation.WithMasterServerClient(master, vs.grpcDialOption, func(masterClient master_pb.SeaweedClient) error {
				resp, err := masterClient.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
				if err != nil {
					return fmt.Errorf("get master %s configuration: %v", master, err)
				}
				vs.metricsAddress, vs.metricsIntervalSec = resp.MetricsAddress, int(resp.MetricsIntervalSeconds)
				backend.LoadFromPbStorageBackends(resp.StorageBackends)
				return nil
			})
			if err == nil {
				return
			} else {
				glog.V(0).Infof("checkWithMaster %s: %v", master, err)
			}
		}
		time.Sleep(1790 * time.Millisecond)
	}
	return
}

func (vs *VolumeServer) heartbeat() {

	glog.V(0).Infof("Volume server start with seed master nodes: %v", vs.SeedMasterNodes)
	vs.store.SetDataCenter(vs.dataCenter)
	vs.store.SetRack(vs.rack)

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.volume")

	var err error
	var newLeader string
	for vs.isHeartbeating {
		for _, master := range vs.SeedMasterNodes {
			if newLeader != "" {
				// the new leader may actually is the same master
				// need to wait a bit before adding itself
				time.Sleep(3 * time.Second)
				master = newLeader
			}
			masterGrpcAddress, parseErr := pb.ParseServerToGrpcAddress(master)
			if parseErr != nil {
				glog.V(0).Infof("failed to parse master grpc %v: %v", masterGrpcAddress, parseErr)
				continue
			}
			vs.store.MasterAddress = master
			newLeader, err = vs.doHeartbeat(master, masterGrpcAddress, grpcDialOption, time.Duration(vs.pulseSeconds)*time.Second)
			if err != nil {
				glog.V(0).Infof("heartbeat error: %v", err)
				time.Sleep(time.Duration(vs.pulseSeconds) * time.Second)
				newLeader = ""
				vs.store.MasterAddress = ""
			}
			if !vs.isHeartbeating {
				break
			}
		}
	}
}

func (vs *VolumeServer) StopHeartbeat() (isAlreadyStopping bool) {
	if !vs.isHeartbeating {
		return true
	}
	vs.isHeartbeating = false
	close(vs.stopChan)
	return false
}

func (vs *VolumeServer) doHeartbeat(masterNode, masterGrpcAddress string, grpcDialOption grpc.DialOption, sleepInterval time.Duration) (newLeader string, err error) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcConection, err := pb.GrpcDial(ctx, masterGrpcAddress, grpcDialOption)
	if err != nil {
		return "", fmt.Errorf("fail to dial %s : %v", masterNode, err)
	}
	defer grpcConection.Close()

	client := master_pb.NewSeaweedClient(grpcConection)
	stream, err := client.SendHeartbeat(ctx)
	if err != nil {
		glog.V(0).Infof("SendHeartbeat to %s: %v", masterNode, err)
		return "", err
	}
	glog.V(0).Infof("Heartbeat to: %v", masterNode)
	vs.currentMaster = masterNode

	doneChan := make(chan error, 1)

	go func() {
		for {
			in, err := stream.Recv()
			if err != nil {
				doneChan <- err
				return
			}
			if in.GetVolumeSizeLimit() != 0 && vs.store.GetVolumeSizeLimit() != in.GetVolumeSizeLimit() {
				vs.store.SetVolumeSizeLimit(in.GetVolumeSizeLimit())
				if vs.store.MaybeAdjustVolumeMax() {
					if err = stream.Send(vs.store.CollectHeartbeat()); err != nil {
						glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", vs.currentMaster, err)
					}
				}
			}
			if in.GetLeader() != "" && vs.currentMaster != in.GetLeader() {
				glog.V(0).Infof("Volume Server found a new master newLeader: %v instead of %v", in.GetLeader(), vs.currentMaster)
				newLeader = in.GetLeader()
				doneChan <- nil
				return
			}
		}
	}()

	if err = stream.Send(vs.store.CollectHeartbeat()); err != nil {
		glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterNode, err)
		return "", err
	}

	if err = stream.Send(vs.store.CollectErasureCodingHeartbeat()); err != nil {
		glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterNode, err)
		return "", err
	}

	volumeTickChan := time.Tick(sleepInterval)
	ecShardTickChan := time.Tick(17 * sleepInterval)

	for {
		select {
		case volumeMessage := <-vs.store.NewVolumesChan:
			deltaBeat := &master_pb.Heartbeat{
				NewVolumes: []*master_pb.VolumeShortInformationMessage{
					&volumeMessage,
				},
			}
			glog.V(1).Infof("volume server %s:%d adds volume %d", vs.store.Ip, vs.store.Port, volumeMessage.Id)
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update to master %s: %v", masterNode, err)
				return "", err
			}
		case ecShardMessage := <-vs.store.NewEcShardsChan:
			deltaBeat := &master_pb.Heartbeat{
				NewEcShards: []*master_pb.VolumeEcShardInformationMessage{
					&ecShardMessage,
				},
			}
			glog.V(1).Infof("volume server %s:%d adds ec shard %d:%d", vs.store.Ip, vs.store.Port, ecShardMessage.Id,
				erasure_coding.ShardBits(ecShardMessage.EcIndexBits).ShardIds())
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update to master %s: %v", masterNode, err)
				return "", err
			}
		case volumeMessage := <-vs.store.DeletedVolumesChan:
			deltaBeat := &master_pb.Heartbeat{
				DeletedVolumes: []*master_pb.VolumeShortInformationMessage{
					&volumeMessage,
				},
			}
			glog.V(1).Infof("volume server %s:%d deletes volume %d", vs.store.Ip, vs.store.Port, volumeMessage.Id)
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update to master %s: %v", masterNode, err)
				return "", err
			}
		case ecShardMessage := <-vs.store.DeletedEcShardsChan:
			deltaBeat := &master_pb.Heartbeat{
				DeletedEcShards: []*master_pb.VolumeEcShardInformationMessage{
					&ecShardMessage,
				},
			}
			glog.V(1).Infof("volume server %s:%d deletes ec shard %d:%d", vs.store.Ip, vs.store.Port, ecShardMessage.Id,
				erasure_coding.ShardBits(ecShardMessage.EcIndexBits).ShardIds())
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update to master %s: %v", masterNode, err)
				return "", err
			}
		case <-volumeTickChan:
			glog.V(4).Infof("volume server %s:%d heartbeat", vs.store.Ip, vs.store.Port)
			vs.store.MaybeAdjustVolumeMax()
			if err = stream.Send(vs.store.CollectHeartbeat()); err != nil {
				glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterNode, err)
				return "", err
			}
		case <-ecShardTickChan:
			glog.V(4).Infof("volume server %s:%d ec heartbeat", vs.store.Ip, vs.store.Port)
			if err = stream.Send(vs.store.CollectErasureCodingHeartbeat()); err != nil {
				glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterNode, err)
				return "", err
			}
		case err = <-doneChan:
			return
		case <-vs.stopChan:
			var volumeMessages []*master_pb.VolumeInformationMessage
			emptyBeat := &master_pb.Heartbeat{
				Ip:           vs.store.Ip,
				Port:         uint32(vs.store.Port),
				PublicUrl:    vs.store.PublicUrl,
				MaxFileKey:   uint64(0),
				DataCenter:   vs.store.GetDataCenter(),
				Rack:         vs.store.GetRack(),
				Volumes:      volumeMessages,
				HasNoVolumes: len(volumeMessages) == 0,
			}
			glog.V(1).Infof("volume server %s:%d stops and deletes all volumes", vs.store.Ip, vs.store.Port)
			if err = stream.Send(emptyBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update to master %s: %v", masterNode, err)
				return "", err
			}
			return
		}
	}
}
