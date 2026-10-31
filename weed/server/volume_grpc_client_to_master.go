package weed_server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/stats"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (vs *VolumeServer) GetMaster(ctx context.Context) pb.ServerAddress {
	return vs.getCurrentMaster()
}

// lookupRaftLeaderMaster resolves the raft leader for topology mutations via
// GetMasterConfiguration on configured peers. It does not update currentMaster;
// the heartbeat loop owns that field and reconnects when the stream reports a
// different leader than the connected peer.
func (vs *VolumeServer) lookupRaftLeaderMaster(ctx context.Context) (pb.ServerAddress, error) {
	return vs.lookupRaftLeaderMasterAmong(ctx, vs.SeedMasterNodes)
}

func (vs *VolumeServer) lookupRaftLeaderMasterAmong(ctx context.Context, masters []pb.ServerAddress) (pb.ServerAddress, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	leader, err := operation.LookupRaftLeaderMaster(ctx, masters, vs.grpcDialOption)
	if err != nil {
		if isContextDoneErr(err) {
			glog.V(1).Infof("volume server %s:%d: raft leader lookup: %v", vs.store.Ip, vs.store.Port, err)
		} else {
			glog.V(0).Infof("volume server %s:%d: raft leader lookup: %v", vs.store.Ip, vs.store.Port, err)
		}
		return "", err
	}
	current := vs.getCurrentMaster()
	if !leader.Equals(current) {
		glog.V(1).Infof("volume server %s:%d: raft leader %v (heartbeat peer %v)", vs.store.Ip, vs.store.Port, leader, current)
	}
	return leader, nil
}

// refreshLeaderFromPeers publishes the leader the configured masters report,
// so lookups stop going to a master the heartbeat just lost while the loop
// retries, and returns it so the loop can dial it next. Without an answer
// within timeout no leader is published: the lost master is dead or no longer
// leader, and a follower answers lookups from its own cache rather than
// refusing them, so failing fast is the safer answer.
//
// The lost master is asked last. Each peer gets up to 5s, so when the lost
// master is unreachable rather than refusing connections (a partition or a
// powered-off host) asking it first spends the whole timeout and the live
// peers that know the new leader are never asked.
func (vs *VolumeServer) refreshLeaderFromPeers(lost pb.ServerAddress, timeout time.Duration) pb.ServerAddress {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	leader, err := vs.lookupRaftLeaderMasterAmong(ctx, mastersAskingLast(vs.SeedMasterNodes, lost))
	if err != nil {
		vs.setCurrentMaster("")
		return ""
	}
	vs.setCurrentMaster(leader)
	return leader
}

// mastersAskingLast returns masters with every entry equal to last moved to
// the end, keeping the order of the rest.
func mastersAskingLast(masters []pb.ServerAddress, last pb.ServerAddress) []pb.ServerAddress {
	ordered := make([]pb.ServerAddress, 0, len(masters))
	var tail []pb.ServerAddress
	for _, m := range masters {
		if last != "" && m.Equals(last) {
			tail = append(tail, m)
			continue
		}
		ordered = append(ordered, m)
	}
	return append(ordered, tail...)
}

func isContextDoneErr(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if st, ok := status.FromError(err); ok {
		return st.Code() == codes.Canceled || st.Code() == codes.DeadlineExceeded
	}
	return false
}

// getCurrentMaster returns vs.currentMaster under a read lock so callers
// (e.g. Ping admission) do not race with the heartbeat goroutine that
// rewrites it on leader changes.
func (vs *VolumeServer) getCurrentMaster() pb.ServerAddress {
	vs.currentMasterLock.RLock()
	defer vs.currentMasterLock.RUnlock()
	return vs.currentMaster
}

// setCurrentMaster updates vs.currentMaster under a write lock. The
// heartbeat goroutine calls this only with a master known to be the leader:
// one that answered a heartbeat as leader, or the one a follower named, on
// the heartbeat stream or, after the stream failed, in its configuration.
// After a failed stream with no leader named, it is cleared to "".
func (vs *VolumeServer) setCurrentMaster(master pb.ServerAddress) {
	vs.currentMasterLock.Lock()
	vs.currentMaster = master
	vs.currentMasterLock.Unlock()
}

func (vs *VolumeServer) checkWithMaster() (err error) {
	for {
		for _, master := range vs.SeedMasterNodes {
			err = operation.WithMasterServerClient(context.Background(), false, master, vs.grpcDialOption, func(masterClient master_pb.SeaweedClient) error {
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
}

func (vs *VolumeServer) heartbeat() {

	glog.V(0).Infof("Volume server start with seed master nodes: %v", vs.SeedMasterNodes)
	vs.store.SetDataCenter(vs.dataCenter)
	vs.store.SetRack(vs.rack)

	if len(vs.SeedMasterNodes) == 0 {
		glog.Warningf("Volume server has no master nodes to heartbeat to; heartbeats disabled")
		return
	}

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.volume")

	var newLeader pb.ServerAddress
	duplicateRetryCount := 0
	for vs.isHeartbeating {
		triedHints := make(map[string]bool)
		for _, master := range vs.SeedMasterNodes {
			// Named leaders are tried on top of the seed scan rather than in
			// the next seed's place: a peer that keeps naming a dead leader
			// would otherwise eat that seed's slot on every pass and starve
			// it. A fresh name is followed right away — the pending seed may
			// be the master the stream just lost — and a name already dialed
			// this pass is not dialed again.
			for hints := 0; newLeader != "" && !newLeader.Equals(master) && !triedHints[newLeader.ToHttpAddress()] && hints < len(vs.SeedMasterNodes); hints++ {
				// the new leader may actually is the same master
				// need to wait a bit before adding itself
				time.Sleep(3 * time.Second)
				triedHints[newLeader.ToHttpAddress()] = true
				newLeader, duplicateRetryCount = vs.heartbeatOnce(newLeader, grpcDialOption, duplicateRetryCount)
				if !vs.isHeartbeating {
					break
				}
			}
			if !vs.isHeartbeating {
				break
			}
			if newLeader.Equals(master) {
				// the seed dial covers a hint that names the seed itself
				newLeader = ""
			}
			triedHints[master.ToHttpAddress()] = true
			named, retryCount := vs.heartbeatOnce(master, grpcDialOption, duplicateRetryCount)
			duplicateRetryCount = retryCount
			if named != "" && newLeader == "" {
				newLeader = named
			}
			if !vs.isHeartbeating {
				break
			}
		}
	}
}

// heartbeatOnce makes one heartbeat attempt on target and returns the leader
// the stream or the peer refresh named — "" when neither did — with the
// running duplicate-UUID retry count.
func (vs *VolumeServer) heartbeatOnce(target pb.ServerAddress, grpcDialOption grpc.DialOption, duplicateRetryCount int) (named pb.ServerAddress, retryCount int) {
	newLeader, err := vs.doHeartbeatWithRetry(target, grpcDialOption, vs.pulsePeriod, duplicateRetryCount)
	if err == nil {
		// Successful connection, reset retry count
		return newLeader, 0
	}

	glog.V(0).Infof("heartbeat to %s error: %v", target, err)

	var leader pb.ServerAddress
	retryCount = duplicateRetryCount
	// Check if this is a duplicate UUID retry error
	if strings.Contains(err.Error(), "duplicate UUIDs detected, retrying connection") {
		retryCount++
		retryDelay := time.Duration(1<<(retryCount-1)) * 2 * time.Second // exponential backoff: 2s, 4s, 8s
		glog.V(0).Infof("Waiting %v before retrying due to duplicate UUID detection...", retryDelay)
		time.Sleep(retryDelay)
	} else {
		// Regular error, reset duplicate retry count
		retryCount = 0
		// The lookup spends part of the retry delay rather than
		// adding to it.
		retryAt := time.Now().Add(vs.pulsePeriod)
		leader = vs.refreshLeaderFromPeers(target, vs.pulsePeriod)
		wait := time.Until(retryAt)
		if wait > time.Second && (vs.getCurrentMaster() != "" || !vs.confirmedLeaderOnce.Load()) {
			// A leader just named by the refresh usually accepts the
			// next stream right away, and an unregistered server has
			// nothing else to do, so reconnect promptly. Once the
			// server was registered and no leader is known, keep the
			// pulse backoff.
			wait = time.Second
		}
		time.Sleep(wait)
	}

	stats.VolumeServerMasterDisconnections.WithLabelValues(target.String()).Inc()
	if leader != "" && !leader.Equals(target) {
		// Dial the leader the other masters just named rather than
		// the next seed, which can be the master this stream just
		// lost: an unreachable master holds each dial for the gRPC
		// connect timeout before the loop moves on.
		return leader, retryCount
	}
	return "", retryCount
}

func (vs *VolumeServer) StopHeartbeat() (isAlreadyStopping bool) {
	if !vs.isHeartbeating {
		return true
	}
	vs.isHeartbeating = false
	close(vs.stopChan)
	return false
}

func (vs *VolumeServer) doHeartbeatWithRetry(masterAddress pb.ServerAddress, grpcDialOption grpc.DialOption, sleepInterval time.Duration, duplicateRetryCount int) (newLeader pb.ServerAddress, err error) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcConnection, err := pb.GrpcDial(ctx, masterAddress.ToGrpcAddress(), false, grpcDialOption)
	if err != nil {
		return "", fmt.Errorf("fail to dial %s : %v", masterAddress, err)
	}
	defer grpcConnection.Close()

	client := master_pb.NewSeaweedClient(grpcConnection)
	stream, err := client.SendHeartbeat(ctx)
	if err != nil {
		glog.V(0).Infof("SendHeartbeat to %s: %v", masterAddress, err)
		return "", err
	}
	glog.V(0).Infof("Heartbeat to: %v", masterAddress)

	doneChan := make(chan error, 1)
	// The receiver asks for a fresh heartbeat here rather than sending it,
	// since a gRPC stream allows only one sender at a time.
	resendChan := make(chan struct{}, 1)

	go func() {
		confirmedLeader := false
		for {
			in, err := stream.Recv()
			if err != nil {
				doneChan <- err
				return
			}
			// A reply names the leader or comes from it, so it settles where
			// lookups go before anything else on the reply is handled: a
			// follower reply carries no other fields, and a leader stays the
			// lookup target even when it rejects this registration.
			if in.GetLeader() != "" {
				// Only a follower names the leader. Publish the named leader right
				// away, so lookups stop landing on this follower while the loop
				// reconnects.
				leader := pb.ServerAddress(in.GetLeader())
				if !masterAddress.Equals(leader) {
					glog.V(0).Infof("Volume Server found a new master newLeader: %v instead of %v", leader, masterAddress)
					vs.setCurrentMaster(leader)
					newLeader = leader
					doneChan <- nil
					return
				}
			} else if !confirmedLeader {
				// The leader answers every new heartbeat stream without naming a
				// leader, so this peer is the one lookups should go to.
				confirmedLeader = true
				vs.confirmedLeaderOnce.Store(true)
				vs.setCurrentMaster(masterAddress)
			}
			if len(in.DuplicatedUuids) > 0 {
				var duplicateDir []string
				for _, loc := range vs.store.Locations {
					for _, uuid := range in.DuplicatedUuids {
						if uuid == loc.DirectoryUuid {
							duplicateDir = append(duplicateDir, loc.Directory)
						}
					}
				}

				// Implement retry logic for potential race conditions
				const maxRetries = 3
				if duplicateRetryCount < maxRetries {
					retryDelay := time.Duration(1<<duplicateRetryCount) * 2 * time.Second // exponential backoff: 2s, 4s, 8s
					glog.Errorf("Master reported duplicate volume directories: %v (retry %d/%d)", duplicateDir, duplicateRetryCount+1, maxRetries)
					glog.Errorf("This might be due to a race condition during reconnection. Waiting %v before retrying...", retryDelay)

					// Return error to trigger retry with increased count
					doneChan <- fmt.Errorf("duplicate UUIDs detected, retrying connection (attempt %d/%d)", duplicateRetryCount+1, maxRetries)
					return
				} else {
					// After max retries, this is likely a real duplicate
					glog.Errorf("Shut down Volume Server due to persistent duplicate volume directories after %d retries: %v", maxRetries, duplicateDir)
					glog.Errorf("Please check if another volume server is using the same directory")
					os.Exit(1)
				}
			}
			vs.store.UpdateMaxVolumeIdSeen(needle.VolumeId(in.MaxVolumeId))
			vs.store.UpdateLastSeenTopologyId(in.TopologyId)
			volumeOptsChanged := false
			if vs.store.GetPreallocate() != in.GetPreallocate() {
				vs.store.SetPreallocate(in.GetPreallocate())
				volumeOptsChanged = true
			}
			if in.GetVolumeSizeLimit() != 0 && vs.store.GetVolumeSizeLimit() != in.GetVolumeSizeLimit() {
				vs.store.SetVolumeSizeLimit(in.GetVolumeSizeLimit())
				volumeOptsChanged = true
			}
			if volumeOptsChanged {
				if vs.store.MaybeAdjustVolumeMax() {
					select {
					case resendChan <- struct{}{}:
					default:
					}
				}
			}
			if in.GetVolumeDigestSupported() {
				vs.store.AcceptVolumeChanges()
			}
			if in.GetResendFullVolumeList() {
				glog.V(0).Infof("master %s asked for the full volume list", masterAddress)
				vs.store.RequestFullVolumeList()
			}
		}
	}()

	// This master may know nothing about this server, and has not yet said
	// whether it understands digests, so start from the whole list.
	vs.store.ResetVolumeReporting()

	if err = stream.Send(vs.store.CollectHeartbeat()); err != nil {
		glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterAddress, err)
		return "", err
	}

	if err = stream.Send(vs.store.CollectErasureCodingHeartbeat()); err != nil {
		glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterAddress, err)
		return "", err
	}

	volumeTickChan := time.NewTicker(sleepInterval)
	defer volumeTickChan.Stop()
	ecShardTickChan := time.NewTicker(17 * sleepInterval)
	defer ecShardTickChan.Stop()
	dataCenter := vs.store.GetDataCenter()
	rack := vs.store.GetRack()
	ip := vs.store.Ip
	port := uint32(vs.store.Port)
	for {
		select {
		case stateMessage := <-vs.store.StateUpdateChan:
			stateBeat := &master_pb.Heartbeat{
				Ip:         ip,
				Port:       port,
				DataCenter: dataCenter,
				Rack:       rack,
				State:      stateMessage,
			}
			glog.V(0).Infof("volume server %s:%d updates state to %v", vs.store.Ip, vs.store.Port, stateMessage)
			if err = stream.Send(stateBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update state to master %s: %v", masterAddress, err)
				return "", err
			}
		case first := <-vs.store.NewVolumesChan:
			volumes := util.DrainChannel(vs.store.NewVolumesChan, first)
			deltaBeat := &master_pb.Heartbeat{
				Ip:         ip,
				Port:       port,
				DataCenter: dataCenter,
				Rack:       rack,
				NewVolumes: volumes,
			}
			for _, v := range volumes {
				glog.V(0).Infof("volume server %s:%d adds volume %d", vs.store.Ip, vs.store.Port, v.Id)
			}
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update to master %s: %v", masterAddress, err)
				return "", err
			}
		case first := <-vs.store.NewEcShardsChan:
			shards := util.DrainChannel(vs.store.NewEcShardsChan, first)
			deltaBeat := &master_pb.Heartbeat{
				Ip:          ip,
				Port:        port,
				DataCenter:  dataCenter,
				Rack:        rack,
				NewEcShards: shards,
			}
			for _, s := range shards {
				si := erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(s)
				glog.V(0).Infof("volume server %s:%d adds ec shards to %d [%s]", vs.store.Ip, vs.store.Port, s.Id, si.String())
			}
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update to master %s: %v", masterAddress, err)
				return "", err
			}
		case first := <-vs.store.DeletedVolumesChan:
			volumes := util.DrainChannel(vs.store.DeletedVolumesChan, first)
			deltaBeat := &master_pb.Heartbeat{
				Ip:             ip,
				Port:           port,
				DataCenter:     dataCenter,
				Rack:           rack,
				DeletedVolumes: volumes,
			}
			for _, v := range volumes {
				glog.V(0).Infof("volume server %s:%d deletes volume %d", vs.store.Ip, vs.store.Port, v.Id)
			}
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update to master %s: %v", masterAddress, err)
				return "", err
			}
		case first := <-vs.store.DeletedEcShardsChan:
			shards := util.DrainChannel(vs.store.DeletedEcShardsChan, first)
			deltaBeat := &master_pb.Heartbeat{
				Ip:              ip,
				Port:            port,
				DataCenter:      dataCenter,
				Rack:            rack,
				DeletedEcShards: shards,
			}
			for _, s := range shards {
				glog.V(0).Infof("volume server %s:%d deletes ec shard %d:%s", vs.store.Ip, vs.store.Port, s.Id,
					erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(s).String())
			}
			if err = stream.Send(deltaBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update to master %s: %v", masterAddress, err)
				return "", err
			}
		case <-resendChan:
			if err = stream.Send(vs.store.CollectHeartbeat()); err != nil {
				glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterAddress, err)
				return "", err
			}
		case <-volumeTickChan.C:
			glog.V(4).Infof("volume server %s:%d heartbeat", vs.store.Ip, vs.store.Port)
			vs.store.MaybeAdjustVolumeMax()
			if err = stream.Send(vs.store.CollectHeartbeat()); err != nil {
				glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterAddress, err)
				return "", err
			}
		case <-ecShardTickChan.C:
			glog.V(4).Infof("volume server %s:%d ec heartbeat", vs.store.Ip, vs.store.Port)
			if err = stream.Send(vs.store.CollectErasureCodingHeartbeat()); err != nil {
				glog.V(0).Infof("Volume Server Failed to talk with master %s: %v", masterAddress, err)
				return "", err
			}
		case err = <-doneChan:
			return
		case <-vs.stopChan:
			var volumeMessages []*master_pb.VolumeInformationMessage
			emptyBeat := &master_pb.Heartbeat{
				Ip:           ip,
				Port:         port,
				PublicUrl:    vs.store.PublicUrl,
				MaxFileKey:   uint64(0),
				DataCenter:   dataCenter,
				Rack:         rack,
				Volumes:      volumeMessages,
				HasNoVolumes: len(volumeMessages) == 0,
			}
			glog.V(1).Infof("volume server %s:%d stops and deletes all volumes", vs.store.Ip, vs.store.Port)
			if err = stream.Send(emptyBeat); err != nil {
				glog.V(0).Infof("Volume Server Failed to update to master %s: %v", masterAddress, err)
				return "", err
			}
			return
		}
	}
}
