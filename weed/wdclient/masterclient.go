package wdclient

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
)

// masterVolumeProvider implements VolumeLocationProvider by querying master
// This is rarely called since master pushes updates proactively via KeepConnected stream
type masterVolumeProvider struct {
	masterClient *MasterClient
}

// LookupVolumeIds queries the master for volume locations (fallback when cache misses)
// Returns partial results with aggregated errors for volumes that failed
func (p *masterVolumeProvider) LookupVolumeIds(ctx context.Context, volumeIds []string) (map[string][]Location, error) {
	result := make(map[string][]Location)
	var lookupErrors []error

	glog.V(2).Infof("Looking up %d volumes from master: %v", len(volumeIds), volumeIds)

	err := pb.WithMasterClient(false, p.masterClient.GetMaster(ctx), p.masterClient.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
		resp, err := client.LookupVolume(ctx, &master_pb.LookupVolumeRequest{
			VolumeOrFileIds: volumeIds,
		})
		if err != nil {
			return fmt.Errorf("master lookup failed: %v", err)
		}

		for _, vidLoc := range resp.VolumeIdLocations {
			// Preserve per-volume errors from master response
			// These could indicate misconfiguration, volume deletion, etc.
			if vidLoc.Error != "" {
				lookupErrors = append(lookupErrors, fmt.Errorf("volume %s: %s", vidLoc.VolumeOrFileId, vidLoc.Error))
				glog.V(1).Infof("volume %s lookup error from master: %s", vidLoc.VolumeOrFileId, vidLoc.Error)
				continue
			}

			// Parse volume ID from response
			parts := strings.Split(vidLoc.VolumeOrFileId, ",")
			vidOnly := parts[0]
			vid, err := strconv.ParseUint(vidOnly, 10, 32)
			if err != nil {
				lookupErrors = append(lookupErrors, fmt.Errorf("volume %s: invalid volume ID format: %w", vidLoc.VolumeOrFileId, err))
				glog.Warningf("Failed to parse volume id '%s' from master response '%s': %v", vidOnly, vidLoc.VolumeOrFileId, err)
				continue
			}

			var locations []Location
			for _, masterLoc := range vidLoc.Locations {
				loc := Location{
					Url:        masterLoc.Url,
					PublicUrl:  masterLoc.PublicUrl,
					GrpcPort:   int(masterLoc.GrpcPort),
					DataCenter: masterLoc.DataCenter,
				}
				// Update cache with the location
				p.masterClient.addLocation(uint32(vid), loc)
				locations = append(locations, loc)
			}

			if len(locations) > 0 {
				result[vidOnly] = locations
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	// Return partial results with detailed errors
	// Callers should check both result map and error
	if len(lookupErrors) > 0 {
		glog.V(2).Infof("MasterClient: looked up %d volumes, found %d, %d errors", len(volumeIds), len(result), len(lookupErrors))
		return result, fmt.Errorf("master volume lookup errors: %w", errors.Join(lookupErrors...))
	}

	glog.V(3).Infof("MasterClient: looked up %d volumes, found %d", len(volumeIds), len(result))
	return result, nil
}

// MasterClient connects to master servers and maintains volume location cache
// It receives real-time updates via KeepConnected streaming and uses vidMapClient for caching
type MasterClient struct {
	*vidMapClient // Embedded cache with shared logic

	FilerGroup        string
	clientType        string
	clientHost        pb.ServerAddress
	rack              string
	currentMaster     pb.ServerAddress
	currentMasterLock sync.RWMutex
	masters           pb.ServerDiscovery
	grpcDialOption    grpc.DialOption
	OnPeerUpdate      func(update *master_pb.ClusterNodeUpdate, startFrom time.Time)
	OnPeerUpdateLock  sync.RWMutex
}

func NewMasterClient(grpcDialOption grpc.DialOption, filerGroup string, clientType string, clientHost pb.ServerAddress, clientDataCenter string, rack string, masters pb.ServerDiscovery) *MasterClient {
	mc := &MasterClient{
		FilerGroup:     filerGroup,
		clientType:     clientType,
		clientHost:     clientHost,
		rack:           rack,
		masters:        masters,
		grpcDialOption: grpcDialOption,
	}

	// Create provider that references this MasterClient
	provider := &masterVolumeProvider{masterClient: mc}

	// Initialize embedded vidMapClient with the provider and default cache size
	mc.vidMapClient = newVidMapClient(provider, clientDataCenter, DefaultVidMapCacheSize)

	return mc
}

func (mc *MasterClient) SetOnPeerUpdateFn(onPeerUpdate func(update *master_pb.ClusterNodeUpdate, startFrom time.Time)) {
	mc.OnPeerUpdateLock.Lock()
	mc.OnPeerUpdate = onPeerUpdate
	mc.OnPeerUpdateLock.Unlock()
}

func (mc *MasterClient) tryAllMasters(ctx context.Context) {
	var nextHintedLeader pb.ServerAddress
	mc.masters.RefreshBySrvIfAvailable()
	for _, master := range mc.masters.GetInstances() {
		nextHintedLeader = mc.tryConnectToMaster(ctx, master)
		for nextHintedLeader != "" {
			select {
			case <-ctx.Done():
				glog.V(0).Infof("Connection attempt to all masters stopped: %v", ctx.Err())
				return
			default:
				nextHintedLeader = mc.tryConnectToMaster(ctx, nextHintedLeader)
			}
		}
		mc.setCurrentMaster("")
	}
}

func (mc *MasterClient) tryConnectToMaster(ctx context.Context, master pb.ServerAddress) (nextHintedLeader pb.ServerAddress) {
	glog.V(1).Infof("%s.%s masterClient Connecting to master %v", mc.FilerGroup, mc.clientType, master)
	stats.MasterClientConnectCounter.WithLabelValues("total").Inc()
	gprcErr := pb.WithMasterClient(true, master, mc.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		stream, err := client.KeepConnected(ctx)
		if err != nil {
			glog.V(1).Infof("%s.%s masterClient failed to keep connected to %s: %v", mc.FilerGroup, mc.clientType, master, err)
			stats.MasterClientConnectCounter.WithLabelValues(stats.FailedToKeepConnected).Inc()
			return err
		}

		if err = stream.Send(&master_pb.KeepConnectedRequest{
			FilerGroup:    mc.FilerGroup,
			DataCenter:    mc.GetDataCenter(),
			Rack:          mc.rack,
			ClientType:    mc.clientType,
			ClientAddress: string(mc.clientHost),
			Version:       version.Version(),
		}); err != nil {
			glog.V(0).Infof("%s.%s masterClient failed to send to %s: %v", mc.FilerGroup, mc.clientType, master, err)
			stats.MasterClientConnectCounter.WithLabelValues(stats.FailedToSend).Inc()
			return err
		}
		glog.V(1).Infof("%s.%s masterClient Connected to %v", mc.FilerGroup, mc.clientType, master)

		resp, err := stream.Recv()
		if err != nil {
			glog.V(0).Infof("%s.%s masterClient failed to receive from %s: %v", mc.FilerGroup, mc.clientType, master, err)
			stats.MasterClientConnectCounter.WithLabelValues(stats.FailedToReceive).Inc()
			return err
		}

		// check if it is the leader to determine whether to reset the vidMap
		if resp.VolumeLocation != nil {
			if resp.VolumeLocation.Leader != "" && string(master) != resp.VolumeLocation.Leader {
				glog.V(0).Infof("master %v redirected to leader %v", master, resp.VolumeLocation.Leader)
				nextHintedLeader = pb.ServerAddress(resp.VolumeLocation.Leader)
				stats.MasterClientConnectCounter.WithLabelValues(stats.RedirectedToLeader).Inc()
				return nil
			}
			mc.resetVidMap()
			mc.updateVidMap(resp)
		} else {
			// First message from master is not VolumeLocation (e.g., ClusterNodeUpdate)
			// Still need to reset cache to ensure we don't use stale data from previous master
			mc.resetVidMap()
		}
		mc.setCurrentMaster(master)

		for {
			resp, err := stream.Recv()
			if err != nil {
				glog.V(0).Infof("%s.%s masterClient failed to receive from %s: %v", mc.FilerGroup, mc.clientType, master, err)
				stats.MasterClientConnectCounter.WithLabelValues(stats.FailedToReceive).Inc()
				return err
			}

			// if resp.VolumeLocation != nil {
			//   glog.V(0).Infof("volume location: %+v", resp.VolumeLocation)
			// }

			if resp.VolumeLocation != nil {
				// Check for leader change during the stream
				// If master announces a new leader, reconnect to it
				if resp.VolumeLocation.Leader != "" && string(mc.GetMaster(ctx)) != resp.VolumeLocation.Leader {
					glog.V(0).Infof("currentMaster %v redirected to leader %v", mc.GetMaster(ctx), resp.VolumeLocation.Leader)
					nextHintedLeader = pb.ServerAddress(resp.VolumeLocation.Leader)
					stats.MasterClientConnectCounter.WithLabelValues(stats.RedirectedToLeader).Inc()
					return nil
				}
				mc.updateVidMap(resp)
			}
			if resp.ClusterNodeUpdate != nil {
				update := resp.ClusterNodeUpdate
				mc.OnPeerUpdateLock.RLock()
				if mc.OnPeerUpdate != nil {
					if update.FilerGroup == mc.FilerGroup {
						if update.IsAdd {
							glog.V(0).Infof("+ %s@%s noticed %s.%s %s\n", mc.clientType, mc.clientHost, update.FilerGroup, update.NodeType, update.Address)
						} else {
							glog.V(0).Infof("- %s@%s noticed %s.%s %s\n", mc.clientType, mc.clientHost, update.FilerGroup, update.NodeType, update.Address)
						}
						stats.MasterClientConnectCounter.WithLabelValues(stats.OnPeerUpdate).Inc()
						mc.OnPeerUpdate(update, time.Now())
					}
				}
				mc.OnPeerUpdateLock.RUnlock()
			}
			if err := ctx.Err(); err != nil {
				glog.V(0).Infof("Connection attempt to master stopped: %v", err)
				return err
			}
		}
	})
	if gprcErr != nil {
		stats.MasterClientConnectCounter.WithLabelValues(stats.Failed).Inc()
		glog.V(1).Infof("%s.%s masterClient failed to connect with master %v: %v", mc.FilerGroup, mc.clientType, master, gprcErr)
	}
	return nextHintedLeader
}

func (mc *MasterClient) updateVidMap(resp *master_pb.KeepConnectedResponse) {
	if resp.VolumeLocation.IsEmptyUrl() {
		glog.V(0).Infof("updateVidMap ignore short heartbeat: %+v", resp)
		return
	}
	// process new volume location
	loc := Location{
		Url:        resp.VolumeLocation.Url,
		PublicUrl:  resp.VolumeLocation.PublicUrl,
		DataCenter: resp.VolumeLocation.DataCenter,
		GrpcPort:   int(resp.VolumeLocation.GrpcPort),
	}
	for _, newVid := range resp.VolumeLocation.NewVids {
		glog.V(2).Infof("%s.%s: %s masterClient adds volume %d", mc.FilerGroup, mc.clientType, loc.Url, newVid)
		mc.addLocation(newVid, loc)
	}
	for _, deletedVid := range resp.VolumeLocation.DeletedVids {
		glog.V(2).Infof("%s.%s: %s masterClient removes volume %d", mc.FilerGroup, mc.clientType, loc.Url, deletedVid)
		mc.deleteLocation(deletedVid, loc)
	}
	for _, newEcVid := range resp.VolumeLocation.NewEcVids {
		glog.V(2).Infof("%s.%s: %s masterClient adds ec volume %d", mc.FilerGroup, mc.clientType, loc.Url, newEcVid)
		mc.addEcLocation(newEcVid, loc)
	}
	for _, deletedEcVid := range resp.VolumeLocation.DeletedEcVids {
		glog.V(2).Infof("%s.%s: %s masterClient removes ec volume %d", mc.FilerGroup, mc.clientType, loc.Url, deletedEcVid)
		mc.deleteEcLocation(deletedEcVid, loc)
	}
	glog.V(1).Infof("updateVidMap(%s) %s.%s: %s volume add: %d, del: %d, add ec: %d del ec: %d",
		resp.VolumeLocation.DataCenter, mc.FilerGroup, mc.clientType, loc.Url,
		len(resp.VolumeLocation.NewVids), len(resp.VolumeLocation.DeletedVids),
		len(resp.VolumeLocation.NewEcVids), len(resp.VolumeLocation.DeletedEcVids))
}

func (mc *MasterClient) WithClient(streamingMode bool, fn func(client master_pb.SeaweedClient) error) error {
	getMasterF := func() pb.ServerAddress {
		return mc.GetMaster(context.Background())
	}
	return mc.WithClientCustomGetMaster(getMasterF, streamingMode, fn)
}

func (mc *MasterClient) WithClientCustomGetMaster(getMasterF func() pb.ServerAddress, streamingMode bool, fn func(client master_pb.SeaweedClient) error) error {
	return util.Retry("master grpc", func() error {
		return pb.WithMasterClient(streamingMode, getMasterF(), mc.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
			return fn(client)
		})
	})
}

func (mc *MasterClient) getCurrentMaster() pb.ServerAddress {
	mc.currentMasterLock.RLock()
	defer mc.currentMasterLock.RUnlock()
	return mc.currentMaster
}

func (mc *MasterClient) setCurrentMaster(master pb.ServerAddress) {
	mc.currentMasterLock.Lock()
	mc.currentMaster = master
	mc.currentMasterLock.Unlock()
}

// GetMaster returns the current master address, blocking until connected.
//
// IMPORTANT: This method blocks until KeepConnectedToMaster successfully establishes
// a connection to a master server. If KeepConnectedToMaster hasn't been started in a
// background goroutine, this will block indefinitely (or until ctx is canceled).
//
// Typical initialization pattern:
//
//	mc := wdclient.NewMasterClient(...)
//	go mc.KeepConnectedToMaster(ctx)  // Start connection management
//	// ... later ...
//	master := mc.GetMaster(ctx)       // Will block until connected
//
// If called before KeepConnectedToMaster establishes a connection, this may cause
// unexpected timeouts in LookupVolumeIds and other operations that depend on it.
func (mc *MasterClient) GetMaster(ctx context.Context) pb.ServerAddress {
	mc.WaitUntilConnected(ctx)
	return mc.getCurrentMaster()
}

// GetMasters returns all configured master addresses, blocking until connected.
// See GetMaster() for important initialization contract details.
func (mc *MasterClient) GetMasters(ctx context.Context) []pb.ServerAddress {
	mc.WaitUntilConnected(ctx)
	return mc.masters.GetInstances()
}

// WaitUntilConnected blocks until a master connection is established or ctx is canceled.
// This does NOT initiate connections - it only waits for KeepConnectedToMaster to succeed.
func (mc *MasterClient) WaitUntilConnected(ctx context.Context) {
	attempts := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
			currentMaster := mc.getCurrentMaster()
			if currentMaster != "" {
				return
			}
			attempts++
			if attempts%100 == 0 { // Log every 100 attempts (roughly every 20 seconds)
				glog.V(0).Infof("%s.%s WaitUntilConnected still waiting for master connection (attempt %d)...", mc.FilerGroup, mc.clientType, attempts)
			}
			time.Sleep(time.Duration(rand.Int31n(200)) * time.Millisecond)
		}
	}
}

func (mc *MasterClient) KeepConnectedToMaster(ctx context.Context) {
	glog.V(1).Infof("%s.%s masterClient bootstraps with masters %v", mc.FilerGroup, mc.clientType, mc.masters)
	for {
		select {
		case <-ctx.Done():
			glog.V(0).Infof("Connection to masters stopped: %v", ctx.Err())
			return
		default:
			mc.tryAllMasters(ctx)
			time.Sleep(time.Second)
		}
	}
}

func (mc *MasterClient) FindLeaderFromOtherPeers(myMasterAddress pb.ServerAddress) (leader string) {
	for _, master := range mc.masters.GetInstances() {
		if master == myMasterAddress {
			continue
		}
		if grpcErr := pb.WithMasterClient(false, master, mc.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
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
