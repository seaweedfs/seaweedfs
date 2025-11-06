package wdclient

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/seaweedfs/seaweedfs/weed/util/version"

	"github.com/seaweedfs/seaweedfs/weed/stats"

	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

type MasterClient struct {
	FilerGroup        string
	clientType        string
	clientHost        pb.ServerAddress
	rack              string
	currentMaster     pb.ServerAddress
	currentMasterLock sync.RWMutex
	masters           pb.ServerDiscovery
	grpcDialOption    grpc.DialOption

	// vidMap stores volume location mappings
	// Protected by vidMapLock to prevent race conditions during pointer swaps in resetVidMap
	vidMap           *vidMap
	vidMapLock       sync.RWMutex
	vidMapCacheSize  int
	OnPeerUpdate     func(update *master_pb.ClusterNodeUpdate, startFrom time.Time)
	OnPeerUpdateLock sync.RWMutex

	// Per-batch in-flight tracking to prevent duplicate lookups for the same set of volumes
	vidLookupGroup singleflight.Group
}

func NewMasterClient(grpcDialOption grpc.DialOption, filerGroup string, clientType string, clientHost pb.ServerAddress, clientDataCenter string, rack string, masters pb.ServerDiscovery) *MasterClient {
	return &MasterClient{
		FilerGroup:      filerGroup,
		clientType:      clientType,
		clientHost:      clientHost,
		rack:            rack,
		masters:         masters,
		grpcDialOption:  grpcDialOption,
		vidMap:          newVidMap(clientDataCenter),
		vidMapCacheSize: 5,
	}
}

func (mc *MasterClient) SetOnPeerUpdateFn(onPeerUpdate func(update *master_pb.ClusterNodeUpdate, startFrom time.Time)) {
	mc.OnPeerUpdateLock.Lock()
	mc.OnPeerUpdate = onPeerUpdate
	mc.OnPeerUpdateLock.Unlock()
}

func (mc *MasterClient) GetLookupFileIdFunction() LookupFileIdFunctionType {
	return mc.LookupFileIdWithFallback
}

func (mc *MasterClient) LookupFileIdWithFallback(ctx context.Context, fileId string) (fullUrls []string, err error) {
	// Try cache first using the fast path - grab both vidMap and dataCenter in one lock
	mc.vidMapLock.RLock()
	vm := mc.vidMap
	dataCenter := vm.DataCenter
	mc.vidMapLock.RUnlock()

	fullUrls, err = vm.LookupFileId(ctx, fileId)
	if err == nil && len(fullUrls) > 0 {
		return
	}

	// Extract volume ID from file ID (format: "volumeId,needle_id_cookie")
	parts := strings.Split(fileId, ",")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid fileId %s", fileId)
	}
	volumeId := parts[0]

	// Use shared lookup logic with batching and singleflight
	vidLocations, err := mc.LookupVolumeIdsWithFallback(ctx, []string{volumeId})
	if err != nil {
		return nil, fmt.Errorf("LookupVolume %s failed: %v", fileId, err)
	}

	locations, found := vidLocations[volumeId]
	if !found || len(locations) == 0 {
		return nil, fmt.Errorf("volume %s not found for fileId %s", volumeId, fileId)
	}

	// Build HTTP URLs from locations, preferring same data center
	var sameDcUrls, otherDcUrls []string
	for _, loc := range locations {
		httpUrl := "http://" + loc.Url + "/" + fileId
		if dataCenter != "" && dataCenter == loc.DataCenter {
			sameDcUrls = append(sameDcUrls, httpUrl)
		} else {
			otherDcUrls = append(otherDcUrls, httpUrl)
		}
	}

	// Prefer same data center
	fullUrls = append(sameDcUrls, otherDcUrls...)
	return fullUrls, nil
}

// LookupVolumeIdsWithFallback looks up volume locations, querying master if not in cache
// Uses singleflight to coalesce concurrent requests for the same batch of volumes
func (mc *MasterClient) LookupVolumeIdsWithFallback(ctx context.Context, volumeIds []string) (map[string][]Location, error) {
	result := make(map[string][]Location)
	var needsLookup []string
	var lookupErrors []error

	// Check cache first and parse volume IDs once
	vidStringToUint := make(map[string]uint32, len(volumeIds))

	// Get stable pointer to vidMap with minimal lock hold time
	vm := mc.getStableVidMap()

	for _, vidString := range volumeIds {
		vid, err := strconv.ParseUint(vidString, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid volume id %s: %v", vidString, err)
		}
		vidStringToUint[vidString] = uint32(vid)

		locations, found := vm.GetLocations(uint32(vid))
		if found && len(locations) > 0 {
			result[vidString] = locations
		} else {
			needsLookup = append(needsLookup, vidString)
		}
	}

	if len(needsLookup) == 0 {
		return result, nil
	}

	// Batch query all missing volumes using singleflight on the batch key
	// Sort for stable key to coalesce identical batches
	sort.Strings(needsLookup)
	batchKey := strings.Join(needsLookup, ",")

	sfResult, err, _ := mc.vidLookupGroup.Do(batchKey, func() (interface{}, error) {
		// Double-check cache for volumes that might have been populated while waiting
		stillNeedLookup := make([]string, 0, len(needsLookup))
		batchResult := make(map[string][]Location)

		// Get stable pointer with minimal lock hold time
		vm := mc.getStableVidMap()

		for _, vidString := range needsLookup {
			vid := vidStringToUint[vidString] // Use pre-parsed value
			if locations, found := vm.GetLocations(vid); found && len(locations) > 0 {
				batchResult[vidString] = locations
			} else {
				stillNeedLookup = append(stillNeedLookup, vidString)
			}
		}

		if len(stillNeedLookup) == 0 {
			return batchResult, nil
		}

		// Query master with batched volume IDs
		glog.V(2).Infof("Looking up %d volumes from master: %v", len(stillNeedLookup), stillNeedLookup)

		err := pb.WithMasterClient(false, mc.GetMaster(ctx), mc.grpcDialOption, false, func(client master_pb.SeaweedClient) error {
			resp, err := client.LookupVolume(ctx, &master_pb.LookupVolumeRequest{
				VolumeOrFileIds: stillNeedLookup,
			})
			if err != nil {
				return fmt.Errorf("master lookup failed: %v", err)
			}

			for _, vidLoc := range resp.VolumeIdLocations {
				if vidLoc.Error != "" {
					glog.V(0).Infof("volume %s lookup error: %s", vidLoc.VolumeOrFileId, vidLoc.Error)
					continue
				}

				// Parse volume ID from response
				parts := strings.Split(vidLoc.VolumeOrFileId, ",")
				vidOnly := parts[0]
				vid, err := strconv.ParseUint(vidOnly, 10, 32)
				if err != nil {
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
					mc.addLocation(uint32(vid), loc)
					locations = append(locations, loc)
				}

				if len(locations) > 0 {
					batchResult[vidOnly] = locations
				}
			}
			return nil
		})

		if err != nil {
			return batchResult, err
		}
		return batchResult, nil
	})

	if err != nil {
		lookupErrors = append(lookupErrors, err)
	}

	// Merge singleflight batch results
	if batchLocations, ok := sfResult.(map[string][]Location); ok {
		for vid, locs := range batchLocations {
			result[vid] = locs
		}
	}

	// Check for volumes that still weren't found
	for _, vidString := range needsLookup {
		if _, found := result[vidString]; !found {
			lookupErrors = append(lookupErrors, fmt.Errorf("volume %s not found", vidString))
		}
	}

	// Return aggregated errors using errors.Join to preserve error types
	return result, errors.Join(lookupErrors...)
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

func (mc *MasterClient) GetMaster(ctx context.Context) pb.ServerAddress {
	mc.WaitUntilConnected(ctx)
	return mc.getCurrentMaster()
}

func (mc *MasterClient) GetMasters(ctx context.Context) []pb.ServerAddress {
	mc.WaitUntilConnected(ctx)
	return mc.masters.GetInstances()
}

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

			if resp.VolumeLocation != nil {
				// maybe the leader is changed
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
	return
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

// getStableVidMap gets a stable pointer to the vidMap, releasing the lock immediately.
// This is safe for read operations as the returned pointer is a stable snapshot,
// and the underlying vidMap methods have their own internal locking.
func (mc *MasterClient) getStableVidMap() *vidMap {
	mc.vidMapLock.RLock()
	vm := mc.vidMap
	mc.vidMapLock.RUnlock()
	return vm
}

// withCurrentVidMap executes a function with the current vidMap under a read lock.
// This is for methods that modify vidMap's internal state, ensuring the pointer
// is not swapped by resetVidMap during the operation. The actual map mutations
// are protected by vidMap's internal mutex.
func (mc *MasterClient) withCurrentVidMap(f func(vm *vidMap)) {
	mc.vidMapLock.RLock()
	defer mc.vidMapLock.RUnlock()
	f(mc.vidMap)
}

// Public methods for external packages to access vidMap safely

// GetLocations safely retrieves volume locations
func (mc *MasterClient) GetLocations(vid uint32) (locations []Location, found bool) {
	return mc.getStableVidMap().GetLocations(vid)
}

// GetLocationsClone safely retrieves a clone of volume locations
func (mc *MasterClient) GetLocationsClone(vid uint32) (locations []Location, found bool) {
	return mc.getStableVidMap().GetLocationsClone(vid)
}

// GetVidLocations safely retrieves volume locations by string ID
func (mc *MasterClient) GetVidLocations(vid string) (locations []Location, err error) {
	return mc.getStableVidMap().GetVidLocations(vid)
}

// LookupFileId safely looks up URLs for a file ID
func (mc *MasterClient) LookupFileId(ctx context.Context, fileId string) (fullUrls []string, err error) {
	return mc.getStableVidMap().LookupFileId(ctx, fileId)
}

// LookupVolumeServerUrl safely looks up volume server URLs
func (mc *MasterClient) LookupVolumeServerUrl(vid string) (serverUrls []string, err error) {
	return mc.getStableVidMap().LookupVolumeServerUrl(vid)
}

// GetDataCenter safely retrieves the data center
func (mc *MasterClient) GetDataCenter() string {
	return mc.getStableVidMap().DataCenter
}

// Thread-safe helpers for vidMap operations

// addLocation adds a volume location
func (mc *MasterClient) addLocation(vid uint32, location Location) {
	mc.withCurrentVidMap(func(vm *vidMap) {
		vm.addLocation(vid, location)
	})
}

// deleteLocation removes a volume location
func (mc *MasterClient) deleteLocation(vid uint32, location Location) {
	mc.withCurrentVidMap(func(vm *vidMap) {
		vm.deleteLocation(vid, location)
	})
}

// addEcLocation adds an EC volume location
func (mc *MasterClient) addEcLocation(vid uint32, location Location) {
	mc.withCurrentVidMap(func(vm *vidMap) {
		vm.addEcLocation(vid, location)
	})
}

// deleteEcLocation removes an EC volume location
func (mc *MasterClient) deleteEcLocation(vid uint32, location Location) {
	mc.withCurrentVidMap(func(vm *vidMap) {
		vm.deleteEcLocation(vid, location)
	})
}

func (mc *MasterClient) resetVidMap() {
	mc.vidMapLock.Lock()
	defer mc.vidMapLock.Unlock()

	// Preserve the existing vidMap in the cache chain
	// No need to clone - the existing vidMap has its own mutex for thread safety
	tail := mc.vidMap

	nvm := newVidMap(tail.DataCenter)
	nvm.cache.Store(tail)
	mc.vidMap = nvm

	// Trim cache chain to vidMapCacheSize by traversing to the last node
	// that should remain and cutting the chain after it
	node := tail
	for i := 0; i < mc.vidMapCacheSize-1; i++ {
		if node.cache.Load() == nil {
			return
		}
		node = node.cache.Load()
	}
	if node != nil {
		node.cache.Store(nil)
	}
}
