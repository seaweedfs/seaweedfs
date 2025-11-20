package wdclient

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sync/singleflight"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// VolumeLocationProvider is the interface for looking up volume locations
// This allows different implementations (master subscription, filer queries, etc.)
type VolumeLocationProvider interface {
	// LookupVolumeIds looks up volume locations for the given volume IDs
	// Returns a map of volume ID to locations
	LookupVolumeIds(ctx context.Context, volumeIds []string) (map[string][]Location, error)
}

// vidMapClient provides volume location caching with pluggable lookup
// It wraps the battle-tested vidMap with customizable volume lookup strategies
type vidMapClient struct {
	vidMap          *vidMap
	vidMapLock      sync.RWMutex
	vidMapCacheSize int
	provider        VolumeLocationProvider
	vidLookupGroup  singleflight.Group
}

// newVidMapClient creates a new client with the given provider and data center
func newVidMapClient(provider VolumeLocationProvider, dataCenter string) *vidMapClient {
	return &vidMapClient{
		vidMap:          newVidMap(dataCenter),
		vidMapCacheSize: 5,
		provider:        provider,
	}
}

// GetLookupFileIdFunction returns a function that can be used to lookup file IDs
func (vc *vidMapClient) GetLookupFileIdFunction() LookupFileIdFunctionType {
	return vc.LookupFileIdWithFallback
}

// LookupFileIdWithFallback looks up a file ID, checking cache first, then using provider
func (vc *vidMapClient) LookupFileIdWithFallback(ctx context.Context, fileId string) (fullUrls []string, err error) {
	// Try cache first using the fast path
	vc.vidMapLock.RLock()
	vm := vc.vidMap
	dataCenter := vm.DataCenter
	vc.vidMapLock.RUnlock()

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
	vidLocations, err := vc.LookupVolumeIdsWithFallback(ctx, []string{volumeId})
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

// LookupVolumeIdsWithFallback looks up volume locations, querying provider if not in cache
// Uses singleflight to coalesce concurrent requests for the same batch of volumes
func (vc *vidMapClient) LookupVolumeIdsWithFallback(ctx context.Context, volumeIds []string) (map[string][]Location, error) {
	result := make(map[string][]Location)
	var needsLookup []string
	var lookupErrors []error

	// Check cache first and parse volume IDs once
	vidStringToUint := make(map[string]uint32, len(volumeIds))

	// Get stable pointer to vidMap with minimal lock hold time
	vm := vc.getStableVidMap()

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

	sfResult, err, _ := vc.vidLookupGroup.Do(batchKey, func() (interface{}, error) {
		// Double-check cache for volumes that might have been populated while waiting
		stillNeedLookup := make([]string, 0, len(needsLookup))
		batchResult := make(map[string][]Location)

		// Get stable pointer with minimal lock hold time
		vm := vc.getStableVidMap()

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

		// Query provider with batched volume IDs
		glog.V(2).Infof("Looking up %d volumes from provider: %v", len(stillNeedLookup), stillNeedLookup)

		providerResults, err := vc.provider.LookupVolumeIds(ctx, stillNeedLookup)
		if err != nil {
			return batchResult, fmt.Errorf("provider lookup failed: %v", err)
		}

		// Update cache with results
		for vidString, locations := range providerResults {
			vid, err := strconv.ParseUint(vidString, 10, 32)
			if err != nil {
				glog.Warningf("Failed to parse volume id '%s': %v", vidString, err)
				continue
			}

			for _, loc := range locations {
				vc.addLocation(uint32(vid), loc)
			}

			if len(locations) > 0 {
				batchResult[vidString] = locations
			}
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

	// Return aggregated errors
	return result, errors.Join(lookupErrors...)
}

// getStableVidMap gets a stable pointer to the vidMap, releasing the lock immediately
func (vc *vidMapClient) getStableVidMap() *vidMap {
	vc.vidMapLock.RLock()
	vm := vc.vidMap
	vc.vidMapLock.RUnlock()
	return vm
}

// withCurrentVidMap executes a function with the current vidMap under a read lock
func (vc *vidMapClient) withCurrentVidMap(f func(vm *vidMap)) {
	vc.vidMapLock.RLock()
	defer vc.vidMapLock.RUnlock()
	f(vc.vidMap)
}

// Public methods for external access

// GetLocations safely retrieves volume locations
func (vc *vidMapClient) GetLocations(vid uint32) (locations []Location, found bool) {
	return vc.getStableVidMap().GetLocations(vid)
}

// GetLocationsClone safely retrieves a clone of volume locations
func (vc *vidMapClient) GetLocationsClone(vid uint32) (locations []Location, found bool) {
	return vc.getStableVidMap().GetLocationsClone(vid)
}

// GetVidLocations safely retrieves volume locations by string ID
func (vc *vidMapClient) GetVidLocations(vid string) (locations []Location, err error) {
	return vc.getStableVidMap().GetVidLocations(vid)
}

// LookupFileId safely looks up URLs for a file ID
func (vc *vidMapClient) LookupFileId(ctx context.Context, fileId string) (fullUrls []string, err error) {
	return vc.getStableVidMap().LookupFileId(ctx, fileId)
}

// LookupVolumeServerUrl safely looks up volume server URLs
func (vc *vidMapClient) LookupVolumeServerUrl(vid string) (serverUrls []string, err error) {
	return vc.getStableVidMap().LookupVolumeServerUrl(vid)
}

// GetDataCenter safely retrieves the data center
func (vc *vidMapClient) GetDataCenter() string {
	return vc.getStableVidMap().DataCenter
}

// Thread-safe helpers for vidMap operations

// addLocation adds a volume location
func (vc *vidMapClient) addLocation(vid uint32, location Location) {
	vc.withCurrentVidMap(func(vm *vidMap) {
		vm.addLocation(vid, location)
	})
}

// deleteLocation removes a volume location
func (vc *vidMapClient) deleteLocation(vid uint32, location Location) {
	vc.withCurrentVidMap(func(vm *vidMap) {
		vm.deleteLocation(vid, location)
	})
}

// addEcLocation adds an EC volume location
func (vc *vidMapClient) addEcLocation(vid uint32, location Location) {
	vc.withCurrentVidMap(func(vm *vidMap) {
		vm.addEcLocation(vid, location)
	})
}

// deleteEcLocation removes an EC volume location
func (vc *vidMapClient) deleteEcLocation(vid uint32, location Location) {
	vc.withCurrentVidMap(func(vm *vidMap) {
		vm.deleteEcLocation(vid, location)
	})
}

// resetVidMap resets the volume ID map
func (vc *vidMapClient) resetVidMap() {
	vc.vidMapLock.Lock()
	defer vc.vidMapLock.Unlock()

	// Preserve the existing vidMap in the cache chain
	tail := vc.vidMap

	nvm := newVidMap(tail.DataCenter)
	nvm.cache.Store(tail)
	vc.vidMap = nvm

	// Trim cache chain to vidMapCacheSize
	node := tail
	for i := 0; i < vc.vidMapCacheSize-1; i++ {
		if node.cache.Load() == nil {
			return
		}
		node = node.cache.Load()
	}
	if node != nil {
		node.cache.Store(nil)
	}
}
