package wdclient

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sync/singleflight"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
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
	vidMap         *vidMap
	provider       VolumeLocationProvider
	vidLookupGroup singleflight.Group
}

const (
	// DefaultVidMapCacheSize is the default number of resets a volume location
	// survives without being relearned. This provides cache history when
	// volumes move between servers.
	DefaultVidMapCacheSize = 5
)

// newVidMapClient creates a new client with the given provider and data center
func newVidMapClient(provider VolumeLocationProvider, dataCenter string, cacheSize int) *vidMapClient {
	return &vidMapClient{
		vidMap:   newVidMap(dataCenter, cacheSize),
		provider: provider,
	}
}

// GetLookupFileIdFunction returns a function that can be used to lookup file IDs
func (vc *vidMapClient) GetLookupFileIdFunction() LookupFileIdFunctionType {
	return vc.LookupFileIdWithFallback
}

// LookupFileIdWithFallback looks up a file ID, checking cache first, then using provider
func (vc *vidMapClient) LookupFileIdWithFallback(ctx context.Context, fileId string) (fullUrls []string, err error) {
	// Try cache first
	dataCenter := vc.vidMap.DataCenter
	fullUrls, err = vc.vidMap.LookupFileId(ctx, fileId)

	// Cache hit - return immediately
	if err == nil && len(fullUrls) > 0 {
		return
	}

	// Cache miss - extract volume ID from file ID (format: "volumeId,needle_id_cookie")
	if fileId == "" {
		return nil, fmt.Errorf("empty fileId")
	}
	parts := strings.Split(fileId, ",")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid fileId %s", fileId)
	}
	volumeId := parts[0]

	// Use shared lookup logic with batching and singleflight
	vidLocations, err := vc.LookupVolumeIdsWithFallback(ctx, []string{volumeId})

	// Check for partial results first (important for multi-volume batched lookups)
	locations, found := vidLocations[volumeId]
	if !found || len(locations) == 0 {
		// Volume not found - return specific error with context from lookup if available
		if err != nil {
			return nil, fmt.Errorf("volume %s not found for fileId %s: %w", volumeId, fileId, err)
		}
		return nil, fmt.Errorf("volume %s not found for fileId %s", volumeId, fileId)
	}

	// Volume found successfully - ignore any errors about other volumes
	// (not relevant for single-volume lookup, but defensive for future batching)

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

	// Shuffle to distribute load across volume servers
	rand.Shuffle(len(sameDcUrls), func(i, j int) { sameDcUrls[i], sameDcUrls[j] = sameDcUrls[j], sameDcUrls[i] })
	rand.Shuffle(len(otherDcUrls), func(i, j int) { otherDcUrls[i], otherDcUrls[j] = otherDcUrls[j], otherDcUrls[i] })

	// Prefer same data center
	fullUrls = append(sameDcUrls, otherDcUrls...)
	return fullUrls, nil
}

// LookupVolumeIdsWithFallback looks up volume locations, querying provider if not in cache.
// Uses singleflight to coalesce concurrent requests for the same batch of volumes.
//
// IMPORTANT: This function may return PARTIAL results with a non-nil error.
// The result map contains successfully looked up volumes, while the error aggregates
// failures for volumes that couldn't be found or had lookup errors.
//
// Callers MUST check both the result map AND the error:
//   - result != nil && err == nil: All volumes found successfully
//   - result != nil && err != nil: Some volumes found, some failed (check both)
//   - result == nil && err != nil: Complete failure (connection error, etc.)
//
// Example usage:
//
//	locs, err := mc.LookupVolumeIdsWithFallback(ctx, []string{"1", "2", "999"})
//	if len(locs) > 0 {
//	    // Process successfully found volumes
//	}
//	if err != nil {
//	    // Log/handle failed volumes
//	}
func (vc *vidMapClient) LookupVolumeIdsWithFallback(ctx context.Context, volumeIds []string) (map[string][]Location, error) {
	result := make(map[string][]Location)
	var needsLookup []string
	var lookupErrors []error

	// Check cache first and parse volume IDs once
	vidStringToUint := make(map[string]uint32, len(volumeIds))

	for _, vidString := range volumeIds {
		vid, err := strconv.ParseUint(vidString, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid volume id %s: %v", vidString, err)
		}
		vidStringToUint[vidString] = uint32(vid)

		locations, found := vc.vidMap.GetLocations(uint32(vid))
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

		for _, vidString := range needsLookup {
			vid := vidStringToUint[vidString] // Use pre-parsed value
			if locations, found := vc.vidMap.GetLocations(vid); found && len(locations) > 0 {
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
			return batchResult, fmt.Errorf("provider lookup failed: %w", err)
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

// Public methods for external access
//
// The vidMap itself is never replaced, so these all read the one map under its
// own lock. Resets bump its generation instead of swapping in a fresh instance.

// GetLocations safely retrieves volume locations
func (vc *vidMapClient) GetLocations(vid uint32) (locations []Location, found bool) {
	return vc.vidMap.GetLocations(vid)
}

// GetLocationsClone safely retrieves a clone of volume locations
func (vc *vidMapClient) GetLocationsClone(vid uint32) (locations []Location, found bool) {
	return vc.vidMap.GetLocationsClone(vid)
}

// GetVidLocations safely retrieves volume locations by string ID
func (vc *vidMapClient) GetVidLocations(vid string) (locations []Location, err error) {
	return vc.vidMap.GetVidLocations(vid)
}

// LookupFileId safely looks up URLs for a file ID
func (vc *vidMapClient) LookupFileId(ctx context.Context, fileId string) (fullUrls []string, err error) {
	return vc.vidMap.LookupFileId(ctx, fileId)
}

// LookupVolumeServerUrl safely looks up volume server URLs
func (vc *vidMapClient) LookupVolumeServerUrl(vid string) (serverUrls []string, err error) {
	return vc.vidMap.LookupVolumeServerUrl(vid)
}

// HasVolumeServer reports whether addr is currently a known volume server
// (hosts at least one volume or EC shard) in the cached vid map. Used by
// admission paths that must only contact peers learned from the master.
func (vc *vidMapClient) HasVolumeServer(addr pb.ServerAddress) bool {
	return vc.vidMap.hasVolumeServer(addr)
}

// GetDataCenter safely retrieves the data center
func (vc *vidMapClient) GetDataCenter() string {
	return vc.vidMap.DataCenter
}

// Thread-safe helpers for vidMap operations

// addLocation adds a volume location
func (vc *vidMapClient) addLocation(vid uint32, location Location) {
	vc.vidMap.addLocation(vid, location)
}

// deleteLocation removes a volume location
func (vc *vidMapClient) deleteLocation(vid uint32, location Location) {
	vc.vidMap.deleteLocation(vid, location)
}

// addEcLocation adds an EC volume location
func (vc *vidMapClient) addEcLocation(vid uint32, location Location) {
	vc.vidMap.addEcLocation(vid, location)
}

// deleteEcLocation removes an EC volume location
func (vc *vidMapClient) deleteEcLocation(vid uint32, location Location) {
	vc.vidMap.deleteEcLocation(vid, location)
}

// resetVidMap starts a new generation, as when the master changes: what the
// previous one told us stays readable until it is relearned or expires.
func (vc *vidMapClient) resetVidMap() {
	vc.vidMap.reset()
}

// InvalidateCache removes all cached locations for a volume ID
func (vc *vidMapClient) InvalidateCache(fileId string) {
	parts := strings.Split(fileId, ",")
	vidString := parts[0]
	vid, err := strconv.ParseUint(vidString, 10, 32)
	if err != nil {
		return
	}
	vc.vidMap.deleteVid(uint32(vid))
}
