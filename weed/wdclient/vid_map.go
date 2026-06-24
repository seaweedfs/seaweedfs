package wdclient

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/pb"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

type HasLookupFileIdFunction interface {
	GetLookupFileIdFunction() LookupFileIdFunctionType
}

type LookupFileIdFunctionType func(ctx context.Context, fileId string) (targetUrls []string, err error)

type Location struct {
	Url        string `json:"url,omitempty"`
	PublicUrl  string `json:"publicUrl,omitempty"`
	DataCenter string `json:"dataCenter,omitempty"`
	GrpcPort   int    `json:"grpcPort,omitempty"`
}

func (l Location) ServerAddress() pb.ServerAddress {
	return pb.NewServerAddressWithGrpcPort(l.Url, l.GrpcPort)
}

type vidMap struct {
	sync.RWMutex
	vid2Locations   map[uint32][]Location
	ecVid2Locations map[uint32][]Location
	// serverRefCount tracks how many vid locations (regular + EC) currently
	// reference each volume server address. Maintaining it incrementally lets
	// hasVolumeServer answer in O(1) instead of walking every volume entry.
	// Keys are the canonical http form of pb.ServerAddress, so callers that
	// pass either "host:port" or "host:port.grpc" find the same entry.
	serverRefCount map[string]int
	DataCenter     string
	cache          atomic.Pointer[vidMap]
}

func newVidMap(dataCenter string) *vidMap {
	return &vidMap{
		vid2Locations:   make(map[uint32][]Location),
		ecVid2Locations: make(map[uint32][]Location),
		serverRefCount:  make(map[string]int),
		DataCenter:      dataCenter,
	}
}

// locationServerKey returns the index key used by serverRefCount for a
// Location. The key normalises away the optional grpc-port suffix so the
// counter stays consistent with hasVolumeServer's lookup.
func locationServerKey(loc Location) string {
	return loc.ServerAddress().ToHttpAddress()
}

func (vc *vidMap) isSameDataCenter(loc *Location) bool {
	if vc.DataCenter == "" || loc.DataCenter == "" || vc.DataCenter != loc.DataCenter {
		return false
	}
	return true
}

func (vc *vidMap) LookupVolumeServerUrl(vid string) (serverUrls []string, err error) {
	id, err := strconv.Atoi(vid)
	if err != nil {
		glog.V(1).Infof("Unknown volume id %s", vid)
		return nil, err
	}

	locations, found := vc.GetLocations(uint32(id))
	if !found {
		return nil, fmt.Errorf("volume %d not found", id)
	}
	var sameDcServers, otherDcServers []string
	for _, loc := range locations {
		if vc.isSameDataCenter(&loc) {
			sameDcServers = append(sameDcServers, loc.Url)
		} else {
			otherDcServers = append(otherDcServers, loc.Url)
		}
	}
	rand.Shuffle(len(sameDcServers), func(i, j int) {
		sameDcServers[i], sameDcServers[j] = sameDcServers[j], sameDcServers[i]
	})
	rand.Shuffle(len(otherDcServers), func(i, j int) {
		otherDcServers[i], otherDcServers[j] = otherDcServers[j], otherDcServers[i]
	})
	// Prefer same data center
	serverUrls = append(sameDcServers, otherDcServers...)
	return
}

func (vc *vidMap) LookupFileId(ctx context.Context, fileId string) (fullUrls []string, err error) {
	parts := strings.Split(fileId, ",")
	if len(parts) != 2 {
		return nil, errors.New("Invalid fileId " + fileId)
	}
	serverUrls, lookupError := vc.LookupVolumeServerUrl(parts[0])
	if lookupError != nil {
		return nil, lookupError
	}
	for _, serverUrl := range serverUrls {
		fullUrls = append(fullUrls, "http://"+serverUrl+"/"+fileId)
	}
	return
}

func (vc *vidMap) GetVidLocations(vid string) (locations []Location, err error) {
	id, err := strconv.Atoi(vid)
	if err != nil {
		glog.V(1).Infof("Unknown volume id %s", vid)
		return nil, fmt.Errorf("Unknown volume id %s", vid)
	}
	foundLocations, found := vc.GetLocations(uint32(id))
	if found {
		return foundLocations, nil
	}
	return nil, fmt.Errorf("volume id %s not found", vid)
}

func (vc *vidMap) GetLocations(vid uint32) (locations []Location, found bool) {
	// glog.V(4).Infof("~ lookup volume id %d: %+v ec:%+v", vid, vc.vid2Locations, vc.ecVid2Locations)
	locations, found = vc.getLocations(vid)
	if found {
		// If volume is explicitly tracked (found=true), return its locations even if empty.
		// An empty array means "volume has no locations" (e.g., during pod restart),
		// which is different from "volume never existed" (found=false).
		// Don't fall back to stale cache for explicitly empty volumes.
		if len(locations) > 0 {
			return locations, found
		}
		// Volume exists but has no locations - return empty, don't check cache
		return nil, false
	}

	// Volume not found in current map - check cache for unknown volumes
	if cachedMap := vc.cache.Load(); cachedMap != nil {
		return cachedMap.GetLocations(vid)
	}

	return nil, false
}

func (vc *vidMap) GetLocationsClone(vid uint32) (locations []Location, found bool) {
	locations, found = vc.GetLocations(vid)

	if found {
		// clone the locations in case the volume locations are changed below
		existingLocations := make([]Location, len(locations))
		copy(existingLocations, locations)
		return existingLocations, found
	}

	return nil, false
}

func (vc *vidMap) getLocations(vid uint32) (locations []Location, found bool) {
	vc.RLock()
	defer vc.RUnlock()

	locations, found = vc.vid2Locations[vid]
	if found && len(locations) > 0 {
		return
	}
	locations, found = vc.ecVid2Locations[vid]
	return
}

// hasVolumeServer reports whether any tracked volume (regular or EC) is hosted
// on addr. It walks the cache chain so recently expired maps are still
// considered. Used to gate admission of operations targeting a volume server.
// The lookup is O(1) thanks to serverRefCount; we still consult the cache
// chain to keep covering volume servers that just rolled out of the live map.
func (vc *vidMap) hasVolumeServer(addr pb.ServerAddress) bool {
	key := addr.ToHttpAddress()
	if key == "" {
		return false
	}
	vc.RLock()
	count := vc.serverRefCount[key]
	vc.RUnlock()
	if count > 0 {
		return true
	}
	if cachedMap := vc.cache.Load(); cachedMap != nil {
		return cachedMap.hasVolumeServer(addr)
	}
	return false
}

func (vc *vidMap) addLocation(vid uint32, location Location) {
	vc.Lock()
	defer vc.Unlock()

	glog.V(4).Infof("+ volume id %d: %+v", vid, location)

	locations, found := vc.vid2Locations[vid]
	if !found {
		vc.vid2Locations[vid] = []Location{location}
		vc.incrementServerRef(locationServerKey(location))
		return
	}

	for _, loc := range locations {
		if loc.Url == location.Url {
			return
		}
	}

	vc.vid2Locations[vid] = append(locations, location)
	vc.incrementServerRef(locationServerKey(location))

}

func (vc *vidMap) addEcLocation(vid uint32, location Location) {
	vc.Lock()
	defer vc.Unlock()

	glog.V(4).Infof("+ ec volume id %d: %+v", vid, location)

	locations, found := vc.ecVid2Locations[vid]
	if !found {
		vc.ecVid2Locations[vid] = []Location{location}
		vc.incrementServerRef(locationServerKey(location))
		return
	}

	for _, loc := range locations {
		if loc.Url == location.Url {
			return
		}
	}

	vc.ecVid2Locations[vid] = append(locations, location)
	vc.incrementServerRef(locationServerKey(location))

}

func (vc *vidMap) deleteLocation(vid uint32, location Location) {
	if cachedMap := vc.cache.Load(); cachedMap != nil {
		cachedMap.deleteLocation(vid, location)
	}

	vc.Lock()
	defer vc.Unlock()

	glog.V(4).Infof("- volume id %d: %+v", vid, location)

	locations, found := vc.vid2Locations[vid]
	if !found {
		return
	}

	for i, loc := range locations {
		if loc.Url == location.Url {
			vc.vid2Locations[vid] = append(locations[0:i], locations[i+1:]...)
			vc.decrementServerRef(locationServerKey(loc))
			break
		}
	}
}

func (vc *vidMap) deleteEcLocation(vid uint32, location Location) {
	if cachedMap := vc.cache.Load(); cachedMap != nil {
		cachedMap.deleteEcLocation(vid, location)
	}

	vc.Lock()
	defer vc.Unlock()

	glog.V(4).Infof("- ec volume id %d: %+v", vid, location)

	locations, found := vc.ecVid2Locations[vid]
	if !found {
		return
	}

	for i, loc := range locations {
		if loc.Url == location.Url {
			vc.ecVid2Locations[vid] = append(locations[0:i], locations[i+1:]...)
			vc.decrementServerRef(locationServerKey(loc))
			break
		}
	}
}

func (vc *vidMap) deleteVid(vid uint32) {
	if cachedMap := vc.cache.Load(); cachedMap != nil {
		cachedMap.deleteVid(vid)
	}

	vc.Lock()
	defer vc.Unlock()

	for _, loc := range vc.vid2Locations[vid] {
		vc.decrementServerRef(locationServerKey(loc))
	}
	for _, loc := range vc.ecVid2Locations[vid] {
		vc.decrementServerRef(locationServerKey(loc))
	}
	delete(vc.vid2Locations, vid)
	delete(vc.ecVid2Locations, vid)
}

// incrementServerRef increases the refcount for key. Empty keys are skipped
// so a zero-value Location (which serialises to "") does not leak a permanent
// bucket that hasVolumeServer and decrementServerRef both ignore. Callers
// must hold vc's write lock.
func (vc *vidMap) incrementServerRef(key string) {
	if key == "" {
		return
	}
	vc.serverRefCount[key]++
}

// decrementServerRef decreases the refcount for key and removes the entry
// once it falls to zero. Callers must hold vc's write lock.
func (vc *vidMap) decrementServerRef(key string) {
	if key == "" {
		return
	}
	if n, ok := vc.serverRefCount[key]; ok {
		if n <= 1 {
			delete(vc.serverRefCount, key)
		} else {
			vc.serverRefCount[key] = n - 1
		}
	}
}
