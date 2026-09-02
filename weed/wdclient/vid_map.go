package wdclient

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type HasLookupFileIdFunction interface {
	GetLookupFileIdFunction() LookupFileIdFunctionType
}

type LookupFileIdFunctionType func(ctx context.Context, fileId string) (targetUrls []string, err error)

type Location struct {
	Url          string `json:"url,omitempty"`
	PublicUrl    string `json:"publicUrl,omitempty"`
	DataCenter   string `json:"dataCenter,omitempty"`
	GrpcPort     int    `json:"grpcPort,omitempty"`
	DataInRemote bool   `json:"dataInRemote,omitempty"`
}

func (l Location) ServerAddress() pb.ServerAddress {
	return pb.NewServerAddressWithGrpcPort(l.Url, l.GrpcPort)
}

// locationsEntry is what a volume id maps to: the locations themselves plus the
// generation they were learned in. An entry is immutable once stored; every
// update installs a new one, so locations handed to a reader are never
// rewritten underneath it.
type locationsEntry struct {
	locations  []Location
	generation uint64
}

type vidMap struct {
	sync.RWMutex
	vid2Locations   map[uint32]*locationsEntry
	ecVid2Locations map[uint32]*locationsEntry
	// serverRefCount tracks how many vid locations (regular + EC) currently
	// reference each volume server address. Maintaining it incrementally lets
	// hasVolumeServer answer in O(1) instead of walking every volume entry.
	// Keys are the canonical http form of pb.ServerAddress, so callers that
	// pass either "host:port" or "host:port.grpc" find the same entry.
	serverRefCount map[string]int
	DataCenter     string
	// generation counts resets. Each entry remembers the generation it was
	// learned in, so history expires per volume rather than by keeping
	// snapshot copies of the whole map.
	generation uint64
	// retainGenerations is how many resets an entry survives without being
	// refreshed before reset drops it.
	retainGenerations uint64
}

func newVidMap(dataCenter string, retainGenerations int) *vidMap {
	if retainGenerations <= 0 {
		retainGenerations = DefaultVidMapCacheSize
	}
	return &vidMap{
		vid2Locations:     make(map[uint32]*locationsEntry),
		ecVid2Locations:   make(map[uint32]*locationsEntry),
		serverRefCount:    make(map[string]int),
		DataCenter:        dataCenter,
		retainGenerations: uint64(retainGenerations),
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

// LookupVolumeServerUrl returns the cached volume-server URLs for vid in
// preference order: same-DC local, then other-DC local, then same-DC
// remote-tier replicas, then other-DC remote-tier replicas. Within each tier
// the order is randomized so load spreads across equivalent servers.
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
	localUrls := make(map[string]bool)

	for _, loc := range locations {
		glog.V(4).Infof("lookup %s => %s, data in remote storage tier: %v", vid, loc.Url, loc.DataInRemote)

		if !loc.DataInRemote {
			localUrls[loc.Url] = true
		}

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
	// Prefer same data center, then move every local replica ahead of any
	// remote-tier replica regardless of which DC it sits in. This keeps the
	// DC preference for the remote fallback chain (same-DC remote beats
	// other-DC remote) while making sure the cheap local replica is always
	// tried first when one exists anywhere in the cluster.
	serverUrls = append(sameDcServers, otherDcServers...)
	if len(localUrls) > 0 {
		serverUrls = util.ReorderToFront(localUrls, serverUrls)
	}
	return
}

// LookupFileId resolves a "<vid>,<cookie>" file id to a list of HTTP read
// URLs using the same DC-then-local-first ordering as LookupVolumeServerUrl.
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

// GetVidLocations returns the cached Location entries for vid as a string.
// The locations preserve the DC-then-local-first ordering produced by
// LookupVolumeServerUrl so callers that need richer per-server fields than
// raw URLs (e.g. DataInRemote, PublicUrl) can consume the same priority.
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

// GetLocations returns the cached Location entries for vid as a uint32.
// When both regular and EC entries are present, whichever was learned last
// wins so a volume that switched between regular and EC encoding stops
// answering from the stale copy. Returns found=false when nothing remains,
// including when only an older-generation entry would otherwise apply.
func (vc *vidMap) GetLocations(vid uint32) (locations []Location, found bool) {
	vc.RLock()
	defer vc.RUnlock()

	regular, hasRegular := lookupEntry(vc.vid2Locations, vid)
	ec, hasEc := lookupEntry(vc.ecVid2Locations, vid)

	switch {
	case hasRegular && hasEc:
		// Whichever was learned last wins: once a volume is EC encoded, the
		// regular copies a previous generation knew must stop answering for
		// it, and a decoded volume must stop answering with its shards. A tie
		// means one generation reported both, where the regular copies serve.
		if ec.generation > regular.generation {
			return ec.locations, true
		}
		return regular.locations, true
	case hasRegular:
		return regular.locations, true
	case hasEc:
		return ec.locations, true
	}

	// Nothing older to fall back to: a volume's history lives in its own entry,
	// so a volume whose locations are all gone (a pod restarting, say) is a
	// miss rather than a reason to serve what it used to have.
	return nil, false
}

// lookupEntry returns vid's entry when it still holds locations. Callers must
// hold the lock.
func lookupEntry(vid2Locations map[uint32]*locationsEntry, vid uint32) (*locationsEntry, bool) {
	entry, found := vid2Locations[vid]
	if !found || len(entry.locations) == 0 {
		return nil, false
	}
	return entry, true
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

// hasVolumeServer reports whether any tracked volume (regular or EC) is hosted
// on addr, including volumes still held from earlier generations. Used to gate
// admission of operations targeting a volume server.
func (vc *vidMap) hasVolumeServer(addr pb.ServerAddress) bool {
	key := addr.ToHttpAddress()
	if key == "" {
		return false
	}
	vc.RLock()
	defer vc.RUnlock()
	return vc.serverRefCount[key] > 0
}

func (vc *vidMap) addLocation(vid uint32, location Location) {
	vc.Lock()
	defer vc.Unlock()

	glog.V(4).Infof("+ volume id %d: %+v", vid, location)

	vc.addLocationToMap(vc.vid2Locations, vid, location)
}

func (vc *vidMap) addEcLocation(vid uint32, location Location) {
	vc.Lock()
	defer vc.Unlock()

	glog.V(4).Infof("+ ec volume id %d: %+v", vid, location)

	vc.addLocationToMap(vc.ecVid2Locations, vid, location)
}

// addLocationToMap records location for vid. The first write of a generation
// replaces what an earlier one held instead of merging with it: after a reset
// the new master is the authority, so a volume that moved must not keep
// answering with the server it moved off. Callers must hold the write lock.
//
// If the URL is already present and the remote/local classification matches,
// the entry is left untouched (same replica, same view). When the
// classification flips -- e.g. a volume tiered to remote storage, or a
// remote-backed replica restored locally -- the entry is rebuilt so
// subsequent lookups pick up the new DataInRemote. The server reference key
// only depends on the URL/grpc port, so it stays stable across the flip and
// the refcount does not need to move.
func (vc *vidMap) addLocationToMap(vid2Locations map[uint32]*locationsEntry, vid uint32, location Location) {
	entry, found := vid2Locations[vid]
	if !found || entry.generation != vc.generation {
		if found {
			vc.releaseEntry(entry)
		}
		vid2Locations[vid] = &locationsEntry{
			locations:  []Location{location},
			generation: vc.generation,
		}
		vc.incrementServerRef(locationServerKey(location))
		return
	}

	for i, loc := range entry.locations {
		if loc.Url == location.Url {
			if loc.DataInRemote == location.DataInRemote {
				return
			}
			// A reader holds the slice GetLocations handed it after the lock
			// was dropped, so the replacement is copied rather than written
			// into the array underneath it.
			updated := make([]Location, len(entry.locations))
			copy(updated, entry.locations)
			updated[i] = location
			entry.locations = updated
			return
		}
	}

	locations := make([]Location, 0, len(entry.locations)+1)
	locations = append(locations, entry.locations...)
	locations = append(locations, location)
	vid2Locations[vid] = &locationsEntry{locations: locations, generation: entry.generation}
	vc.incrementServerRef(locationServerKey(location))
}

func (vc *vidMap) deleteLocation(vid uint32, location Location) {
	vc.Lock()
	defer vc.Unlock()

	glog.V(4).Infof("- volume id %d: %+v", vid, location)

	vc.deleteLocationFromMap(vc.vid2Locations, vid, location)
}

func (vc *vidMap) deleteEcLocation(vid uint32, location Location) {
	vc.Lock()
	defer vc.Unlock()

	glog.V(4).Infof("- ec volume id %d: %+v", vid, location)

	vc.deleteLocationFromMap(vc.ecVid2Locations, vid, location)
}

// deleteLocationFromMap drops one location from vid's entry, and the entry
// itself once its last location is gone. The generation is untouched: a delete
// only speaks about the location it names, it does not make the rest of the
// entry any fresher. Callers must hold the write lock.
func (vc *vidMap) deleteLocationFromMap(vid2Locations map[uint32]*locationsEntry, vid uint32, location Location) {
	entry, found := vid2Locations[vid]
	if !found {
		return
	}

	for i, loc := range entry.locations {
		if loc.Url != location.Url {
			continue
		}
		vc.decrementServerRef(locationServerKey(loc))
		if len(entry.locations) == 1 {
			delete(vid2Locations, vid)
			return
		}
		remaining := make([]Location, 0, len(entry.locations)-1)
		remaining = append(remaining, entry.locations[:i]...)
		remaining = append(remaining, entry.locations[i+1:]...)
		vid2Locations[vid] = &locationsEntry{locations: remaining, generation: entry.generation}
		return
	}
}

func (vc *vidMap) deleteVid(vid uint32) {
	vc.Lock()
	defer vc.Unlock()

	if entry, found := vc.vid2Locations[vid]; found {
		vc.releaseEntry(entry)
		delete(vc.vid2Locations, vid)
	}
	if entry, found := vc.ecVid2Locations[vid]; found {
		vc.releaseEntry(entry)
		delete(vc.ecVid2Locations, vid)
	}
}

// reset starts a new generation, as when the master changes and everything it
// told us has to be relearned. Entries stay readable while they are relearned
// and are dropped once they fall out of the retained window.
func (vc *vidMap) reset() {
	vc.Lock()
	defer vc.Unlock()

	vc.generation++
	if vc.generation <= vc.retainGenerations {
		return
	}
	oldest := vc.generation - vc.retainGenerations
	vc.expire(vc.vid2Locations, oldest)
	vc.expire(vc.ecVid2Locations, oldest)
}

// expire drops entries last refreshed before oldest. Callers must hold the
// write lock.
func (vc *vidMap) expire(vid2Locations map[uint32]*locationsEntry, oldest uint64) {
	for vid, entry := range vid2Locations {
		if entry.generation >= oldest {
			continue
		}
		vc.releaseEntry(entry)
		delete(vid2Locations, vid)
	}
}

// releaseEntry drops the server references an entry holds. Callers must hold
// the write lock.
func (vc *vidMap) releaseEntry(entry *locationsEntry) {
	for _, loc := range entry.locations {
		vc.decrementServerRef(locationServerKey(loc))
	}
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
