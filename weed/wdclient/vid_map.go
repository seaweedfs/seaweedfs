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
	Url               string `json:"url,omitempty"`
	PublicUrl         string `json:"publicUrl,omitempty"`
	DataCenter        string `json:"dataCenter,omitempty"`
	GrpcPort          int    `json:"grpcPort,omitempty"`
	DataInRemote      bool   `json:"dataInRemote,omitempty"`
	ReadOnly          bool   `json:"readOnly,omitempty"`
	ReadOnlyCanDelete bool   `json:"readOnlyCanDelete,omitempty"`
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
	state      entryState
	// learnedAt orders when the locations were learned, across volumes.
	// Setting an entry aside or reviving it keeps it.
	learnedAt uint64
}

// entryState tells what the cache learned from what a failed read set aside.
// An entry that is not entryLearned holds only what was known before the
// read failed, less what pushed removals took out since, so the next thing
// learned about the volume replaces it instead of merging with it.
type entryState uint8

const (
	// entryLearned: reported by the master or a lookup.
	entryLearned entryState = iota
	// entryStale: set aside by InvalidateCache. Readers do not see it and it
	// holds no server references. It stays in the map so that a pushed
	// removal still edits it.
	entryStale
	// entryRevived: a stale entry served again because the lookup that
	// followed its invalidation got no answer.
	entryRevived
)

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
	// learned counts entries learned, for locationsEntry.learnedAt.
	learned uint64
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
// preference order: same-DC local, same-DC remote-tier, then the other data
// centers on the same footing. Within each group the order is randomized so
// load spreads across equivalent servers.
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
	// Local replicas go first inside each data center, but never ahead of the
	// data-center preference itself: a remote tier is often in the same region
	// as the local replicas, so crossing a DC boundary to avoid it can cost
	// more than the remote read it saves.
	if len(localUrls) > 0 {
		sameDcServers = util.ReorderToFront(localUrls, sameDcServers)
		otherDcServers = util.ReorderToFront(localUrls, otherDcServers)
	}
	serverUrls = append(sameDcServers, otherDcServers...)
	return
}

// LookupFileId resolves a "<vid>,<cookie>" file id to a list of HTTP read
// URLs using the same DC-then-local ordering as LookupVolumeServerUrl.
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

// GetVidLocations returns the cached Location entries for vid as a string,
// for callers that need richer per-server fields than raw URLs (e.g.
// DataInRemote, PublicUrl).
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

// hasLocation reports whether url serves vid, as a regular copy or ec shards,
// in an entry learned since the last reset.
func (vc *vidMap) hasLocation(vid uint32, url string) bool {
	vc.RLock()
	defer vc.RUnlock()
	for _, vid2Locations := range []map[uint32]*locationsEntry{vc.vid2Locations, vc.ecVid2Locations} {
		entry, found := vid2Locations[vid]
		if !found || entry.state == entryStale || entry.generation != vc.generation {
			continue
		}
		for _, loc := range entry.locations {
			if loc.Url == url {
				return true
			}
		}
	}
	return false
}

// currentLocations returns what vid was learned at since the last reset,
// leaving out anything only an earlier generation reported. When both a
// regular and an ec entry are current, the regular copies serve, as in
// GetLocations.
func (vc *vidMap) currentLocations(vid uint32) ([]Location, bool) {
	vc.RLock()
	defer vc.RUnlock()
	for _, vid2Locations := range []map[uint32]*locationsEntry{vc.vid2Locations, vc.ecVid2Locations} {
		if entry, found := lookupEntry(vid2Locations, vid); found && entry.generation == vc.generation {
			return entry.locations, true
		}
	}
	return nil, false
}

// setLocations makes locations vid's regular entry in the current
// generation, in place of whatever the entry held.
func (vc *vidMap) setLocations(vid uint32, locations []Location) {
	vc.Lock()
	defer vc.Unlock()
	vc.dropSetAsideEntry(vc.ecVid2Locations, vid, vc.learned)
	if entry, found := vc.vid2Locations[vid]; found {
		vc.releaseEntry(entry)
	}
	if len(locations) == 0 {
		delete(vc.vid2Locations, vid)
		return
	}
	stored := make([]Location, len(locations))
	copy(stored, locations)
	vc.learned++
	vc.vid2Locations[vid] = &locationsEntry{locations: stored, generation: vc.generation, learnedAt: vc.learned}
	for _, loc := range stored {
		vc.incrementServerRef(locationServerKey(loc))
	}
}

// lookupEntry returns vid's entry when it still holds locations and is not
// stale. Callers must hold the lock.
func lookupEntry(vid2Locations map[uint32]*locationsEntry, vid uint32) (*locationsEntry, bool) {
	entry, found := vid2Locations[vid]
	if !found || entry.state == entryStale || len(entry.locations) == 0 {
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

	vc.dropSetAsideEntry(vc.ecVid2Locations, vid, vc.learned)
	vc.addLocationToMap(vc.vid2Locations, vid, location)
}

func (vc *vidMap) addEcLocation(vid uint32, location Location) {
	vc.Lock()
	defer vc.Unlock()

	glog.V(4).Infof("+ ec volume id %d: %+v", vid, location)

	vc.dropSetAsideEntry(vc.vid2Locations, vid, vc.learned)
	vc.addLocationToMap(vc.ecVid2Locations, vid, location)
}

// addLocationToMap records location for vid. The first write of a generation
// replaces what an earlier one held instead of merging with it: after a reset
// the new master is the authority, so a volume that moved must not keep
// answering with the server it moved off. A stale or revived entry is replaced
// the same way: the volume is being learned again, and what a failed read set
// aside is older. Callers must hold the write lock.
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
	if !found || entry.state != entryLearned || entry.generation != vc.generation {
		if found {
			vc.releaseEntry(entry)
		}
		vc.learned++
		vid2Locations[vid] = &locationsEntry{
			locations:  []Location{location},
			generation: vc.generation,
			learnedAt:  vc.learned,
		}
		vc.incrementServerRef(locationServerKey(location))
		return
	}

	for i, loc := range entry.locations {
		if loc.Url == location.Url {
			if loc.DataInRemote == location.DataInRemote && loc.ReadOnly == location.ReadOnly && loc.ReadOnlyCanDelete == location.ReadOnlyCanDelete {
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
	vc.learned++
	vid2Locations[vid] = &locationsEntry{locations: locations, generation: entry.generation, learnedAt: vc.learned}
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
// entry any fresher, and it keeps the entry's state. A stale entry is edited
// like any other, so a location removed while it was set aside does not come
// back with it. Callers must hold the write lock.
func (vc *vidMap) deleteLocationFromMap(vid2Locations map[uint32]*locationsEntry, vid uint32, location Location) {
	entry, found := vid2Locations[vid]
	if !found {
		return
	}

	for i, loc := range entry.locations {
		if loc.Url != location.Url {
			continue
		}
		if entry.state != entryStale {
			vc.decrementServerRef(locationServerKey(loc))
		}
		if len(entry.locations) == 1 {
			delete(vid2Locations, vid)
			return
		}
		remaining := make([]Location, 0, len(entry.locations)-1)
		remaining = append(remaining, entry.locations[:i]...)
		remaining = append(remaining, entry.locations[i+1:]...)
		vid2Locations[vid] = &locationsEntry{locations: remaining, generation: entry.generation, state: entry.state, learnedAt: entry.learnedAt}
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

// markStale sets vid's entries aside: lookups miss until the volume is learned
// again, as if the entries were deleted, and their server references are
// released. The entries stay in the map so that reviveStale can bring back
// whatever pushed removals left of them.
func (vc *vidMap) markStale(vid uint32) {
	vc.Lock()
	defer vc.Unlock()

	for _, vid2Locations := range []map[uint32]*locationsEntry{vc.vid2Locations, vc.ecVid2Locations} {
		entry, found := vid2Locations[vid]
		if !found || entry.state == entryStale {
			continue
		}
		vc.releaseEntry(entry)
		vid2Locations[vid] = &locationsEntry{locations: entry.locations, generation: entry.generation, state: entryStale, learnedAt: entry.learnedAt}
	}
}

// reviveStale makes vid's stale entries readable again, with the generation
// they were learned in, and reports whether there were any.
func (vc *vidMap) reviveStale(vid uint32) (revived bool) {
	vc.Lock()
	defer vc.Unlock()

	for _, vid2Locations := range []map[uint32]*locationsEntry{vc.vid2Locations, vc.ecVid2Locations} {
		entry, found := vid2Locations[vid]
		if !found || entry.state != entryStale {
			continue
		}
		vid2Locations[vid] = &locationsEntry{locations: entry.locations, generation: entry.generation, state: entryRevived, learnedAt: entry.learnedAt}
		for _, loc := range entry.locations {
			vc.incrementServerRef(locationServerKey(loc))
		}
		revived = true
	}
	return revived
}

// learnedSoFar marks the entries learned until now, for dropSetAside.
func (vc *vidMap) learnedSoFar() uint64 {
	vc.RLock()
	defer vc.RUnlock()
	return vc.learned
}

// dropSetAside deletes vid's stale and revived entries, for an answer about
// vid that is newer than they are: one asked for after the entries were
// learned, as marked by learnedSoFar. An entry learned since is left alone.
func (vc *vidMap) dropSetAside(vid uint32, asked uint64) {
	vc.Lock()
	defer vc.Unlock()

	vc.dropSetAsideEntry(vc.vid2Locations, vid, asked)
	vc.dropSetAsideEntry(vc.ecVid2Locations, vid, asked)
}

// dropSetAsideEntry deletes vid's entry unless it was learned, or set aside
// from locations learned after asked. Callers must hold the write lock.
func (vc *vidMap) dropSetAsideEntry(vid2Locations map[uint32]*locationsEntry, vid uint32, asked uint64) {
	if entry, found := vid2Locations[vid]; found && entry.state != entryLearned && entry.learnedAt <= asked {
		vc.releaseEntry(entry)
		delete(vid2Locations, vid)
	}
}

// addAnswer caches the locations a lookup answer reported for vid, as
// addLocation does, unless a failed read has set aside locations of vid that
// were learned after the lookup was asked, as marked by learnedSoFar. Those
// are newer than the answer, and adding it would replace them. It reports
// whether it cached the answer.
func (vc *vidMap) addAnswer(vid uint32, locations []Location, asked uint64) bool {
	vc.Lock()
	defer vc.Unlock()

	for _, vid2Locations := range []map[uint32]*locationsEntry{vc.vid2Locations, vc.ecVid2Locations} {
		if entry, found := vid2Locations[vid]; found && entry.state != entryLearned && entry.learnedAt > asked {
			return false
		}
	}
	for _, location := range locations {
		glog.V(4).Infof("+ volume id %d: %+v", vid, location)
		vc.dropSetAsideEntry(vc.ecVid2Locations, vid, vc.learned)
		vc.addLocationToMap(vc.vid2Locations, vid, location)
	}
	return true
}

// clear drops every location, keeping no history to fall back to.
func (vc *vidMap) clear() {
	vc.Lock()
	defer vc.Unlock()

	vc.vid2Locations = make(map[uint32]*locationsEntry)
	vc.ecVid2Locations = make(map[uint32]*locationsEntry)
	vc.serverRefCount = make(map[string]int)
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

// releaseEntry drops the server references an entry holds; a stale entry
// holds none. Callers must hold the write lock.
func (vc *vidMap) releaseEntry(entry *locationsEntry) {
	if entry.state == entryStale {
		return
	}
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
