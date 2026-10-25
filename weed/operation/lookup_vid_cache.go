package operation

import (
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

var ErrorNotFound = errors.New("not found")

type VidInfo struct {
	Locations       []Location
	NextRefreshTime time.Time
}
type VidCache struct {
	sync.RWMutex
	cache map[uint32]VidInfo
}

func (vc *VidCache) Get(vid string) ([]Location, error) {
	id, err := strconv.ParseUint(vid, 10, 32)
	if err != nil {
		glog.V(1).Infof("Unknown volume id %s", vid)
		return nil, err
	}
	if id == 0 {
		return nil, ErrorNotFound
	}
	vc.RLock()
	defer vc.RUnlock()
	info, found := vc.cache[uint32(id)]
	if !found || info.Locations == nil {
		return nil, ErrorNotFound
	}
	if info.NextRefreshTime.Before(time.Now()) {
		return nil, errors.New("expired")
	}
	return info.Locations, nil
}

func (vc *VidCache) Set(vid string, locations []Location, duration time.Duration) {
	id, err := strconv.ParseUint(vid, 10, 32)
	if err != nil {
		glog.V(1).Infof("Unknown volume id %s", vid)
		return
	}
	if id == 0 {
		return
	}
	vc.Lock()
	defer vc.Unlock()
	if vc.cache == nil {
		vc.cache = make(map[uint32]VidInfo)
	}
	vc.cache[uint32(id)] = VidInfo{
		Locations:       locations,
		NextRefreshTime: time.Now().Add(duration),
	}
}

func (vc *VidCache) Delete(vid string) {
	id, err := strconv.ParseUint(vid, 10, 32)
	if err != nil || id == 0 {
		return
	}
	vc.Lock()
	defer vc.Unlock()
	delete(vc.cache, uint32(id))
}
