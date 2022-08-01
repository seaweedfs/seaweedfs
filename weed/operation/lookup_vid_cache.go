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
	cache []VidInfo
}

func (vc *VidCache) Get(vid string) ([]Location, error) {
	id, err := strconv.Atoi(vid)
	if err != nil {
		glog.V(1).Infof("Unknown volume id %s", vid)
		return nil, err
	}
	vc.RLock()
	defer vc.RUnlock()
	if 0 < id && id <= len(vc.cache) {
		if vc.cache[id-1].Locations == nil {
			return nil, errors.New("not set")
		}
		if vc.cache[id-1].NextRefreshTime.Before(time.Now()) {
			return nil, errors.New("expired")
		}
		return vc.cache[id-1].Locations, nil
	}
	return nil, ErrorNotFound
}
func (vc *VidCache) Set(vid string, locations []Location, duration time.Duration) {
	id, err := strconv.Atoi(vid)
	if err != nil {
		glog.V(1).Infof("Unknown volume id %s", vid)
		return
	}
	vc.Lock()
	defer vc.Unlock()
	if id > len(vc.cache) {
		for i := id - len(vc.cache); i > 0; i-- {
			vc.cache = append(vc.cache, VidInfo{})
		}
	}
	if id > 0 {
		vc.cache[id-1].Locations = locations
		vc.cache[id-1].NextRefreshTime = time.Now().Add(duration)
	}
}
