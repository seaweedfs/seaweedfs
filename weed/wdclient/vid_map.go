package wdclient

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

const (
	maxCursorIndex = 4096
)

type Location struct {
	Url       string `json:"url,omitempty"`
	PublicUrl string `json:"publicUrl,omitempty"`
}

type vidMap struct {
	sync.RWMutex
	vid2Locations map[uint32][]Location

	cursor int32
}

func newVidMap() vidMap {
	return vidMap{
		vid2Locations: make(map[uint32][]Location),
		cursor:        -1,
	}
}

func (vc *vidMap) getLocationIndex(length int) (int, error) {
	if length <= 0 {
		return 0, fmt.Errorf("invalid length: %d", length)
	}
	if atomic.LoadInt32(&vc.cursor) == maxCursorIndex {
		atomic.CompareAndSwapInt32(&vc.cursor, maxCursorIndex, -1)
	}
	return int(atomic.AddInt32(&vc.cursor, 1)) % length, nil
}

func (vc *vidMap) LookupVolumeServerUrl(vid string) (serverUrl string, err error) {
	id, err := strconv.Atoi(vid)
	if err != nil {
		glog.V(1).Infof("Unknown volume id %s", vid)
		return "", err
	}

	return vc.GetRandomLocation(uint32(id))
}

func (vc *vidMap) LookupFileId(fileId string) (fullUrl string, err error) {
	parts := strings.Split(fileId, ",")
	if len(parts) != 2 {
		return "", errors.New("Invalid fileId " + fileId)
	}
	serverUrl, lookupError := vc.LookupVolumeServerUrl(parts[0])
	if lookupError != nil {
		return "", lookupError
	}
	return "http://" + serverUrl + "/" + fileId, nil
}

func (vc *vidMap) LookupVolumeServer(fileId string) (volumeServer string, err error) {
	parts := strings.Split(fileId, ",")
	if len(parts) != 2 {
		return "", errors.New("Invalid fileId " + fileId)
	}
	serverUrl, lookupError := vc.LookupVolumeServerUrl(parts[0])
	if lookupError != nil {
		return "", lookupError
	}
	return serverUrl, nil
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
	vc.RLock()
	defer vc.RUnlock()

	locations, found = vc.vid2Locations[vid]
	return
}

func (vc *vidMap) GetRandomLocation(vid uint32) (serverUrl string, err error) {
	vc.RLock()
	defer vc.RUnlock()

	locations := vc.vid2Locations[vid]
	if len(locations) == 0 {
		return "", fmt.Errorf("volume %d not found", vid)
	}

	index, err := vc.getLocationIndex(len(locations))
	if err != nil {
		return "", fmt.Errorf("volume %d: %v", vid, err)
	}

	return locations[index].Url, nil
}

func (vc *vidMap) addLocation(vid uint32, location Location) {
	vc.Lock()
	defer vc.Unlock()

	locations, found := vc.vid2Locations[vid]
	if !found {
		vc.vid2Locations[vid] = []Location{location}
		return
	}

	for _, loc := range locations {
		if loc.Url == location.Url {
			return
		}
	}

	vc.vid2Locations[vid] = append(locations, location)

}

func (vc *vidMap) deleteLocation(vid uint32, location Location) {
	vc.Lock()
	defer vc.Unlock()

	locations, found := vc.vid2Locations[vid]
	if !found {
		return
	}

	for i, loc := range locations {
		if loc.Url == location.Url {
			vc.vid2Locations[vid] = append(locations[0:i], locations[i+1:]...)
			break
		}
	}

}
