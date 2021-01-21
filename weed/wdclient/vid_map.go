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

type HasLookupFileIdFunction interface {
	GetLookupFileIdFunction() LookupFileIdFunctionType
}

type LookupFileIdFunctionType func(fileId string) (targetUrls []string, err error)

type Location struct {
	Url        string `json:"url,omitempty"`
	PublicUrl  string `json:"publicUrl,omitempty"`
	DataCenter string `json:"dataCenter,omitempty"`
}

type vidMap struct {
	sync.RWMutex
	vid2Locations map[uint32][]Location
	DataCenter    string
	cursor        int32
}

func newVidMap(dataCenter string) vidMap {
	return vidMap{
		vid2Locations: make(map[uint32][]Location),
		DataCenter:    dataCenter,
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
	for _, loc := range locations {
		if vc.DataCenter == "" || loc.DataCenter == "" || vc.DataCenter != loc.DataCenter {
			serverUrls = append(serverUrls, loc.Url)
		} else {
			serverUrls = append([]string{loc.Url}, serverUrls...)
		}
	}
	return
}

func (vc *vidMap) GetLookupFileIdFunction() LookupFileIdFunctionType {
	return vc.LookupFileId
}

func (vc *vidMap) LookupFileId(fileId string) (fullUrls []string, err error) {
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
	vc.RLock()
	defer vc.RUnlock()

	locations, found = vc.vid2Locations[vid]
	return
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
