package wdclient

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

type Location struct {
	Url       string `json:"url,omitempty"`
	PublicUrl string `json:"publicUrl,omitempty"`
}

type vidMap struct {
	sync.RWMutex
	vid2Locations map[uint32][]Location
}

func newVidMap() vidMap {
	return vidMap{
		vid2Locations: make(map[uint32][]Location),
	}
}

func (vc *vidMap) LookupVolumeServerUrl(vid string) (serverUrl string, err error) {
	id, err := strconv.Atoi(vid)
	if err != nil {
		glog.V(1).Infof("Unknown volume id %s", vid)
		return "", err
	}

	locations := vc.GetLocations(uint32(id))
	if len(locations) == 0 {
		return "", fmt.Errorf("volume %d not found", id)
	}

	return locations[rand.Intn(len(locations))].Url, nil
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

func (vc *vidMap) GetVidLocations(vid string) (locations []Location) {
	id, err := strconv.Atoi(vid)
	if err != nil {
		glog.V(1).Infof("Unknown volume id %s", vid)
		return nil
	}
	return vc.GetLocations(uint32(id))
}

func (vc *vidMap) GetLocations(vid uint32) (locations []Location) {
	vc.RLock()
	defer vc.RUnlock()

	return vc.vid2Locations[vid]
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
		}
	}

}
