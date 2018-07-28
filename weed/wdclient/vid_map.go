package wdclient

import (
	"sync"
	"strings"
	"math/rand"
	"errors"
	"strconv"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"fmt"
)

type Location struct {
	Url       string `json:"url,omitempty"`
	PublicUrl string `json:"publicUrl,omitempty"`
}

type VidMap struct {
	sync.RWMutex
	vid2Locations map[uint32][]Location
}

func (vc *VidMap) LookupVolumeServerUrl(vid string) (serverUrl string, err error) {
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

func (vc *VidMap) LookupFileId(fileId string) (fullUrl string, err error) {
	parts := strings.Split(fileId, ",")
	if len(parts) != 2 {
		return "", errors.New("Invalid fileId " + fileId)
	}
	serverUrl, lookupError := LookupVolumeServerUrl(parts[0])
	if lookupError != nil {
		return "", lookupError
	}
	return "http://" + serverUrl + "/" + fileId, nil
}

func (vc *VidMap) GetLocations(vid uint32) (locations []Location) {
	vc.RLock()
	defer vc.RUnlock()

	return vc.vid2Locations[vid]
}

func (vc *VidMap) AddLocation(vid uint32, location Location) {
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

func (vc *VidMap) DeleteLocation(vid uint32, location Location) {
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
