package wdclient

import (
	"sync"
)

type Location struct {
	Url       string `json:"url,omitempty"`
	PublicUrl string `json:"publicUrl,omitempty"`
}

type VidMap struct {
	sync.RWMutex
	vid2Locations map[uint32][]Location
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
