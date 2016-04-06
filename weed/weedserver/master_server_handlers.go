package weedserver

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

func (ms *MasterServer) lookupVolumeId(vids []string, collection string) (volumeLocations map[string]operation.LookupResult) {
	volumeLocations = make(map[string]operation.LookupResult)
	for _, vid := range vids {
		commaSep := strings.Index(vid, ",")
		if commaSep > 0 {
			vid = vid[0:commaSep]
		}
		if _, ok := volumeLocations[vid]; ok {
			continue
		}
		volumeId, err := storage.NewVolumeId(vid)
		if err == nil {
			locationList := ms.Topo.Lookup(collection, volumeId)
			if locationList != nil && locationList.Length() > 0 {
				var ret operation.Locations
				for _, dn := range locationList.AllDataNode() {
					ret = append(ret, operation.Location{Url: dn.Url(), PublicUrl: dn.PublicUrl})
				}
				volumeLocations[vid] = operation.LookupResult{VolumeId: vid, Locations: ret}
			} else {
				volumeLocations[vid] = operation.LookupResult{VolumeId: vid, Error: "volumeId not found."}
			}
		} else {
			volumeLocations[vid] = operation.LookupResult{VolumeId: vid, Error: "Unknown volumeId format."}
		}
	}
	return
}

// Takes one volumeId only, can not do batch lookup
func (ms *MasterServer) dirLookupHandler(w http.ResponseWriter, r *http.Request) {
	vid := r.FormValue("volumeId")
	commaSep := strings.Index(vid, ",")
	if commaSep > 0 {
		vid = vid[0:commaSep]
	}
	vids := []string{vid}
	collection := r.FormValue("collection") //optional, but can be faster if too many collections
	volumeLocations := ms.lookupVolumeId(vids, collection)
	location := volumeLocations[vid]
	httpStatus := http.StatusOK
	if location.Error != "" {
		httpStatus = http.StatusNotFound
	}
	writeJsonQuiet(w, r, httpStatus, location)
}

// This can take batched volumeIds, &volumeId=x&volumeId=y&volumeId=z
func (ms *MasterServer) volumeLookupHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	vids := r.Form["volumeId"]
	collection := r.FormValue("collection") //optional, but can be faster if too many collections
	volumeLocations := ms.lookupVolumeId(vids, collection)
	writeJsonQuiet(w, r, http.StatusOK, volumeLocations)
}

func (ms *MasterServer) dirAssignHandler(w http.ResponseWriter, r *http.Request) {
	stats.AssignRequest()
	requestedCount, e := strconv.ParseUint(r.FormValue("count"), 10, 64)
	if e != nil || requestedCount == 0 {
		requestedCount = 1
	}

	option, err := ms.getVolumeGrowOption(r)
	if err != nil {
		writeJsonQuiet(w, r, http.StatusNotAcceptable, operation.AssignResult{Error: err.Error()})
		return
	}

	if !ms.Topo.HasWritableVolume(option) {
		if ms.Topo.FreeSpace() <= 0 {
			writeJsonQuiet(w, r, http.StatusNotFound, operation.AssignResult{Error: "No free volumes left!"})
			return
		}
		ms.vgLock.Lock()
		defer ms.vgLock.Unlock()
		if !ms.Topo.HasWritableVolume(option) {
			if _, err = ms.vg.AutomaticGrowByType(option, ms.Topo); err != nil {
				writeJsonError(w, r, http.StatusInternalServerError,
					fmt.Errorf("Cannot grow volume group! %v", err))
				return
			}
		}
	}
	fid, count, dn, err := ms.Topo.PickForWrite(requestedCount, option)
	if err == nil {
		writeJsonQuiet(w, r, http.StatusOK, operation.AssignResult{Fid: fid, Url: dn.Url(), PublicUrl: dn.PublicUrl, Count: count})
	} else {
		writeJsonQuiet(w, r, http.StatusNotAcceptable, operation.AssignResult{Error: err.Error()})
	}
}
