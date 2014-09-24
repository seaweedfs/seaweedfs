package weed_server

import (
	"github.com/aszxqw/weed-fs/go/operation"
	"github.com/aszxqw/weed-fs/go/stats"
	"github.com/aszxqw/weed-fs/go/storage"
	"net/http"
	"strconv"
	"strings"
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
			machines := ms.Topo.Lookup(collection, volumeId)
			if machines != nil {
				var ret []operation.Location
				for _, dn := range machines {
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
	if location.Error != "" {
		w.WriteHeader(http.StatusNotFound)
	}
	writeJsonQuiet(w, r, location)
}

// This can take batched volumeIds, &volumeId=x&volumeId=y&volumeId=z
func (ms *MasterServer) volumeLookupHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	vids := r.Form["volumeId"]
	collection := r.FormValue("collection") //optional, but can be faster if too many collections
	volumeLocations := ms.lookupVolumeId(vids, collection)
	writeJsonQuiet(w, r, volumeLocations)
}

func (ms *MasterServer) dirAssignHandler(w http.ResponseWriter, r *http.Request) {
	stats.AssignRequest()
	requestedCount, e := strconv.Atoi(r.FormValue("count"))
	if e != nil {
		requestedCount = 1
	}

	option, err := ms.getVolumeGrowOption(r)
	if err != nil {
		w.WriteHeader(http.StatusNotAcceptable)
		writeJsonQuiet(w, r, operation.AssignResult{Error: err.Error()})
		return
	}

	if !ms.Topo.HasWriableVolume(option) {
		if ms.Topo.FreeSpace() <= 0 {
			w.WriteHeader(http.StatusNotFound)
			writeJsonQuiet(w, r, operation.AssignResult{Error: "No free volumes left!"})
			return
		} else {
			ms.vgLock.Lock()
			defer ms.vgLock.Unlock()
			if !ms.Topo.HasWriableVolume(option) {
				if _, err = ms.vg.AutomaticGrowByType(option, ms.Topo); err != nil {
					writeJsonQuiet(w, r, operation.AssignResult{Error: "Cannot grow volume group! " + err.Error()})
					return
				}
			}
		}
	}
	fid, count, dn, err := ms.Topo.PickForWrite(requestedCount, option)
	if err == nil {
		writeJsonQuiet(w, r, operation.AssignResult{Fid: fid, Url: dn.Url(), PublicUrl: dn.PublicUrl, Count: count})
	} else {
		w.WriteHeader(http.StatusNotAcceptable)
		writeJsonQuiet(w, r, operation.AssignResult{Error: err.Error()})
	}
}
