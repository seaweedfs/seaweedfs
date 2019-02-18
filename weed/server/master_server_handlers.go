package weed_server

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/security"
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
			machines := ms.Topo.Lookup(collection, volumeId)
			if machines != nil {
				var ret []operation.Location
				for _, dn := range machines {
					ret = append(ret, operation.Location{Url: dn.Url(), PublicUrl: dn.PublicUrl})
				}
				volumeLocations[vid] = operation.LookupResult{VolumeId: vid, Locations: ret}
			} else {
				volumeLocations[vid] = operation.LookupResult{VolumeId: vid, Error: fmt.Sprintf("volumeId %s not found.", vid)}
			}
		} else {
			volumeLocations[vid] = operation.LookupResult{VolumeId: vid, Error: fmt.Sprintf("Unknown volumeId format: %s", vid)}
		}
	}
	return
}

// If "fileId" is provided, this returns the fileId location and a JWT to update or delete the file.
// If "volumeId" is provided, this only returns the volumeId location
func (ms *MasterServer) dirLookupHandler(w http.ResponseWriter, r *http.Request) {
	vid := r.FormValue("volumeId")
	if vid != "" {
		// backward compatible
		commaSep := strings.Index(vid, ",")
		if commaSep > 0 {
			vid = vid[0:commaSep]
		}
	}
	fileId := r.FormValue("fileId")
	if fileId != "" {
		commaSep := strings.Index(fileId, ",")
		if commaSep > 0 {
			vid = fileId[0:commaSep]
		}
	}
	vids := []string{vid}
	collection := r.FormValue("collection") //optional, but can be faster if too many collections
	volumeLocations := ms.lookupVolumeId(vids, collection)
	location := volumeLocations[vid]
	httpStatus := http.StatusOK
	if location.Error != "" {
		httpStatus = http.StatusNotFound
	} else {
		ms.maybeAddJwtAuthorization(w, fileId)
	}
	writeJsonQuiet(w, r, httpStatus, location)
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
			if _, err = ms.vg.AutomaticGrowByType(option, ms.grpcDialOpiton, ms.Topo); err != nil {
				writeJsonError(w, r, http.StatusInternalServerError,
					fmt.Errorf("Cannot grow volume group! %v", err))
				return
			}
		}
	}
	fid, count, dn, err := ms.Topo.PickForWrite(requestedCount, option)
	if err == nil {
		ms.maybeAddJwtAuthorization(w, fid)
		writeJsonQuiet(w, r, http.StatusOK, operation.AssignResult{Fid: fid, Url: dn.Url(), PublicUrl: dn.PublicUrl, Count: count})
	} else {
		writeJsonQuiet(w, r, http.StatusNotAcceptable, operation.AssignResult{Error: err.Error()})
	}
}

func (ms *MasterServer) maybeAddJwtAuthorization(w http.ResponseWriter, fileId string) {
	encodedJwt := security.GenJwt(ms.guard.SigningKey, fileId)
	if encodedJwt == "" {
		return
	}
	w.Header().Set("Authorization", "BEARER "+string(encodedJwt))
}
