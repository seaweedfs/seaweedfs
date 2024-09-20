package weed_server

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/topology"
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
		volumeLocations[vid] = ms.findVolumeLocation(collection, vid)
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
	collection := r.FormValue("collection") // optional, but can be faster if too many collections
	location := ms.findVolumeLocation(collection, vid)
	httpStatus := http.StatusOK
	if location.Error != "" || location.Locations == nil {
		httpStatus = http.StatusNotFound
	} else {
		forRead := r.FormValue("read")
		isRead := forRead == "yes"
		ms.maybeAddJwtAuthorization(w, fileId, !isRead)
	}
	writeJsonQuiet(w, r, httpStatus, location)
}

// findVolumeLocation finds the volume location from master topo if it is leader,
// or from master client if not leader
func (ms *MasterServer) findVolumeLocation(collection, vid string) operation.LookupResult {
	var locations []operation.Location
	var err error
	if ms.Topo.IsLeader() {
		volumeId, newVolumeIdErr := needle.NewVolumeId(vid)
		if newVolumeIdErr != nil {
			err = fmt.Errorf("Unknown volume id %s", vid)
		} else {
			machines := ms.Topo.Lookup(collection, volumeId)
			for _, loc := range machines {
				locations = append(locations, operation.Location{
					Url:        loc.Url(),
					PublicUrl:  loc.PublicUrl,
					DataCenter: loc.GetDataCenterId(),
					GrpcPort:   loc.GrpcPort,
				})
			}
		}
	} else {
		machines, getVidLocationsErr := ms.MasterClient.GetVidLocations(vid)
		for _, loc := range machines {
			locations = append(locations, operation.Location{
				Url:        loc.Url,
				PublicUrl:  loc.PublicUrl,
				DataCenter: loc.DataCenter,
				GrpcPort:   loc.GrpcPort,
			})
		}
		err = getVidLocationsErr
	}
	if len(locations) == 0 && err == nil {
		err = fmt.Errorf("volume id %s not found", vid)
	}
	ret := operation.LookupResult{
		VolumeOrFileId: vid,
		Locations:      locations,
	}
	if err != nil {
		ret.Error = err.Error()
	}
	return ret
}

func (ms *MasterServer) dirAssignHandler(w http.ResponseWriter, r *http.Request) {
	stats.AssignRequest()
	requestedCount, e := strconv.ParseUint(r.FormValue("count"), 10, 64)
	if e != nil || requestedCount == 0 {
		requestedCount = 1
	}

	writableVolumeCount, e := strconv.ParseUint(r.FormValue("writableVolumeCount"), 10, 32)
	if e != nil {
		writableVolumeCount = 0
	}

	option, err := ms.getVolumeGrowOption(r)
	if err != nil {
		writeJsonQuiet(w, r, http.StatusNotAcceptable, operation.AssignResult{Error: err.Error()})
		return
	}

	vl := ms.Topo.GetVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl, option.DiskType)

	var (
		lastErr    error
		maxTimeout = time.Second * 10
		startTime  = time.Now()
	)

	if !ms.Topo.DataCenterExists(option.DataCenter) {
		writeJsonQuiet(w, r, http.StatusBadRequest, operation.AssignResult{
			Error: fmt.Sprintf("data center %v not found in topology", option.DataCenter),
		})
		return
	}

	for time.Now().Sub(startTime) < maxTimeout {
		fid, count, dnList, shouldGrow, err := ms.Topo.PickForWrite(requestedCount, option, vl)
		if shouldGrow && !vl.HasGrowRequest() {
			glog.V(0).Infof("dirAssign volume growth %v from %v", option.String(), r.RemoteAddr)
			if err != nil && ms.Topo.AvailableSpaceFor(option) <= 0 {
				err = fmt.Errorf("%s and no free volumes left for %s", err.Error(), option.String())
			}
			vl.AddGrowRequest()
			ms.volumeGrowthRequestChan <- &topology.VolumeGrowRequest{
				Option: option,
				Count:  uint32(writableVolumeCount),
				Reason: "http assign",
			}
		}
		if err != nil {
			stats.MasterPickForWriteErrorCounter.Inc()
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		} else {
			ms.maybeAddJwtAuthorization(w, fid, true)
			dn := dnList.Head()
			if dn == nil {
				continue
			}
			writeJsonQuiet(w, r, http.StatusOK, operation.AssignResult{Fid: fid, Url: dn.Url(), PublicUrl: dn.PublicUrl, Count: count})
			return
		}
	}

	if lastErr != nil {
		writeJsonQuiet(w, r, http.StatusNotAcceptable, operation.AssignResult{Error: lastErr.Error()})
	} else {
		writeJsonQuiet(w, r, http.StatusRequestTimeout, operation.AssignResult{Error: "request timeout"})
	}
}

func (ms *MasterServer) maybeAddJwtAuthorization(w http.ResponseWriter, fileId string, isWrite bool) {
	if fileId == "" {
		return
	}
	var encodedJwt security.EncodedJwt
	if isWrite {
		encodedJwt = security.GenJwtForVolumeServer(ms.guard.SigningKey, ms.guard.ExpiresAfterSec, fileId)
	} else {
		encodedJwt = security.GenJwtForVolumeServer(ms.guard.ReadSigningKey, ms.guard.ReadExpiresAfterSec, fileId)
	}
	if encodedJwt == "" {
		return
	}

	w.Header().Set("Authorization", "BEARER "+string(encodedJwt))
}
