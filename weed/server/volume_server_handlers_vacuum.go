package weed_server

import (
	"net/http"
	"strconv"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

func (vs *VolumeServer) vacuumVolumeCheckHandler(w http.ResponseWriter, r *http.Request) {
	err, ret := vs.store.CheckCompactVolume(r.FormValue("volume"), r.FormValue("garbageThreshold"))
	if err == nil {
		writeJsonQuiet(w, r, http.StatusOK, map[string]interface{}{"error": "", "result": ret})
	} else {
		writeJsonQuiet(w, r, http.StatusInternalServerError, map[string]interface{}{"error": err.Error(), "result": false})
	}
	glog.V(2).Infoln("checked compacting volume =", r.FormValue("volume"), "garbageThreshold =", r.FormValue("garbageThreshold"), "vacuum =", ret)
}
func (vs *VolumeServer) vacuumVolumeCompactHandler(w http.ResponseWriter, r *http.Request) {
	var preallocate int64
	var err error
	if r.FormValue("preallocate") != "" {
		preallocate, err = strconv.ParseInt(r.FormValue("preallocate"), 10, 64)
		if err != nil {
			glog.V(0).Infoln("Failed to parse int64 preallocate = %s: %v", r.FormValue("preallocate"), err)
		}
	}
	err = vs.store.CompactVolume(r.FormValue("volume"), preallocate)
	if err == nil {
		writeJsonQuiet(w, r, http.StatusOK, map[string]string{"error": ""})
	} else {
		writeJsonError(w, r, http.StatusInternalServerError, err)
	}
	glog.V(2).Infoln("compacted volume =", r.FormValue("volume"), ", error =", err)
}
func (vs *VolumeServer) vacuumVolumeCommitHandler(w http.ResponseWriter, r *http.Request) {
	err := vs.store.CommitCompactVolume(r.FormValue("volume"))
	if err == nil {
		writeJsonQuiet(w, r, http.StatusOK, map[string]string{"error": ""})
	} else {
		writeJsonError(w, r, http.StatusInternalServerError, err)
	}
	glog.V(2).Infoln("commit compact volume =", r.FormValue("volume"), ", error =", err)
}
func (vs *VolumeServer) vacuumVolumeCleanupHandler(w http.ResponseWriter, r *http.Request) {
	err := vs.store.CommitCleanupVolume(r.FormValue("volume"))
	if err == nil {
		writeJsonQuiet(w, r, http.StatusOK, map[string]string{"error": ""})
	} else {
		writeJsonError(w, r, http.StatusInternalServerError, err)
	}
	glog.V(2).Infoln("cleanup compact volume =", r.FormValue("volume"), ", error =", err)
}
