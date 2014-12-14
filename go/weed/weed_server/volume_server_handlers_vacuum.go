package weed_server

import (
	"net/http"

	"github.com/mcqueenorama/weed-fs/go/glog"
)

func (vs *VolumeServer) vacuumVolumeCheckHandler(w http.ResponseWriter, r *http.Request) {
	err, ret := vs.store.CheckCompactVolume(r.FormValue("volume"), r.FormValue("garbageThreshold"))
	if err == nil {
		writeJsonQuiet(w, r, map[string]interface{}{"error": "", "result": ret})
	} else {
		writeJsonQuiet(w, r, map[string]interface{}{"error": err.Error(), "result": false})
	}
	glog.V(2).Infoln("checked compacting volume =", r.FormValue("volume"), "garbageThreshold =", r.FormValue("garbageThreshold"), "vacuum =", ret)
}
func (vs *VolumeServer) vacuumVolumeCompactHandler(w http.ResponseWriter, r *http.Request) {
	err := vs.store.CompactVolume(r.FormValue("volume"))
	if err == nil {
		writeJsonQuiet(w, r, map[string]string{"error": ""})
	} else {
		writeJsonQuiet(w, r, map[string]string{"error": err.Error()})
	}
	glog.V(2).Infoln("compacted volume =", r.FormValue("volume"), ", error =", err)
}
func (vs *VolumeServer) vacuumVolumeCommitHandler(w http.ResponseWriter, r *http.Request) {
	err := vs.store.CommitCompactVolume(r.FormValue("volume"))
	if err == nil {
		writeJsonQuiet(w, r, map[string]interface{}{"error": ""})
	} else {
		writeJsonQuiet(w, r, map[string]string{"error": err.Error()})
	}
	glog.V(2).Infoln("commit compact volume =", r.FormValue("volume"), ", error =", err)
}
