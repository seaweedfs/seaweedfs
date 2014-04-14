package weed_server

import (
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/stats"
	"code.google.com/p/weed-fs/go/util"
	"net/http"
	"path/filepath"
)

func (vs *VolumeServer) statusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.VERSION
	m["Volumes"] = vs.store.Status()
	writeJsonQuiet(w, r, m)
}

func (vs *VolumeServer) assignVolumeHandler(w http.ResponseWriter, r *http.Request) {
	err := vs.store.AddVolume(r.FormValue("volume"), r.FormValue("collection"), r.FormValue("replication"))
	if err == nil {
		writeJsonQuiet(w, r, map[string]string{"error": ""})
	} else {
		writeJsonQuiet(w, r, map[string]string{"error": err.Error()})
	}
	glog.V(2).Infoln("assign volume =", r.FormValue("volume"), ", collection =", r.FormValue("collection"), ", replication =", r.FormValue("replication"), ", error =", err)
}

func (vs *VolumeServer) deleteCollectionHandler(w http.ResponseWriter, r *http.Request) {
	if "benchmark" != r.FormValue("collection") {
		glog.V(0).Infoln("deleting collection =", r.FormValue("collection"), "!!!")
		return
	}
	err := vs.store.DeleteCollection(r.FormValue("collection"))
	if err == nil {
		writeJsonQuiet(w, r, map[string]string{"error": ""})
	} else {
		writeJsonQuiet(w, r, map[string]string{"error": err.Error()})
	}
	glog.V(2).Infoln("deleting collection =", r.FormValue("collection"), ", error =", err)
}

func (vs *VolumeServer) freezeVolumeHandler(w http.ResponseWriter, r *http.Request) {
	//TODO: notify master that this volume will be read-only
	err := vs.store.FreezeVolume(r.FormValue("volume"))
	if err == nil {
		writeJsonQuiet(w, r, map[string]interface{}{"error": ""})
	} else {
		writeJsonQuiet(w, r, map[string]string{"error": err.Error()})
	}
	glog.V(2).Infoln("freeze volume =", r.FormValue("volume"), ", error =", err)
}

func (vs *VolumeServer) submitFromVolumeServerHandler(w http.ResponseWriter, r *http.Request) {
	submitForClientHandler(w, r, vs.masterNode)
}

func (vs *VolumeServer) statsDiskHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.VERSION
	ds := make([]*stats.DiskStatus, 0)
	for _, loc := range vs.store.Locations {
		if dir, e := filepath.Abs(loc.Directory); e == nil {
			ds = append(ds, stats.NewDiskStatus(dir))
		}
	}
	m["DiskStatues"] = ds
	writeJsonQuiet(w, r, m)
}
