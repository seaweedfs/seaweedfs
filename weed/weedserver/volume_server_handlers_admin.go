package weedserver

import (
	"errors"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type VolumeOptError struct {
	Volume string `json:"volume"`
	Err    string `json:"err"`
}

func (vs *VolumeServer) statusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.VERSION
	m["Volumes"] = vs.store.Status()
	writeJsonQuiet(w, r, http.StatusOK, m)
}

func (vs *VolumeServer) assignVolumeHandler(w http.ResponseWriter, r *http.Request) {
	err := vs.store.AddVolume(r.FormValue("volume"), r.FormValue("collection"), r.FormValue("ttl"))
	if err == nil {
		writeJsonQuiet(w, r, http.StatusAccepted, map[string]string{"error": ""})
	} else {
		writeJsonError(w, r, http.StatusNotAcceptable, err)
	}
	glog.V(2).Infoln("assign volume =", r.FormValue("volume"), ", collection =", r.FormValue("collection"), ", replication =", r.FormValue("replication"), ", error =", err)
}

func (vs *VolumeServer) deleteCollectionHandler(w http.ResponseWriter, r *http.Request) {
	err := vs.store.DeleteCollection(r.FormValue("collection"))
	if err == nil {
		writeJsonQuiet(w, r, http.StatusOK, map[string]string{"error": ""})
	} else {
		writeJsonError(w, r, http.StatusInternalServerError, err)
	}
	glog.V(2).Infoln("deleting collection =", r.FormValue("collection"), ", error =", err)
}

func (vs *VolumeServer) statsDiskHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.VERSION
	var ds []*stats.DiskStatus
	for _, loc := range vs.store.Locations {
		if dir, e := filepath.Abs(loc.Directory); e == nil {
			ds = append(ds, stats.NewDiskStatus(dir))
		}
	}
	m["DiskStatuses"] = ds
	writeJsonQuiet(w, r, http.StatusOK, m)
}

func (vs *VolumeServer) setVolumeOptionHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	errs := []VolumeOptError{}
	var (
		setter storage.VolumeWalker
	)

	key := r.FormValue("key")
	value := r.FormValue("value")
	if key == "readonly" {
		isReadOnly, e := strconv.ParseBool(value)
		if e != nil {
			writeJsonError(w, r, http.StatusBadRequest, e)
			return
		}
		setter = func(v *storage.Volume) error {
			if e := v.SetReadOnly(isReadOnly); e != nil {
				errs = append(errs, VolumeOptError{
					Volume: v.Id.String(),
					Err:    e.Error(),
				})
			}
			return nil
		}
	} else {
		writeJsonError(w, r, http.StatusBadRequest, errors.New("Unkonw setting: "+key))
		return
	}

	all, _ := strconv.ParseBool(r.FormValue("all"))
	if all {
		vs.store.WalkVolume(setter)
	} else {
		volumesSet := make(map[string]bool)
		for _, volume := range r.Form["volume"] {
			volumesSet[strings.TrimSpace(volume)] = true
		}
		collectionsSet := make(map[string]bool)
		for _, c := range r.Form["collection"] {
			collectionsSet[strings.TrimSpace(c)] = true
		}
		if len(collectionsSet) > 0 || len(volumesSet) > 0 {
			vs.store.WalkVolume(func(v *storage.Volume) (e error) {
				if !collectionsSet[v.Collection] && !volumesSet[v.Id.String()] {
					return nil
				}
				setter(v)
				return nil
			})
		}

	}

	result := make(map[string]interface{})
	if len(errs) > 0 {
		result["error"] = "set volume replica error."
		result["errors"] = errs
	}

	writeObjResponse(w, r, http.StatusAccepted, result)
}
