package weedserver

import (
	"net/http"

	"time"

	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

func (vs *VolumeServer) newTaskHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	tid, e := vs.store.TaskManager.NewTask(vs.store, r.Form)
	if e == nil {
		writeJsonQuiet(w, r, http.StatusOK, map[string]string{"tid": tid})
	} else {
		writeJsonError(w, r, http.StatusInternalServerError, e)
	}
	glog.V(2).Infoln("new store task =", tid, ", error =", e)
}

func (vs *VolumeServer) queryTaskHandler(w http.ResponseWriter, r *http.Request) {
	tid := r.FormValue("tid")
	timeoutStr := strings.TrimSpace(r.FormValue("timeout"))
	d := time.Minute
	if td, e := time.ParseDuration(timeoutStr); e == nil {
		d = td
	}
	err := vs.store.TaskManager.QueryResult(tid, d)
	if err == storage.ErrTaskNotFinish {
		writeJsonError(w, r, http.StatusRequestTimeout, err)
	} else if err == nil {
		writeJsonError(w, r, http.StatusOK, err)
	} else {
		writeJsonError(w, r, http.StatusInternalServerError, err)
	}
	glog.V(2).Infoln("query task =", tid, ", error =", err)
}
func (vs *VolumeServer) commitTaskHandler(w http.ResponseWriter, r *http.Request) {
	tid := r.FormValue("tid")
	err := vs.store.TaskManager.Commit(tid)
	if err == storage.ErrTaskNotFinish {
		writeJsonError(w, r, http.StatusRequestTimeout, err)
	} else if err == nil {
		writeJsonError(w, r, http.StatusOK, err)
	}
	glog.V(2).Infoln("query task =", tid, ", error =", err)
}
func (vs *VolumeServer) cleanTaskHandler(w http.ResponseWriter, r *http.Request) {
	tid := r.FormValue("tid")
	err := vs.store.TaskManager.Clean(tid)
	if err == storage.ErrTaskNotFinish {
		writeJsonError(w, r, http.StatusRequestTimeout, err)
	} else if err == nil {
		writeJsonError(w, r, http.StatusOK, err)
	}
	glog.V(2).Infoln("clean task =", tid, ", error =", err)
}

func (vs *VolumeServer) allTaskHandler(w http.ResponseWriter, r *http.Request) {
	//TODO get all task
	glog.V(2).Infoln("TODO: get all task")
}
