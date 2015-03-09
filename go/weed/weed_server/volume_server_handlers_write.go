package weed_server

import (
	"errors"
	"net/http"

	"github.com/chrislusf/weed-fs/go/glog"
	"github.com/chrislusf/weed-fs/go/operation"
	"github.com/chrislusf/weed-fs/go/storage"
	"github.com/chrislusf/weed-fs/go/topology"
)

func (vs *VolumeServer) PostHandler(w http.ResponseWriter, r *http.Request) {
	if e := r.ParseForm(); e != nil {
		glog.V(0).Infoln("form parse error:", e)
		writeJsonError(w, r, http.StatusBadRequest, e)
		return
	}
	vid, _, _, _, _ := parseURLPath(r.URL.Path)
	volumeId, ve := storage.NewVolumeId(vid)
	if ve != nil {
		glog.V(0).Infoln("NewVolumeId error:", ve)
		writeJsonError(w, r, http.StatusBadRequest, ve)
		return
	}
	needle, ne := storage.NewNeedle(r, vs.FixJpgOrientation)
	if ne != nil {
		writeJsonError(w, r, http.StatusBadRequest, ne)
		return
	}

	ret := operation.UploadResult{}
	size, errorStatus := topology.ReplicatedWrite(vs.GetMasterNode(),
		vs.store, volumeId, needle, r)
	httpStatus := http.StatusCreated
	if errorStatus != "" {
		httpStatus = http.StatusInternalServerError
		ret.Error = errorStatus
	}
	if needle.HasName() {
		ret.Name = string(needle.Name)
	}
	ret.Size = size
	writeJsonQuiet(w, r, httpStatus, ret)
}

func (vs *VolumeServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	n := new(storage.Needle)
	vid, fid, _, _, _ := parseURLPath(r.URL.Path)
	volumeId, _ := storage.NewVolumeId(vid)
	n.ParsePath(fid)

	glog.V(2).Infoln("deleting", n)

	cookie := n.Cookie
	count, ok := vs.store.Read(volumeId, n)

	if ok != nil {
		m := make(map[string]uint32)
		m["size"] = 0
		writeJsonQuiet(w, r, http.StatusNotFound, m)
		return
	}

	if n.Cookie != cookie {
		glog.V(0).Infoln("delete", r.URL.Path, "with unmaching cookie from ", r.RemoteAddr, "agent", r.UserAgent())
		return
	}

	ret := topology.ReplicatedDelete(vs.GetMasterNode(), vs.store, volumeId, n, r)

	if ret != 0 {
		m := make(map[string]uint32)
		m["size"] = uint32(count)
		writeJsonQuiet(w, r, http.StatusAccepted, m)
	} else {
		writeJsonError(w, r, http.StatusInternalServerError, errors.New("Deletion Failed."))
	}

}

//Experts only: takes multiple fid parameters. This function does not propagate deletes to replicas.
func (vs *VolumeServer) batchDeleteHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	var ret []operation.DeleteResult
	for _, fid := range r.Form["fid"] {
		vid, id_cookie, err := operation.ParseFileId(fid)
		if err != nil {
			ret = append(ret, operation.DeleteResult{Fid: fid, Error: err.Error()})
			continue
		}
		n := new(storage.Needle)
		volumeId, _ := storage.NewVolumeId(vid)
		n.ParsePath(id_cookie)
		glog.V(4).Infoln("batch deleting", n)
		cookie := n.Cookie
		if _, err := vs.store.Read(volumeId, n); err != nil {
			ret = append(ret, operation.DeleteResult{Fid: fid, Error: err.Error()})
			continue
		}
		if n.Cookie != cookie {
			ret = append(ret, operation.DeleteResult{Fid: fid, Error: "File Random Cookie does not match."})
			glog.V(0).Infoln("deleting", fid, "with unmaching cookie from ", r.RemoteAddr, "agent", r.UserAgent())
			return
		}
		if size, err := vs.store.Delete(volumeId, n); err != nil {
			ret = append(ret, operation.DeleteResult{Fid: fid, Error: err.Error()})
		} else {
			ret = append(ret, operation.DeleteResult{Fid: fid, Size: int(size)})
		}
	}

	writeJsonQuiet(w, r, http.StatusAccepted, ret)
}
