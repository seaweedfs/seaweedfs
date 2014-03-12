package weed_server

import (
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/replication"
	"code.google.com/p/weed-fs/go/storage"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var fileNameEscaper = strings.NewReplacer("\\", "\\\\", "\"", "\\\"")

func (vs *VolumeServer) statusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = vs.version
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

func (vs *VolumeServer) storeHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		vs.GetOrHeadHandler(w, r, true)
	case "HEAD":
		vs.GetOrHeadHandler(w, r, false)
	case "DELETE":
		secure(vs.whiteList, vs.DeleteHandler)(w, r)
	case "PUT":
		secure(vs.whiteList, vs.PostHandler)(w, r)
	case "POST":
		secure(vs.whiteList, vs.PostHandler)(w, r)
	}
}
func (vs *VolumeServer) GetOrHeadHandler(w http.ResponseWriter, r *http.Request, isGetMethod bool) {
	n := new(storage.Needle)
	vid, fid, filename, ext, _ := parseURLPath(r.URL.Path)
	volumeId, err := storage.NewVolumeId(vid)
	if err != nil {
		glog.V(2).Infoln("parsing error:", err, r.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	err = n.ParsePath(fid)
	if err != nil {
		glog.V(2).Infoln("parsing fid error:", err, r.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	glog.V(2).Infoln("volume", volumeId, "reading", n)
	if !vs.store.HasVolume(volumeId) {
		lookupResult, err := operation.Lookup(vs.masterNode, volumeId.String())
		glog.V(2).Infoln("volume", volumeId, "found on", lookupResult, "error", err)
		if err == nil && len(lookupResult.Locations) > 0 {
			http.Redirect(w, r, "http://"+lookupResult.Locations[0].PublicUrl+r.URL.Path, http.StatusMovedPermanently)
		} else {
			glog.V(2).Infoln("lookup error:", err, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
		return
	}
	cookie := n.Cookie
	count, e := vs.store.Read(volumeId, n)
	glog.V(2).Infoln("read bytes", count, "error", e)
	if e != nil || count <= 0 {
		glog.V(0).Infoln("read error:", e, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if n.Cookie != cookie {
		glog.V(0).Infoln("request with unmaching cookie seen:", cookie, "expected:", n.Cookie, "from", r.RemoteAddr, "agent", r.UserAgent())
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if n.LastModified != 0 {
		w.Header().Set("Last-Modified", time.Unix(int64(n.LastModified), 0).UTC().Format(http.TimeFormat))
		if r.Header.Get("If-Modified-Since") != "" {
			if t, parseError := time.Parse(http.TimeFormat, r.Header.Get("If-Modified-Since")); parseError == nil {
				if t.Unix() >= int64(n.LastModified) {
					w.WriteHeader(http.StatusNotModified)
					return
				}
			}
		}
	}
	if n.NameSize > 0 && filename == "" {
		filename = string(n.Name)
		dotIndex := strings.LastIndex(filename, ".")
		if dotIndex > 0 {
			ext = filename[dotIndex:]
		}
	}
	mtype := ""
	if ext != "" {
		mtype = mime.TypeByExtension(ext)
	}
	if n.MimeSize > 0 {
		mtype = string(n.Mime)
	}
	if mtype != "" {
		w.Header().Set("Content-Type", mtype)
	}
	if filename != "" {
		w.Header().Set("Content-Disposition", "filename="+fileNameEscaper.Replace(filename))
	}
	if ext != ".gz" {
		if n.IsGzipped() {
			if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
				w.Header().Set("Content-Encoding", "gzip")
			} else {
				if n.Data, err = storage.UnGzipData(n.Data); err != nil {
					glog.V(0).Infoln("lookup error:", err, r.URL.Path)
				}
			}
		}
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(n.Data)))
	if isGetMethod {
		if _, e = w.Write(n.Data); e != nil {
			glog.V(0).Infoln("response write error:", e)
		}
	}
}
func (vs *VolumeServer) PostHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	if e := r.ParseForm(); e != nil {
		glog.V(0).Infoln("form parse error:", e)
		writeJsonError(w, r, e)
		return
	}
	vid, _, _, _, _ := parseURLPath(r.URL.Path)
	volumeId, ve := storage.NewVolumeId(vid)
	if ve != nil {
		glog.V(0).Infoln("NewVolumeId error:", ve)
		writeJsonError(w, r, ve)
		return
	}
	needle, ne := storage.NewNeedle(r)
	if ne != nil {
		writeJsonError(w, r, ne)
		return
	}
	ret, errorStatus := replication.ReplicatedWrite(vs.masterNode, vs.store, volumeId, needle, r)
	if errorStatus == "" {
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		m["error"] = errorStatus
	}
	m["size"] = ret
	writeJsonQuiet(w, r, m)
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
		writeJsonQuiet(w, r, m)
		return
	}

	if n.Cookie != cookie {
		glog.V(0).Infoln("delete with unmaching cookie from ", r.RemoteAddr, "agent", r.UserAgent())
		return
	}

	n.Size = 0
	ret := replication.ReplicatedDelete(vs.masterNode, vs.store, volumeId, n, r)

	if ret != 0 {
		w.WriteHeader(http.StatusAccepted)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}

	m := make(map[string]uint32)
	m["size"] = uint32(count)
	writeJsonQuiet(w, r, m)
}
