package weed_server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/images"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var serverStats *stats.ServerStats
var startTime = time.Now()

func init() {
	serverStats = stats.NewServerStats()
	go serverStats.Start()

}

func writeJson(w http.ResponseWriter, r *http.Request, httpStatus int, obj interface{}) (err error) {
	var bytes []byte
	if r.FormValue("pretty") != "" {
		bytes, err = json.MarshalIndent(obj, "", "  ")
	} else {
		bytes, err = json.Marshal(obj)
	}
	if err != nil {
		return
	}
	callback := r.FormValue("callback")
	if callback == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(httpStatus)
		_, err = w.Write(bytes)
	} else {
		w.Header().Set("Content-Type", "application/javascript")
		w.WriteHeader(httpStatus)
		if _, err = w.Write([]uint8(callback)); err != nil {
			return
		}
		if _, err = w.Write([]uint8("(")); err != nil {
			return
		}
		fmt.Fprint(w, string(bytes))
		if _, err = w.Write([]uint8(")")); err != nil {
			return
		}
	}

	return
}

// wrapper for writeJson - just logs errors
func writeJsonQuiet(w http.ResponseWriter, r *http.Request, httpStatus int, obj interface{}) {
	if err := writeJson(w, r, httpStatus, obj); err != nil {
		glog.V(0).Infof("error writing JSON %s: %v", obj, err)
	}
}
func writeJsonError(w http.ResponseWriter, r *http.Request, httpStatus int, err error) {
	m := make(map[string]interface{})
	m["error"] = err.Error()
	writeJsonQuiet(w, r, httpStatus, m)
}

func debug(params ...interface{}) {
	glog.V(4).Infoln(params...)
}

func submitForClientHandler(w http.ResponseWriter, r *http.Request, masterUrl string) {
	jwt := security.GetJwt(r)
	m := make(map[string]interface{})
	if r.Method != "POST" {
		writeJsonError(w, r, http.StatusMethodNotAllowed, errors.New("Only submit via POST!"))
		return
	}

	debug("parsing upload file...")
	fname, data, mimeType, pairMap, isGzipped, lastModified, _, _, pe := storage.ParseUpload(r)
	if pe != nil {
		writeJsonError(w, r, http.StatusBadRequest, pe)
		return
	}

	debug("assigning file id for", fname)
	r.ParseForm()
	ar := &operation.VolumeAssignRequest{
		Count:       1,
		Replication: r.FormValue("replication"),
		Collection:  r.FormValue("collection"),
		Ttl:         r.FormValue("ttl"),
	}
	assignResult, ae := operation.Assign(masterUrl, ar)
	if ae != nil {
		writeJsonError(w, r, http.StatusInternalServerError, ae)
		return
	}

	url := "http://" + assignResult.Url + "/" + assignResult.Fid
	if lastModified != 0 {
		url = url + "?ts=" + strconv.FormatUint(lastModified, 10)
	}

	debug("upload file to store", url)
	uploadResult, err := operation.Upload(url, fname, bytes.NewReader(data), isGzipped, mimeType, pairMap, jwt)
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	m["fileName"] = fname
	m["fid"] = assignResult.Fid
	m["fileUrl"] = assignResult.PublicUrl + "/" + assignResult.Fid
	m["size"] = uploadResult.Size
	m["eTag"] = uploadResult.ETag
	writeJsonQuiet(w, r, http.StatusCreated, m)
	return
}

func deleteForClientHandler(w http.ResponseWriter, r *http.Request, masterUrl string) {
	r.ParseForm()
	fids := r.Form["fid"]
	ret, err := operation.DeleteFiles(masterUrl, fids)
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}
	writeJsonQuiet(w, r, http.StatusAccepted, ret)
}

func parseURLPath(path string) (vid, fid, filename, ext string, isVolumeIdOnly bool) {
	switch strings.Count(path, "/") {
	case 3:
		parts := strings.Split(path, "/")
		vid, fid, filename = parts[1], parts[2], parts[3]
		ext = filepath.Ext(filename)
	case 2:
		parts := strings.Split(path, "/")
		vid, fid = parts[1], parts[2]
		dotIndex := strings.LastIndex(fid, ".")
		if dotIndex > 0 {
			ext = fid[dotIndex:]
			fid = fid[0:dotIndex]
		}
	default:
		sepIndex := strings.LastIndex(path, "/")
		commaIndex := strings.LastIndex(path[sepIndex:], ",")
		if commaIndex <= 0 {
			vid, isVolumeIdOnly = path[sepIndex+1:], true
			return
		}
		dotIndex := strings.LastIndex(path[sepIndex:], ".")
		vid = path[sepIndex+1 : commaIndex]
		fid = path[commaIndex+1:]
		ext = ""
		if dotIndex > 0 {
			fid = path[commaIndex+1 : dotIndex]
			ext = path[dotIndex:]
		}
	}
	return
}

func statsHealthHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.VERSION
	writeJsonQuiet(w, r, http.StatusOK, m)
}
func statsCounterHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.VERSION
	m["Counters"] = serverStats
	writeJsonQuiet(w, r, http.StatusOK, m)
}

func statsMemoryHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.VERSION
	m["Memory"] = stats.MemStat()
	writeJsonQuiet(w, r, http.StatusOK, m)
}

func faviconHandler(w http.ResponseWriter, r *http.Request) {
	data, err := images.Asset("favicon/favicon.ico")
	if err != nil {
		glog.V(2).Infoln("favicon read error:", err)
		return
	}

	if e := writeResponseContent("favicon.ico", "image/x-icon", bytes.NewReader(data), w, r); e != nil {
		glog.V(2).Infoln("response write error:", e)
	}
}
