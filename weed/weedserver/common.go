package weedserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"io/ioutil"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/golang/protobuf/proto"
)

var startTime = time.Now()

func readObjRequest(r *http.Request, obj interface{}) error {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}
	if pbMsg, ok := obj.(proto.Message); ok && strings.Contains(r.Header.Get("Content-Type"), "protobuf") {
		if err := proto.Unmarshal(body, pbMsg); err != nil {
			return err
		}
	} else {
		if err := json.Unmarshal(body, obj); err != nil {
			return err
		}
	}
	return nil
}

func writeObjResponse(w http.ResponseWriter, r *http.Request, httpStatus int, obj interface{}) (err error) {
	var (
		bytes       []byte
		contentType string
	)
	if pbMsg, ok := obj.(proto.Message); ok && strings.Contains(r.Header.Get("Accept"), "protobuf") {
		bytes, err = proto.Marshal(pbMsg)
		contentType = "application/protobuf"
	} else {
		if r.FormValue("pretty") != "" {
			bytes, err = json.MarshalIndent(obj, "", "  ")
		} else {
			bytes, err = json.Marshal(obj)
		}
		if callback := r.FormValue("callback"); callback != "" {
			contentType = "application/javascript"
			bytes = []byte(callback + "(" + string(bytes) + ")")
		} else {
			contentType = "application/json"
		}
	}

	if err != nil {
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(httpStatus)
	_, err = w.Write(bytes)
	return
}

// wrapper for writeJson - just logs errors
func writeJsonQuiet(w http.ResponseWriter, r *http.Request, httpStatus int, obj interface{}) {
	if err := writeObjResponse(w, r, httpStatus, obj); err != nil {
		glog.V(0).Infof("error writing JSON %s: %v", obj, err)
	}
}
func writeJsonError(w http.ResponseWriter, r *http.Request, httpStatus int, err error) {
	m := make(map[string]interface{})
	if err == nil {
		m["error"] = ""
	} else {
		m["error"] = err.Error()

	}
	writeJsonQuiet(w, r, httpStatus, m)
}

func debug(params ...interface{}) {
	glog.V(4).Infoln(params)
}

func submitForClientHandler(w http.ResponseWriter, r *http.Request, masterUrl string) {
	jwt := security.GetJwt(r)
	m := make(map[string]interface{})
	if r.Method != "POST" {
		writeJsonError(w, r, http.StatusMethodNotAllowed, errors.New("Only submit via POST!"))
		return
	}

	debug("parsing upload file...")
	fname, data, mimeType, isGzipped, lastModified, _, _, pe := storage.ParseUpload(r)
	if pe != nil {
		writeJsonError(w, r, http.StatusBadRequest, pe)
		return
	}

	debug("assigning file id for", fname)
	r.ParseForm()
	assignResult, ae := operation.Assign(masterUrl, 1, r.FormValue("replication"), r.FormValue("collection"), r.FormValue("ttl"))
	if ae != nil {
		writeJsonError(w, r, http.StatusInternalServerError, ae)
		return
	}

	url := "http://" + assignResult.Url + "/" + assignResult.Fid
	if lastModified != 0 {
		url = url + "?ts=" + strconv.FormatUint(lastModified, 10)
	}

	debug("upload file to store", url)
	uploadResult, err := operation.Upload(url, fname, bytes.NewReader(data), isGzipped, mimeType, jwt)
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	m["fileName"] = fname
	m["fid"] = assignResult.Fid
	m["fileUrl"] = assignResult.PublicUrl + "/" + assignResult.Fid
	m["size"] = uploadResult.Size
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

func parseURLPath(path string) (vid, nid, filename, ext string, isVolumeIdOnly bool) {
	switch strings.Count(path, "/") {
	case 3:
		parts := strings.Split(path, "/")
		vid, nid, filename = parts[1], parts[2], parts[3]
		ext = filepath.Ext(filename)
	case 2:
		parts := strings.Split(path, "/")
		vid, nid = parts[1], parts[2]
		dotIndex := strings.LastIndex(nid, ".")
		if dotIndex > 0 {
			ext = nid[dotIndex:]
			nid = nid[0:dotIndex]
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
		nid = path[commaIndex+1:]
		ext = ""
		if dotIndex > 0 {
			nid = path[commaIndex+1 : dotIndex]
			ext = path[dotIndex:]
		}
	}
	return
}

func statsCounterHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.VERSION
	m["Counters"] = stats.ServStats
	writeJsonQuiet(w, r, http.StatusOK, m)
}

func statsMemoryHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.VERSION
	m["Memory"] = stats.MemStat()
	writeJsonQuiet(w, r, http.StatusOK, m)
}
