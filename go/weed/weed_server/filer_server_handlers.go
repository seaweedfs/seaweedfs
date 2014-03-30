package weed_server

import (
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/util"
	"encoding/json"
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

func (fs *FilerServer) filerHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		fs.GetOrHeadHandler(w, r, true)
	case "HEAD":
		fs.GetOrHeadHandler(w, r, false)
	case "DELETE":
		fs.DeleteHandler(w, r)
	case "PUT":
		fs.PostHandler(w, r)
	case "POST":
		fs.PostHandler(w, r)
	}
}
func (fs *FilerServer) listDirectoryHandler(w http.ResponseWriter, r *http.Request) {
	if !strings.HasSuffix(r.URL.Path, "/") {
		return
	}
	dirlist, err := fs.ListDirectories(r.URL.Path)
	if err == leveldb.ErrNotFound {
		glog.V(3).Infoln("Directory Not Found in db", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	m := make(map[string]interface{})
	m["Directory"] = r.URL.Path
	m["Subdirectories"] = dirlist
	start, _ := strconv.Atoi(r.FormValue("start"))
	limit, limit_err := strconv.Atoi(r.FormValue("limit"))
	if limit_err != nil {
		limit = 100
	}
	m["Files"] = fs.ListFiles(r.URL.Path, start, limit)
	writeJsonQuiet(w, r, m)
}
func (fs *FilerServer) GetOrHeadHandler(w http.ResponseWriter, r *http.Request, isGetMethod bool) {
	if strings.HasSuffix(r.URL.Path, "/") {
		fs.listDirectoryHandler(w, r)
		return
	}
	fileId, err := fs.FindFile(r.URL.Path)
	if err == leveldb.ErrNotFound {
		glog.V(3).Infoln("Not found in db", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	parts := strings.Split(fileId, ",")
	if len(parts) != 2 {
		glog.V(1).Infoln("Invalid fileId", fileId)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	lookup, lookupError := operation.Lookup(fs.master, parts[0])
	if lookupError != nil {
		glog.V(1).Infoln("Invalid lookup", lookupError.Error())
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if len(lookup.Locations) == 0 {
		glog.V(1).Infoln("Can not find location for volume", parts[0])
		w.WriteHeader(http.StatusNotFound)
		return
	}
	urlLocation := lookup.Locations[rand.Intn(len(lookup.Locations))].PublicUrl
	u, _ := url.Parse("http://" + urlLocation + "/" + fileId)
	request := &http.Request{
		Method:        r.Method,
		URL:           u,
		Proto:         r.Proto,
		ProtoMajor:    r.ProtoMajor,
		ProtoMinor:    r.ProtoMinor,
		Header:        r.Header,
		Body:          r.Body,
		Host:          r.Host,
		ContentLength: r.ContentLength,
	}
	glog.V(3).Infoln("retrieving from", u)
	resp, do_err := util.Do(request)
	if do_err != nil {
		glog.V(0).Infoln("failing to connect to volume server", do_err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		writeJsonError(w, r, do_err)
		return
	}
	defer resp.Body.Close()
	io.Copy(w, resp.Body)
}

func (fs *FilerServer) PostHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	assignResult, ae := operation.Assign(fs.master, 1, query.Get("replication"), fs.collection)
	if ae != nil {
		glog.V(0).Infoln("failing to assign a file id", ae.Error())
		w.WriteHeader(http.StatusInternalServerError)
		writeJsonError(w, r, ae)
		return
	}

	u, _ := url.Parse("http://" + assignResult.PublicUrl + "/" + assignResult.Fid)
	request := &http.Request{
		Method:        r.Method,
		URL:           u,
		Proto:         r.Proto,
		ProtoMajor:    r.ProtoMajor,
		ProtoMinor:    r.ProtoMinor,
		Header:        r.Header,
		Body:          r.Body,
		Host:          r.Host,
		ContentLength: r.ContentLength,
	}
	resp, do_err := util.Do(request)
	if do_err != nil {
		glog.V(0).Infoln("failing to connect to volume server", do_err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		writeJsonError(w, r, do_err)
		return
	}
	defer resp.Body.Close()
	resp_body, ra_err := ioutil.ReadAll(resp.Body)
	if ra_err != nil {
		glog.V(0).Infoln("failing to upload to volume server", ra_err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		writeJsonError(w, r, ra_err)
		return
	}
	var ret operation.UploadResult
	unmarshal_err := json.Unmarshal(resp_body, &ret)
	if unmarshal_err != nil {
		glog.V(0).Infoln("failing to read upload resonse", string(resp_body))
		w.WriteHeader(http.StatusInternalServerError)
		writeJsonError(w, r, unmarshal_err)
		return
	}
	if ret.Error != "" {
		glog.V(0).Infoln("failing to post to volume server", ra_err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		writeJsonError(w, r, errors.New(ret.Error))
		return
	}
	path := r.URL.Path
	if strings.HasSuffix(path, "/") {
		if ret.Name != "" {
			path += ret.Name
		} else {
			operation.DeleteFile(fs.master, assignResult.Fid) //clean up
			glog.V(0).Infoln("Can not to write to folder", path, "without a file name!")
			w.WriteHeader(http.StatusInternalServerError)
			writeJsonError(w, r, errors.New("Can not to write to folder "+path+" without a file name"))
			return
		}
	}
	if db_err := fs.CreateFile(path, assignResult.Fid); db_err != nil {
		operation.DeleteFile(fs.master, assignResult.Fid) //clean up
		glog.V(0).Infoln("failing to write to filer server", db_err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		writeJsonError(w, r, db_err)
		return
	}
}

func (fs *FilerServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	isForceDirectoryRemoval := r.FormValue("force") == "true" // force remove for directories
	fid, isFile, err := fs.Delete(r.URL.Path, isForceDirectoryRemoval)
	if err == nil {
		if isFile {
			err = operation.DeleteFile(fs.master, fid)
		}
	}
	if err != nil {
		glog.V(1).Infoln("deleting", r.URL.Path, ":", err.Error())
		w.WriteHeader(http.StatusAccepted)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}
