package weedserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/syndtr/goleveldb/leveldb"
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

// listDirectoryHandler lists directories and folers under a directory
// files are sorted by name and paginated via "lastFileName" and "limit".
// sub directories are listed on the first page, when "lastFileName"
// is empty.
func (fs *FilerServer) listDirectoryHandler(w http.ResponseWriter, r *http.Request) {
	if !strings.HasSuffix(r.URL.Path, "/") {
		return
	}
	dirlist, err := fs.filer.ListDirectories(r.URL.Path)
	if err == leveldb.ErrNotFound {
		glog.V(3).Infoln("Directory Not Found in db", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	m := make(map[string]interface{})
	m["Directory"] = r.URL.Path
	lastFileName := r.FormValue("lastFileName")
	if lastFileName == "" {
		m["Subdirectories"] = dirlist
	}
	limit, limit_err := strconv.Atoi(r.FormValue("limit"))
	if limit_err != nil {
		limit = 100
	}
	m["Files"], _ = fs.filer.ListFiles(r.URL.Path, lastFileName, limit)
	writeJsonQuiet(w, r, http.StatusOK, m)
}

func (fs *FilerServer) GetOrHeadHandler(w http.ResponseWriter, r *http.Request, isGetMethod bool) {
	if strings.HasSuffix(r.URL.Path, "/") {
		if fs.disableDirListing {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		fs.listDirectoryHandler(w, r)
		return
	}

	fileId, err := fs.filer.FindFile(r.URL.Path)
	if err == leveldb.ErrNotFound {
		glog.V(3).Infoln("Not found in db", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	query := r.URL.Query()
	collection := query.Get("collection")
	if collection == "" {
		collection = fs.collection
	}
	urlString, err := operation.LookupFileId(fs.master, fileId, collection, true)
	if err != nil {
		glog.V(1).Infoln("operation LookupFileId %s failed, err is %s", fileId, err.Error())
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if fs.redirectOnRead {
		http.Redirect(w, r, urlString, http.StatusFound)
		return
	}
	u, _ := url.Parse(urlString)
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
	resp, do_err := util.HttpDo(request)
	if do_err != nil {
		glog.V(0).Infoln("failing to connect to volume server", do_err.Error())
		writeJsonError(w, r, http.StatusInternalServerError, do_err)
		return
	}
	defer resp.Body.Close()
	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

type analogueReader struct {
	*bytes.Buffer
}

// So that it implements the io.ReadCloser interface
func (m analogueReader) Close() error { return nil }

func (fs *FilerServer) PostHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	replication := query.Get("replication")
	if replication == "" {
		replication = fs.defaultReplication
	}
	collection := query.Get("collection")
	if collection == "" {
		collection = fs.collection
	}

	var fileId string
	var err error
	var urlLocation string
	if r.Method == "PUT" {
		buf, _ := ioutil.ReadAll(r.Body)
		r.Body = analogueReader{bytes.NewBuffer(buf)}
		fileName, _, _, _, _, _, _, pe := storage.ParseUpload(r)
		if pe != nil {
			glog.V(0).Infoln("failing to parse post body", pe.Error())
			writeJsonError(w, r, http.StatusInternalServerError, pe)
			return
		}
		//reconstruct http request body for following new request to volume server
		r.Body = analogueReader{bytes.NewBuffer(buf)}

		path := r.URL.Path
		if strings.HasSuffix(path, "/") {
			if fileName != "" {
				path += fileName
			}
		}

		if fileId, err = fs.filer.FindFile(path); err != nil && err != leveldb.ErrNotFound {
			glog.V(0).Infoln("failing to find path in filer store", path, err.Error())
			writeJsonError(w, r, http.StatusInternalServerError, err)
			return
		} else if fileId != "" && err == nil {
			var le error
			urlLocation, le = operation.LookupFileId(fs.master, fileId, collection, false)
			if le != nil {
				glog.V(1).Infoln("operation LookupFileId %s failed, err is %s", fileId, le.Error())
				w.WriteHeader(http.StatusNotFound)
				return
			}
		}
	} else {
		assignResult, ae := operation.Assign(fs.master, 1, replication, collection, query.Get("ttl"))
		if ae != nil {
			glog.V(0).Infoln("failing to assign a file id", ae.Error())
			writeJsonError(w, r, http.StatusInternalServerError, ae)
			return
		}
		fileId = assignResult.Fid
		urlLocation = "http://" + assignResult.Url + "/" + assignResult.Fid
	}

	u, _ := url.Parse(urlLocation)
	glog.V(4).Infoln("post to", u)
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
	resp, do_err := util.HttpDo(request)
	if do_err != nil {
		glog.V(0).Infoln("failing to connect to volume server", r.RequestURI, do_err.Error())
		writeJsonError(w, r, http.StatusInternalServerError, do_err)
		return
	}
	defer resp.Body.Close()
	resp_body, ra_err := ioutil.ReadAll(resp.Body)
	if ra_err != nil {
		glog.V(0).Infoln("failing to upload to volume server", r.RequestURI, ra_err.Error())
		writeJsonError(w, r, http.StatusInternalServerError, ra_err)
		return
	}
	glog.V(4).Infoln("post result", string(resp_body))
	var ret operation.UploadResult
	unmarshal_err := json.Unmarshal(resp_body, &ret)
	if unmarshal_err != nil {
		glog.V(0).Infoln("failing to read upload resonse", r.RequestURI, string(resp_body))
		writeJsonError(w, r, http.StatusInternalServerError, unmarshal_err)
		return
	}
	if ret.Error != "" {
		glog.V(0).Infoln("failing to post to volume server", r.RequestURI, ret.Error)
		writeJsonError(w, r, http.StatusInternalServerError, errors.New(ret.Error))
		return
	}
	path := r.URL.Path
	if strings.HasSuffix(path, "/") {
		if ret.Name != "" {
			path += ret.Name
		} else {
			operation.DeleteFile(fs.master, fileId, collection, fs.jwt(fileId)) //clean up
			glog.V(0).Infoln("Can not to write to folder", path, "without a file name!")
			writeJsonError(w, r, http.StatusInternalServerError,
				errors.New("Can not to write to folder "+path+" without a file name"))
			return
		}
	}
	glog.V(4).Infoln("saving", path, "=>", fileId)
	if db_err := fs.filer.CreateFile(path, fileId); db_err != nil {
		operation.DeleteFile(fs.master, fileId, collection, fs.jwt(fileId)) //clean up
		glog.V(0).Infof("failing to write %s to filer server : %v", path, db_err)
		writeJsonError(w, r, http.StatusInternalServerError, db_err)
		return
	}
	w.WriteHeader(http.StatusCreated)
	w.Write(resp_body)
}

// curl -X DELETE http://localhost:8888/path/to
// curl -X DELETE http://localhost:8888/path/to?recursive=true
func (fs *FilerServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var fid string
	if strings.HasSuffix(r.URL.Path, "/") {
		isRecursive := r.FormValue("recursive") == "true"
		err = fs.filer.DeleteDirectory(r.URL.Path, isRecursive)
	} else {
		fid, err = fs.filer.DeleteFile(r.URL.Path)
		if err == nil && fid != "" {
			err = operation.DeleteFile(fs.master, fid, r.FormValue("collection"), fs.jwt(fid))
		}
	}
	if err == nil {
		writeJsonQuiet(w, r, http.StatusAccepted, map[string]string{"error": ""})
	} else {
		glog.V(4).Infoln("deleting", r.URL.Path, ":", err.Error())
		writeJsonError(w, r, http.StatusInternalServerError, err)
	}
}
