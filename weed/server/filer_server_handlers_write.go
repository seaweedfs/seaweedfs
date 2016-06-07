package weed_server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/syndtr/goleveldb/leveldb"
)

type analogueReader struct {
	*bytes.Buffer
}

// So that it implements the io.ReadCloser interface
func (m analogueReader) Close() error { return nil }

type FilerPostResult struct {
	Name  string `json:"name,omitempty"`
	Size  uint32 `json:"size,omitempty"`
	Error string `json:"error,omitempty"`
	Fid   string `json:"fid,omitempty"`
	Url   string `json:"url,omitempty"`
}

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
			urlLocation, le = operation.LookupFileId(fs.getMasterNode(), fileId)
			if le != nil {
				glog.V(1).Infoln("operation LookupFileId %s failed, err is %s", fileId, le.Error())
				w.WriteHeader(http.StatusNotFound)
				return
			}
		}
	} else {
		assignResult, ae := operation.Assign(fs.getMasterNode(), 1, replication, collection, query.Get("ttl"))
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
	resp, do_err := util.Do(request)
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
			operation.DeleteFile(fs.getMasterNode(), fileId, fs.jwt(fileId)) //clean up
			glog.V(0).Infoln("Can not to write to folder", path, "without a file name!")
			writeJsonError(w, r, http.StatusInternalServerError,
				errors.New("Can not to write to folder "+path+" without a file name"))
			return
		}
	}

	// also delete the old fid unless PUT operation
	if r.Method != "PUT" {
		if oldFid, err := fs.filer.FindFile(path); err == nil {
			operation.DeleteFile(fs.getMasterNode(), oldFid, fs.jwt(oldFid))
		}
	}

	glog.V(4).Infoln("saving", path, "=>", fileId)
	if db_err := fs.filer.CreateFile(path, fileId); db_err != nil {
		operation.DeleteFile(fs.getMasterNode(), fileId, fs.jwt(fileId)) //clean up
		glog.V(0).Infof("failing to write %s to filer server : %v", path, db_err)
		writeJsonError(w, r, http.StatusInternalServerError, db_err)
		return
	}

	reply := FilerPostResult{
		Name:  ret.Name,
		Size:  ret.Size,
		Error: ret.Error,
		Fid:   fileId,
		Url:   urlLocation,
	}
	writeJsonQuiet(w, r, http.StatusCreated, reply)
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
			err = operation.DeleteFile(fs.getMasterNode(), fid, fs.jwt(fid))
		}
	}
	if err == nil {
		writeJsonQuiet(w, r, http.StatusAccepted, map[string]string{"error": ""})
	} else {
		glog.V(4).Infoln("deleting", r.URL.Path, ":", err.Error())
		writeJsonError(w, r, http.StatusInternalServerError, err)
	}
}
