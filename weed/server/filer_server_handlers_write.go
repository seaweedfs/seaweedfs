package weed_server

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type FilerPostResult struct {
	Name  string `json:"name,omitempty"`
	Size  uint32 `json:"size,omitempty"`
	Error string `json:"error,omitempty"`
	Fid   string `json:"fid,omitempty"`
	Url   string `json:"url,omitempty"`
}

func (fs *FilerServer) queryFileInfoByPath(w http.ResponseWriter, r *http.Request, path string) (fileId, urlLocation string, err error) {
	var entry *filer2.Entry
	entry, err = fs.filer.FindEntry(filer2.FullPath(path))
	if err == filer2.ErrNotFound {
		return "", "", nil
	}

	if err != nil {
		glog.V(0).Infoln("failing to find path in filer store", path, err.Error())
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	if len(entry.Chunks) == 0 {
		glog.V(1).Infof("empty entry: %s", path)
		w.WriteHeader(http.StatusNoContent)
	} else {
		fileId = entry.Chunks[0].FileId
		urlLocation, err = operation.LookupFileId(fs.filer.GetMaster(), fileId)
		if err != nil {
			glog.V(1).Infof("operation LookupFileId %s failed, err is %s", fileId, err.Error())
			w.WriteHeader(http.StatusNotFound)
		}
	}
	return
}

func (fs *FilerServer) assignNewFileInfo(w http.ResponseWriter, r *http.Request, replication, collection string, dataCenter string) (fileId, urlLocation string, err error) {
	ar := &operation.VolumeAssignRequest{
		Count:       1,
		Replication: replication,
		Collection:  collection,
		Ttl:         r.URL.Query().Get("ttl"),
		DataCenter:  dataCenter,
	}
	var altRequest *operation.VolumeAssignRequest
	if dataCenter != "" {
		altRequest = &operation.VolumeAssignRequest{
			Count:       1,
			Replication: replication,
			Collection:  collection,
			Ttl:         r.URL.Query().Get("ttl"),
			DataCenter:  "",
		}
	}

	assignResult, ae := operation.Assign(fs.filer.GetMaster(), ar, altRequest)
	if ae != nil {
		glog.Errorf("failing to assign a file id: %v", ae)
		writeJsonError(w, r, http.StatusInternalServerError, ae)
		err = ae
		return
	}
	fileId = assignResult.Fid
	urlLocation = "http://" + assignResult.Url + "/" + assignResult.Fid
	return
}

func (fs *FilerServer) PostHandler(w http.ResponseWriter, r *http.Request) {

	query := r.URL.Query()
	replication := query.Get("replication")
	if replication == "" {
		replication = fs.option.DefaultReplication
	}
	collection := query.Get("collection")
	if collection == "" {
		collection = fs.option.Collection
	}
	dataCenter := query.Get("dataCenter")
	if dataCenter == "" {
		dataCenter = fs.option.DataCenter
	}

	if autoChunked := fs.autoChunk(w, r, replication, collection, dataCenter); autoChunked {
		return
	}

	/*
	var path string
	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data; boundary=") {
		path, err = fs.multipartUploadAnalyzer(w, r, replication, collection, dataCenter)
	} else {
		path, err = fs.monolithicUploadAnalyzer(w, r, replication, collection, dataCenter)
	}
	*/

	fileId, urlLocation, err := fs.queryFileInfoByPath(w, r, r.URL.Path)
	if fileId, urlLocation, err = fs.queryFileInfoByPath(w, r, r.URL.Path); err == nil && fileId == "" {
		fileId, urlLocation, err = fs.assignNewFileInfo(w, r, replication, collection, dataCenter)
	}
	if err != nil || fileId == "" || urlLocation == "" {
		return
	}

	glog.V(0).Infof("request header %+v, urlLocation: %v", r.Header, urlLocation)

	u, _ := url.Parse(urlLocation)

	// This allows a client to generate a chunk manifest and submit it to the filer -- it is a little off
	// because they need to provide FIDs instead of file paths...
	cm, _ := strconv.ParseBool(query.Get("cm"))
	if cm {
		q := u.Query()
		q.Set("cm", "true")
		u.RawQuery = q.Encode()
	}
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
		glog.Errorf("failing to connect to volume server %s: %v, %+v", r.RequestURI, do_err, r.Method)
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
			operation.DeleteFile(fs.filer.GetMaster(), fileId, fs.jwt(fileId)) //clean up
			glog.V(0).Infoln("Can not to write to folder", path, "without a file name!")
			writeJsonError(w, r, http.StatusInternalServerError,
				errors.New("Can not to write to folder "+path+" without a file name"))
			return
		}
	}

	// also delete the old fid unless PUT operation
	if r.Method != "PUT" {
		if entry, err := fs.filer.FindEntry(filer2.FullPath(path)); err == nil {
			oldFid := entry.Chunks[0].FileId
			operation.DeleteFile(fs.filer.GetMaster(), oldFid, fs.jwt(oldFid))
		} else if err != nil && err != filer2.ErrNotFound {
			glog.V(0).Infof("error %v occur when finding %s in filer store", err, path)
		}
	}

	glog.V(4).Infoln("saving", path, "=>", fileId)
	entry := &filer2.Entry{
		FullPath: filer2.FullPath(path),
		Attr: filer2.Attr{
			Mtime:       time.Now(),
			Crtime:      time.Now(),
			Mode:        0660,
			Replication: replication,
			Collection:  collection,
			TtlSec:      int32(util.ParseInt(r.URL.Query().Get("ttl"), 0)),
		},
		Chunks: []*filer_pb.FileChunk{{
			FileId: fileId,
			Size:   uint64(r.ContentLength),
			Mtime:  time.Now().UnixNano(),
		}},
	}
	if db_err := fs.filer.CreateEntry(entry); db_err != nil {
		operation.DeleteFile(fs.filer.GetMaster(), fileId, fs.jwt(fileId)) //clean up
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
func (fs *FilerServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {

	err := fs.filer.DeleteEntryMetaAndData(filer2.FullPath(r.URL.Path), false, true)
	if err != nil {
		glog.V(4).Infoln("deleting", r.URL.Path, ":", err.Error())
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	writeJsonQuiet(w, r, http.StatusAccepted, map[string]string{"error": ""})
}
