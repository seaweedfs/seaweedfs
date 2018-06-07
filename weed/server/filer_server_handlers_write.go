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
	if entry, err = fs.filer.FindEntry(filer2.FullPath(path)); err != nil {
		glog.V(0).Infoln("failing to find path in filer store", path, err.Error())
		writeJsonError(w, r, http.StatusInternalServerError, err)
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

func (fs *FilerServer) assignNewFileInfo(w http.ResponseWriter, r *http.Request, replication, collection string) (fileId, urlLocation string, err error) {
	ar := &operation.VolumeAssignRequest{
		Count:       1,
		Replication: replication,
		Collection:  collection,
		Ttl:         r.URL.Query().Get("ttl"),
	}
	assignResult, ae := operation.Assign(fs.filer.GetMaster(), ar)
	if ae != nil {
		glog.V(0).Infoln("failing to assign a file id", ae.Error())
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
		replication = fs.defaultReplication
	}
	collection := query.Get("collection")
	if collection == "" {
		collection = fs.collection
	}

	if autoChunked := fs.autoChunk(w, r, replication, collection); autoChunked {
		return
	}

	var fileId, urlLocation string
	var err error

	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data; boundary=") {
		fileId, urlLocation, err = fs.multipartUploadAnalyzer(w, r, replication, collection)
		if err != nil {
			return
		}
	} else {
		fileId, urlLocation, err = fs.monolithicUploadAnalyzer(w, r, replication, collection)
		if err != nil {
			return
		}
	}

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
			Mode: 0660,
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

	err := fs.filer.DeleteEntryMetaAndData(filer2.FullPath(r.URL.Path), true)
	if err != nil {
		glog.V(4).Infoln("deleting", r.URL.Path, ":", err.Error())
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	writeJsonQuiet(w, r, http.StatusAccepted, map[string]string{"error": ""})
}
