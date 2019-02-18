package weed_server

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	OS_UID = uint32(os.Getuid())
	OS_GID = uint32(os.Getgid())
)

type FilerPostResult struct {
	Name  string `json:"name,omitempty"`
	Size  uint32 `json:"size,omitempty"`
	Error string `json:"error,omitempty"`
	Fid   string `json:"fid,omitempty"`
	Url   string `json:"url,omitempty"`
}

func (fs *FilerServer) assignNewFileInfo(w http.ResponseWriter, r *http.Request, replication, collection string, dataCenter string) (fileId, urlLocation string, auth security.EncodedJwt, err error) {
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

	assignResult, ae := operation.Assign(fs.filer.GetMaster(), fs.grpcDialOption, ar, altRequest)
	if ae != nil {
		glog.Errorf("failing to assign a file id: %v", ae)
		writeJsonError(w, r, http.StatusInternalServerError, ae)
		err = ae
		return
	}
	fileId = assignResult.Fid
	urlLocation = "http://" + assignResult.Url + "/" + assignResult.Fid
	auth = assignResult.Auth
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

	fileId, urlLocation, auth, err := fs.assignNewFileInfo(w, r, replication, collection, dataCenter)

	if err != nil || fileId == "" || urlLocation == "" {
		glog.V(0).Infof("fail to allocate volume for %s, collection:%s, datacenter:%s", r.URL.Path, collection, dataCenter)
		return
	}

	glog.V(4).Infof("write %s to %v", r.URL.Path, urlLocation)

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

	// send request to volume server
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
	if auth != "" {
		request.Header.Set("Authorization", "BEARER "+string(auth))
	}
	resp, do_err := util.Do(request)
	if do_err != nil {
		glog.Errorf("failing to connect to volume server %s: %v, %+v", r.RequestURI, do_err, r.Method)
		writeJsonError(w, r, http.StatusInternalServerError, do_err)
		return
	}
	defer resp.Body.Close()
	etag := resp.Header.Get("ETag")
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

	// find correct final path
	path := r.URL.Path
	if strings.HasSuffix(path, "/") {
		if ret.Name != "" {
			path += ret.Name
		} else {
			fs.filer.DeleteFileByFileId(fileId)
			glog.V(0).Infoln("Can not to write to folder", path, "without a file name!")
			writeJsonError(w, r, http.StatusInternalServerError,
				errors.New("Can not to write to folder "+path+" without a file name"))
			return
		}
	}

	// update metadata in filer store
	existingEntry, err := fs.filer.FindEntry(filer2.FullPath(path))
	crTime := time.Now()
	if err == nil && existingEntry != nil {
		// glog.V(4).Infof("existing %s => %+v", path, existingEntry)
		if existingEntry.IsDirectory() {
			path += "/" + ret.Name
		} else {
			crTime = existingEntry.Crtime
		}
	}
	entry := &filer2.Entry{
		FullPath: filer2.FullPath(path),
		Attr: filer2.Attr{
			Mtime:       time.Now(),
			Crtime:      crTime,
			Mode:        0660,
			Uid:         OS_UID,
			Gid:         OS_GID,
			Replication: replication,
			Collection:  collection,
			TtlSec:      int32(util.ParseInt(r.URL.Query().Get("ttl"), 0)),
		},
		Chunks: []*filer_pb.FileChunk{{
			FileId: fileId,
			Size:   uint64(ret.Size),
			Mtime:  time.Now().UnixNano(),
			ETag:   etag,
		}},
	}
	// glog.V(4).Infof("saving %s => %+v", path, entry)
	if db_err := fs.filer.CreateEntry(entry); db_err != nil {
		fs.filer.DeleteFileByFileId(fileId)
		glog.V(0).Infof("failing to write %s to filer server : %v", path, db_err)
		writeJsonError(w, r, http.StatusInternalServerError, db_err)
		return
	}

	// send back post result
	reply := FilerPostResult{
		Name:  ret.Name,
		Size:  ret.Size,
		Error: ret.Error,
		Fid:   fileId,
		Url:   urlLocation,
	}
	setEtag(w, etag)
	writeJsonQuiet(w, r, http.StatusCreated, reply)
}

// curl -X DELETE http://localhost:8888/path/to
// curl -X DELETE http://localhost:8888/path/to?recursive=true
func (fs *FilerServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {

	isRecursive := r.FormValue("recursive") == "true"

	err := fs.filer.DeleteEntryMetaAndData(filer2.FullPath(r.URL.Path), isRecursive, true)
	if err != nil {
		glog.V(1).Infoln("deleting", r.URL.Path, ":", err.Error())
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
