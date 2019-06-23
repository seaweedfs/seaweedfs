package weed_server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"net/url"
	"os"
	filenamePath "path"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/stats"
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

	stats.FilerRequestCounter.WithLabelValues("assign").Inc()
	start := time.Now()
	defer func() { stats.FilerRequestHistogram.WithLabelValues("assign").Observe(time.Since(start).Seconds()) }()

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

	stats.FilerRequestCounter.WithLabelValues("post").Inc()
	start := time.Now()
	defer func() { stats.FilerRequestHistogram.WithLabelValues("post").Observe(time.Since(start).Seconds()) }()

	ctx := context.Background()

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

	if autoChunked := fs.autoChunk(ctx, w, r, replication, collection, dataCenter); autoChunked {
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

	ret, err := fs.uploadToVolumeServer(r, u, auth, w, fileId)
	if err != nil {
		return
	}

	if err = fs.updateFilerStore(ctx, r, w, replication, collection, ret, fileId); err != nil {
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
	setEtag(w, ret.ETag)
	writeJsonQuiet(w, r, http.StatusCreated, reply)
}

// update metadata in filer store
func (fs *FilerServer) updateFilerStore(ctx context.Context, r *http.Request, w http.ResponseWriter,
	replication string, collection string, ret operation.UploadResult, fileId string) (err error) {

	stats.FilerRequestCounter.WithLabelValues("postStoreWrite").Inc()
	start := time.Now()
	defer func() { stats.FilerRequestHistogram.WithLabelValues("postStoreWrite").Observe(time.Since(start).Seconds()) }()

	path := r.URL.Path
	existingEntry, err := fs.filer.FindEntry(ctx, filer2.FullPath(path))
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
			ETag:   ret.ETag,
		}},
	}
	if ext := filenamePath.Ext(path); ext != "" {
		entry.Attr.Mime = mime.TypeByExtension(ext)
	}
	// glog.V(4).Infof("saving %s => %+v", path, entry)
	if dbErr := fs.filer.CreateEntry(ctx, entry); dbErr != nil {
		fs.filer.DeleteChunks(entry.FullPath, entry.Chunks)
		glog.V(0).Infof("failing to write %s to filer server : %v", path, dbErr)
		writeJsonError(w, r, http.StatusInternalServerError, dbErr)
		err = dbErr
		return
	}

	return nil
}

// send request to volume server
func (fs *FilerServer) uploadToVolumeServer(r *http.Request, u *url.URL, auth security.EncodedJwt, w http.ResponseWriter, fileId string) (ret operation.UploadResult, err error) {

	stats.FilerRequestCounter.WithLabelValues("postUpload").Inc()
	start := time.Now()
	defer func() { stats.FilerRequestHistogram.WithLabelValues("postUpload").Observe(time.Since(start).Seconds()) }()

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
	resp, doErr := util.Do(request)
	if doErr != nil {
		glog.Errorf("failing to connect to volume server %s: %v, %+v", r.RequestURI, doErr, r.Method)
		writeJsonError(w, r, http.StatusInternalServerError, doErr)
		err = doErr
		return
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()
	etag := resp.Header.Get("ETag")
	respBody, raErr := ioutil.ReadAll(resp.Body)
	if raErr != nil {
		glog.V(0).Infoln("failing to upload to volume server", r.RequestURI, raErr.Error())
		writeJsonError(w, r, http.StatusInternalServerError, raErr)
		err = raErr
		return
	}
	glog.V(4).Infoln("post result", string(respBody))
	unmarshalErr := json.Unmarshal(respBody, &ret)
	if unmarshalErr != nil {
		glog.V(0).Infoln("failing to read upload resonse", r.RequestURI, string(respBody))
		writeJsonError(w, r, http.StatusInternalServerError, unmarshalErr)
		err = unmarshalErr
		return
	}
	if ret.Error != "" {
		err = errors.New(ret.Error)
		glog.V(0).Infoln("failing to post to volume server", r.RequestURI, ret.Error)
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}
	// find correct final path
	path := r.URL.Path
	if strings.HasSuffix(path, "/") {
		if ret.Name != "" {
			path += ret.Name
		} else {
			err = fmt.Errorf("can not to write to folder %s without a file name", path)
			fs.filer.DeleteFileByFileId(fileId)
			glog.V(0).Infoln("Can not to write to folder", path, "without a file name!")
			writeJsonError(w, r, http.StatusInternalServerError, err)
			return
		}
	}
	if etag != "" {
		ret.ETag = etag
	}
	return
}

// curl -X DELETE http://localhost:8888/path/to
// curl -X DELETE http://localhost:8888/path/to?recursive=true
func (fs *FilerServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {

	stats.FilerRequestCounter.WithLabelValues("delete").Inc()
	start := time.Now()
	defer func() { stats.FilerRequestHistogram.WithLabelValues("delete").Observe(time.Since(start).Seconds()) }()

	isRecursive := r.FormValue("recursive") == "true"

	err := fs.filer.DeleteEntryMetaAndData(context.Background(), filer2.FullPath(r.URL.Path), isRecursive, true)
	if err != nil {
		glog.V(1).Infoln("deleting", r.URL.Path, ":", err.Error())
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
