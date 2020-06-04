package weed_server

import (
	"context"
	"crypto/md5"
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
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	OS_UID = uint32(os.Getuid())
	OS_GID = uint32(os.Getgid())
)

type FilerPostResult struct {
	Name  string `json:"name,omitempty"`
	Size  int64  `json:"size,omitempty"`
	Error string `json:"error,omitempty"`
	Fid   string `json:"fid,omitempty"`
	Url   string `json:"url,omitempty"`
}

func (fs *FilerServer) assignNewFileInfo(w http.ResponseWriter, r *http.Request, replication, collection, dataCenter, ttlString string, fsync bool) (fileId, urlLocation string, auth security.EncodedJwt, err error) {

	stats.FilerRequestCounter.WithLabelValues("assign").Inc()
	start := time.Now()
	defer func() { stats.FilerRequestHistogram.WithLabelValues("assign").Observe(time.Since(start).Seconds()) }()

	ar := &operation.VolumeAssignRequest{
		Count:       1,
		Replication: replication,
		Collection:  collection,
		Ttl:         ttlString,
		DataCenter:  dataCenter,
	}
	var altRequest *operation.VolumeAssignRequest
	if dataCenter != "" {
		altRequest = &operation.VolumeAssignRequest{
			Count:       1,
			Replication: replication,
			Collection:  collection,
			Ttl:         ttlString,
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
	if fsync {
		urlLocation += "?fsync=true"
	}
	auth = assignResult.Auth
	return
}

func (fs *FilerServer) PostHandler(w http.ResponseWriter, r *http.Request) {

	ctx := context.Background()

	query := r.URL.Query()
	collection, replication, fsync := fs.detectCollection(r.RequestURI, query.Get("collection"), query.Get("replication"))
	dataCenter := query.Get("dataCenter")
	if dataCenter == "" {
		dataCenter = fs.option.DataCenter
	}
	ttlString := r.URL.Query().Get("ttl")

	// read ttl in seconds
	ttl, err := needle.ReadTTL(ttlString)
	ttlSeconds := int32(0)
	if err == nil {
		ttlSeconds = int32(ttl.Minutes()) * 60
	}

	if autoChunked := fs.autoChunk(ctx, w, r, replication, collection, dataCenter, ttlSeconds, ttlString, fsync); autoChunked {
		return
	}

	if fs.option.Cipher {
		reply, err := fs.encrypt(ctx, w, r, replication, collection, dataCenter, ttlSeconds, ttlString, fsync)
		if err != nil {
			writeJsonError(w, r, http.StatusInternalServerError, err)
		} else if reply != nil {
			writeJsonQuiet(w, r, http.StatusCreated, reply)
		}

		return
	}

	fileId, urlLocation, auth, err := fs.assignNewFileInfo(w, r, replication, collection, dataCenter, ttlString, fsync)

	if err != nil || fileId == "" || urlLocation == "" {
		glog.V(0).Infof("fail to allocate volume for %s, collection:%s, datacenter:%s", r.URL.Path, collection, dataCenter)
		writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("fail to allocate volume for %s, collection:%s, datacenter:%s", r.URL.Path, collection, dataCenter))
		return
	}

	glog.V(4).Infof("write %s to %v", r.URL.Path, urlLocation)

	u, _ := url.Parse(urlLocation)
	ret, md5value, err := fs.uploadToVolumeServer(r, u, auth, w, fileId)
	if err != nil {
		return
	}

	if err = fs.updateFilerStore(ctx, r, w, replication, collection, ret, md5value, fileId, ttlSeconds); err != nil {
		return
	}

	// send back post result
	reply := FilerPostResult{
		Name:  ret.Name,
		Size:  int64(ret.Size),
		Error: ret.Error,
		Fid:   fileId,
		Url:   urlLocation,
	}
	setEtag(w, ret.ETag)
	writeJsonQuiet(w, r, http.StatusCreated, reply)
}

// update metadata in filer store
func (fs *FilerServer) updateFilerStore(ctx context.Context, r *http.Request, w http.ResponseWriter, replication string,
	collection string, ret *operation.UploadResult, md5value []byte, fileId string, ttlSeconds int32) (err error) {

	stats.FilerRequestCounter.WithLabelValues("postStoreWrite").Inc()
	start := time.Now()
	defer func() {
		stats.FilerRequestHistogram.WithLabelValues("postStoreWrite").Observe(time.Since(start).Seconds())
	}()

	modeStr := r.URL.Query().Get("mode")
	if modeStr == "" {
		modeStr = "0660"
	}
	mode, err := strconv.ParseUint(modeStr, 8, 32)
	if err != nil {
		glog.Errorf("Invalid mode format: %s, use 0660 by default", modeStr)
		mode = 0660
	}

	path := r.URL.Path
	if strings.HasSuffix(path, "/") {
		if ret.Name != "" {
			path += ret.Name
		}
	}
	existingEntry, err := fs.filer.FindEntry(ctx, util.FullPath(path))
	crTime := time.Now()
	if err == nil && existingEntry != nil {
		crTime = existingEntry.Crtime
	}
	entry := &filer2.Entry{
		FullPath: util.FullPath(path),
		Attr: filer2.Attr{
			Mtime:       time.Now(),
			Crtime:      crTime,
			Mode:        os.FileMode(mode),
			Uid:         OS_UID,
			Gid:         OS_GID,
			Replication: replication,
			Collection:  collection,
			TtlSec:      ttlSeconds,
			Mime:        ret.Mime,
			Md5:         md5value,
		},
		Chunks: []*filer_pb.FileChunk{{
			FileId: fileId,
			Size:   uint64(ret.Size),
			Mtime:  time.Now().UnixNano(),
			ETag:   ret.ETag,
		}},
	}
	if entry.Attr.Mime == "" {
		if ext := filenamePath.Ext(path); ext != "" {
			entry.Attr.Mime = mime.TypeByExtension(ext)
		}
	}
	// glog.V(4).Infof("saving %s => %+v", path, entry)
	if dbErr := fs.filer.CreateEntry(ctx, entry, false); dbErr != nil {
		fs.filer.DeleteChunks(entry.Chunks)
		glog.V(0).Infof("failing to write %s to filer server : %v", path, dbErr)
		writeJsonError(w, r, http.StatusInternalServerError, dbErr)
		err = dbErr
		return
	}

	return nil
}

// send request to volume server
func (fs *FilerServer) uploadToVolumeServer(r *http.Request, u *url.URL, auth security.EncodedJwt, w http.ResponseWriter, fileId string) (ret *operation.UploadResult, md5value []byte, err error) {

	stats.FilerRequestCounter.WithLabelValues("postUpload").Inc()
	start := time.Now()
	defer func() { stats.FilerRequestHistogram.WithLabelValues("postUpload").Observe(time.Since(start).Seconds()) }()

	ret = &operation.UploadResult{}

	md5Hash := md5.New()
	body := r.Body
	if r.Method == "PUT" {
		// only PUT or large chunked files has Md5 in attributes
		body = ioutil.NopCloser(io.TeeReader(r.Body, md5Hash))
	}

	request := &http.Request{
		Method:        r.Method,
		URL:           u,
		Proto:         r.Proto,
		ProtoMajor:    r.ProtoMajor,
		ProtoMinor:    r.ProtoMinor,
		Header:        r.Header,
		Body:          body,
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
	// use filer calculated md5 ETag, instead of the volume server crc ETag
	if r.Method == "PUT" {
		md5value = md5Hash.Sum(nil)
	}
	ret.ETag = getEtag(resp)
	return
}

// curl -X DELETE http://localhost:8888/path/to
// curl -X DELETE http://localhost:8888/path/to?recursive=true
// curl -X DELETE http://localhost:8888/path/to?recursive=true&ignoreRecursiveError=true
// curl -X DELETE http://localhost:8888/path/to?recursive=true&skipChunkDeletion=true
func (fs *FilerServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {

	isRecursive := r.FormValue("recursive") == "true"
	if !isRecursive && fs.option.recursiveDelete {
		if r.FormValue("recursive") != "false" {
			isRecursive = true
		}
	}
	ignoreRecursiveError := r.FormValue("ignoreRecursiveError") == "true"
	skipChunkDeletion := r.FormValue("skipChunkDeletion") == "true"

	err := fs.filer.DeleteEntryMetaAndData(context.Background(), util.FullPath(r.URL.Path), isRecursive, ignoreRecursiveError, !skipChunkDeletion)
	if err != nil {
		glog.V(1).Infoln("deleting", r.URL.Path, ":", err.Error())
		httpStatus := http.StatusInternalServerError
		if err == filer_pb.ErrNotFound {
			httpStatus = http.StatusNotFound
		}
		writeJsonError(w, r, httpStatus, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (fs *FilerServer) detectCollection(requestURI, qCollection, qReplication string) (collection, replication string, fsync bool) {
	// default
	collection = fs.option.Collection
	replication = fs.option.DefaultReplication

	// get default collection settings
	if qCollection != "" {
		collection = qCollection
	}
	if qReplication != "" {
		replication = qReplication
	}

	// required by buckets folder
	if strings.HasPrefix(requestURI, fs.filer.DirBucketsPath+"/") {
		bucketAndObjectKey := requestURI[len(fs.filer.DirBucketsPath)+1:]
		t := strings.Index(bucketAndObjectKey, "/")
		if t < 0 {
			collection = bucketAndObjectKey
		}
		if t > 0 {
			collection = bucketAndObjectKey[:t]
		}
		replication, fsync = fs.filer.ReadBucketOption(collection)
	}

	return
}
