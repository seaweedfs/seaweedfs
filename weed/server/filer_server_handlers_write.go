package weed_server

import (
	"context"
	"net/http"
	"os"
	"strings"
	"time"

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

func (fs *FilerServer) assignNewFileInfo(replication, collection, dataCenter, ttlString string, fsync bool) (fileId, urlLocation string, auth security.EncodedJwt, err error) {

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

	fs.autoChunk(ctx, w, r, replication, collection, dataCenter, ttlSeconds, ttlString, fsync)

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

	objectPath := r.URL.Path
	if len(r.URL.Path) > 1 && strings.HasSuffix(objectPath, "/") {
		objectPath = objectPath[0 : len(objectPath)-1]
	}

	err := fs.filer.DeleteEntryMetaAndData(context.Background(), util.FullPath(objectPath), isRecursive, ignoreRecursiveError, !skipChunkDeletion, false)
	if err != nil {
		glog.V(1).Infoln("deleting", objectPath, ":", err.Error())
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
