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

func (fs *FilerServer) assignNewFileInfo(so *operation.StorageOption) (fileId, urlLocation string, auth security.EncodedJwt, err error) {

	stats.FilerRequestCounter.WithLabelValues("assign").Inc()
	start := time.Now()
	defer func() { stats.FilerRequestHistogram.WithLabelValues("assign").Observe(time.Since(start).Seconds()) }()

	ar, altRequest := so.ToAssignRequests(1)

	assignResult, ae := operation.Assign(fs.filer.GetMaster(), fs.grpcDialOption, ar, altRequest)
	if ae != nil {
		glog.Errorf("failing to assign a file id: %v", ae)
		err = ae
		return
	}
	fileId = assignResult.Fid
	urlLocation = "http://" + assignResult.Url + "/" + assignResult.Fid
	if so.Fsync {
		urlLocation += "?fsync=true"
	}
	auth = assignResult.Auth
	return
}

func (fs *FilerServer) PostHandler(w http.ResponseWriter, r *http.Request) {

	ctx := context.Background()

	query := r.URL.Query()
	so := fs.detectStorageOption0(r.RequestURI,
		query.Get("collection"),
		query.Get("replication"),
		query.Get("ttl"),
		query.Get("dataCenter"),
		query.Get("rack"),
	)

	fs.autoChunk(ctx, w, r, so)
	util.CloseRequest(r)

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

	err := fs.filer.DeleteEntryMetaAndData(context.Background(), util.FullPath(objectPath), isRecursive, ignoreRecursiveError, !skipChunkDeletion, false, nil)
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

func (fs *FilerServer) detectStorageOption(requestURI, qCollection, qReplication string, ttlSeconds int32, dataCenter, rack string) *operation.StorageOption {
	collection := util.Nvl(qCollection, fs.option.Collection)
	replication := util.Nvl(qReplication, fs.option.DefaultReplication)

	// required by buckets folder
	bucketDefaultReplication, fsync := "", false
	if strings.HasPrefix(requestURI, fs.filer.DirBucketsPath+"/") {
		collection = fs.filer.DetectBucket(util.FullPath(requestURI))
		bucketDefaultReplication, fsync = fs.filer.ReadBucketOption(collection)
	}
	if replication == "" {
		replication = bucketDefaultReplication
	}

	rule := fs.filer.FilerConf.MatchStorageRule(requestURI)

	if ttlSeconds == 0 {
		ttl, err := needle.ReadTTL(rule.GetTtl())
		if err != nil {
			glog.Errorf("fail to parse %s ttl setting %s: %v", rule.LocationPrefix, rule.Ttl, err)
		}
		ttlSeconds = int32(ttl.Minutes()) * 60
	}

	return &operation.StorageOption{
		Replication:       util.Nvl(replication, rule.Replication),
		Collection:        util.Nvl(collection, rule.Collection),
		DataCenter:        util.Nvl(dataCenter, fs.option.DataCenter),
		Rack:              util.Nvl(rack, fs.option.Rack),
		TtlSeconds:        ttlSeconds,
		Fsync:             fsync || rule.Fsync,
		VolumeGrowthCount: rule.VolumeGrowthCount,
	}
}

func (fs *FilerServer) detectStorageOption0(requestURI, qCollection, qReplication string, qTtl string, dataCenter, rack string) *operation.StorageOption {

	ttl, err := needle.ReadTTL(qTtl)
	if err != nil {
		glog.Errorf("fail to parse ttl %s: %v", qTtl, err)
	}

	return fs.detectStorageOption(requestURI, qCollection, qReplication, int32(ttl.Minutes())*60, dataCenter, rack)
}
