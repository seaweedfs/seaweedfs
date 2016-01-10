package topology

import (
	"bytes"
	"net/http"
	"strconv"

	"github.com/chrislusf/seaweedfs/go/glog"
	"github.com/chrislusf/seaweedfs/go/operation"
	"github.com/chrislusf/seaweedfs/go/security"
	"github.com/chrislusf/seaweedfs/go/storage"
	"github.com/chrislusf/seaweedfs/go/util"
)

func ReplicatedWrite(masterNode string, s *storage.Store,
	volumeId storage.VolumeId, needle *storage.Needle,
	r *http.Request) (size uint32, errorStatus string) {

	//check JWT
	jwt := security.GetJwt(r)

	ret, err := s.Write(volumeId, needle)
	if err != nil {
		errorStatus = "Failed to write to local disk (" + err.Error() + ")"
	} else if ret <= 0 {
		errorStatus = "Failed to write to local disk"
	}
	//send to other replica locations
	if r.FormValue("type") != "replicate" {
		if !distributedOperation(masterNode, s, volumeId, func(location operation.Location) bool {
			_, err := operation.Upload(
				"http://"+location.Url+r.URL.Path+"?type=replicate&ts="+strconv.FormatUint(needle.LastModified, 10),
				string(needle.Name), bytes.NewReader(needle.Data), needle.IsGzipped(), string(needle.Mime),
				jwt)
			return err == nil
		}) {
			ret = 0
			errorStatus = "Failed to write to replicas for volume " + volumeId.String()
		}
	}

	size = ret
	return
}

func ReplicatedDelete(masterNode string, store *storage.Store,
	volumeId storage.VolumeId, n *storage.Needle,
	r *http.Request) (ret uint32) {

	//check JWT
	jwt := security.GetJwt(r)

	ret, err := store.Delete(volumeId, n)
	if err != nil {
		glog.V(0).Infoln("delete error:", err)
		return
	}
	//send to other replica locations
	if r.FormValue("type") != "replicate" {
		if !distributedOperation(masterNode, store, volumeId, func(location operation.Location) bool {
			return nil == util.Delete("http://"+location.Url+r.URL.Path+"?type=replicate", jwt)
		}) {
			ret = 0
		}
	}
	return
}

func distributedOperation(masterNode string, store *storage.Store, volumeId storage.VolumeId, op func(location operation.Location) bool) bool {
	if lookupResult, lookupErr := operation.LookupNoCache(masterNode, volumeId.String()); lookupErr == nil {
		length := 0
		selfUrl := (store.Ip + ":" + strconv.Itoa(store.Port))
		results := make(chan bool)
		for _, location := range lookupResult.Locations {
			if location.Url != selfUrl {
				length++
				go func(location operation.Location, results chan bool) {
					results <- op(location)
				}(location, results)
			}
		}
		ret := true
		for i := 0; i < length; i++ {
			ret = ret && <-results
		}
		// we shouldn't check ReplicaPlacement because the needle have been written in head volume

		// if volume := store.GetVolume(volumeId); volume != nil {
		// 		if length+1 < volume.ReplicaPlacement.GetCopyCount() {
		// 			glog.V(0).Infof("replicating opetations [%d] is less than volume's replication copy count [%d]", length+1, volume.ReplicaPlacement.GetCopyCount())
		// 			ret = false
		// 		}
		// }
		return ret
	} else {
		glog.V(0).Infoln("Failed to lookup for", volumeId, lookupErr.Error())
	}
	return false
}
