package topology

import (
	"bytes"
	"net/http"
	"strconv"

	"net"
	"net/url"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func ReplicatedWrite(masterNode string, s *storage.Store,
	volumeId storage.VolumeId, needle *storage.Needle,
	r *http.Request) (size uint32, errorStatus string) {
	//check JWT
	jwt := security.GetJwt(r)
	defer func() {
		if errorStatus == "" {
			return
		}
		ReplicatedDelete(masterNode, s, volumeId, needle, r)
	}()
	ret, err := s.Write(volumeId, needle)
	if err != nil {
		errorStatus = "Failed to write to local disk (" + err.Error() + ")"
	} else if ret <= 0 {
		errorStatus = "Failed to write to local disk"
	}
	//send to other replica locations
	if r.FormValue("type") != "replicate" {
		repWrite := func(location operation.Location) bool {
			args := url.Values{
				"type": {"replicate"},
			}
			if needle.LastModified > 0 {
				args.Set("ts", strconv.FormatUint(needle.LastModified, 10))
			}
			if needle.IsChunkedManifest() {
				args.Set("cm", "true")
			}

			u := util.MkUrl(location.Url, r.URL.Path, args)
			glog.V(4).Infoln("write replication to", u)
			_, err := operation.Upload(u,
				string(needle.Name), bytes.NewReader(needle.Data), needle.IsGzipped(), string(needle.Mime),
				jwt)
			if err != nil {
				glog.V(0).Infof("write replication to %s err, %v", u, err)
			}
			return err == nil
		}
		if !distributedOperation(masterNode, s, volumeId, repWrite) {
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
	collection := ""
	if v := store.GetVolume(volumeId); v != nil {
		collection = v.Collection
	}
	if lookupResult, lookupErr := operation.LookupNoCache(masterNode, volumeId.String(), collection); lookupErr == nil {
		length := 0
		selfUrl := net.JoinHostPort(store.GetIP(), strconv.Itoa(store.Port))
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
		if rp := store.GetVolumeReplicaPlacement(volumeId); rp != nil {
			if length+1 < rp.GetCopyCount() {
				glog.V(0).Infof("replicating opetations [%d] is less than volume's replication copy count [%d]", length+1, rp.GetCopyCount())
				ret = false
			}
		}
		return ret
	} else {
		glog.V(0).Infoln("Failed to lookup for", volumeId, lookupErr.Error())
	}
	return false
}
