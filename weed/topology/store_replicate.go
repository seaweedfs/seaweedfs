package topology

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func ReplicatedWrite(masterNode string, s *storage.Store,
	volumeId needle.VolumeId, n *needle.Needle,
	r *http.Request) (size uint32, isUnchanged bool, err error) {

	//check JWT
	jwt := security.GetJwt(r)

	size, isUnchanged, err = s.WriteVolumeNeedle(volumeId, n)
	if err != nil {
		err = fmt.Errorf("failed to write to local disk: %v", err)
		return
	}

	needToReplicate := !s.HasVolume(volumeId)
	needToReplicate = needToReplicate || s.GetVolume(volumeId).NeedToReplicate()
	if !needToReplicate {
		needToReplicate = s.GetVolume(volumeId).NeedToReplicate()
	}
	if needToReplicate { //send to other replica locations
		if r.FormValue("type") != "replicate" {

			if err = distributedOperation(masterNode, s, volumeId, func(location operation.Location) error {
				u := url.URL{
					Scheme: "http",
					Host:   location.Url,
					Path:   r.URL.Path,
				}
				q := url.Values{
					"type": {"replicate"},
					"ttl":  {n.Ttl.String()},
				}
				if n.LastModified > 0 {
					q.Set("ts", strconv.FormatUint(n.LastModified, 10))
				}
				if n.IsChunkedManifest() {
					q.Set("cm", "true")
				}
				u.RawQuery = q.Encode()

				pairMap := make(map[string]string)
				if n.HasPairs() {
					tmpMap := make(map[string]string)
					err := json.Unmarshal(n.Pairs, &tmpMap)
					if err != nil {
						glog.V(0).Infoln("Unmarshal pairs error:", err)
					}
					for k, v := range tmpMap {
						pairMap[needle.PairNamePrefix+k] = v
					}
				}

				_, err := operation.Upload(u.String(),
					string(n.Name), bytes.NewReader(n.Data), n.IsGzipped(), string(n.Mime),
					pairMap, jwt)
				return err
			}); err != nil {
				size = 0
				err = fmt.Errorf("failed to write to replicas for volume %d: %v", volumeId, err)
			}
		}
	}
	return
}

func ReplicatedDelete(masterNode string, store *storage.Store,
	volumeId needle.VolumeId, n *needle.Needle,
	r *http.Request) (uint32, error) {

	//check JWT
	jwt := security.GetJwt(r)

	ret, err := store.DeleteVolumeNeedle(volumeId, n)
	if err != nil {
		glog.V(0).Infoln("delete error:", err)
		return ret, err
	}

	needToReplicate := !store.HasVolume(volumeId)
	if !needToReplicate && ret > 0 {
		needToReplicate = store.GetVolume(volumeId).NeedToReplicate()
	}
	if needToReplicate { //send to other replica locations
		if r.FormValue("type") != "replicate" {
			if err = distributedOperation(masterNode, store, volumeId, func(location operation.Location) error {
				return util.Delete("http://"+location.Url+r.URL.Path+"?type=replicate", string(jwt))
			}); err != nil {
				ret = 0
			}
		}
	}
	return ret, err
}

type DistributedOperationResult map[string]error

func (dr DistributedOperationResult) Error() error {
	var errs []string
	for k, v := range dr {
		if v != nil {
			errs = append(errs, fmt.Sprintf("[%s]: %v", k, v))
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.New(strings.Join(errs, "\n"))
}

type RemoteResult struct {
	Host  string
	Error error
}

func distributedOperation(masterNode string, store *storage.Store, volumeId needle.VolumeId, op func(location operation.Location) error) error {
	if lookupResult, lookupErr := operation.Lookup(masterNode, volumeId.String()); lookupErr == nil {
		length := 0
		selfUrl := (store.Ip + ":" + strconv.Itoa(store.Port))
		results := make(chan RemoteResult)
		for _, location := range lookupResult.Locations {
			if location.Url != selfUrl {
				length++
				go func(location operation.Location, results chan RemoteResult) {
					results <- RemoteResult{location.Url, op(location)}
				}(location, results)
			}
		}
		ret := DistributedOperationResult(make(map[string]error))
		for i := 0; i < length; i++ {
			result := <-results
			ret[result.Host] = result.Error
		}
		if volume := store.GetVolume(volumeId); volume != nil {
			if length+1 < volume.ReplicaPlacement.GetCopyCount() {
				return fmt.Errorf("replicating opetations [%d] is less than volume's replication copy count [%d]", length+1, volume.ReplicaPlacement.GetCopyCount())
			}
		}
		return ret.Error()
	} else {
		glog.V(0).Infoln()
		return fmt.Errorf("Failed to lookup for %d: %v", volumeId, lookupErr)
	}
}
