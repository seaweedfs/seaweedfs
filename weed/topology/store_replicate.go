package topology

import (
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
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func ReplicatedWrite(masterFn operation.GetMasterFn, s *storage.Store, volumeId needle.VolumeId, n *needle.Needle, r *http.Request) (isUnchanged bool, err error) {

	//check JWT
	jwt := security.GetJwt(r)

	// check whether this is a replicated write request
	var remoteLocations []operation.Location
	if r.FormValue("type") != "replicate" {
		// this is the initial request
		remoteLocations, err = getWritableRemoteReplications(s, volumeId, masterFn)
		if err != nil {
			glog.V(0).Infoln(err)
			return
		}
	}

	// read fsync value
	fsync := false
	if r.FormValue("fsync") == "true" {
		fsync = true
	}

	if s.GetVolume(volumeId) != nil {
		isUnchanged, err = s.WriteVolumeNeedle(volumeId, n, fsync)
		if err != nil {
			err = fmt.Errorf("failed to write to local disk: %v", err)
			glog.V(0).Infoln(err)
			return
		}
	}

	if len(remoteLocations) > 0 { //send to other replica locations
		if err = distributedOperation(remoteLocations, s, func(location operation.Location) error {
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

			// volume server do not know about encryption
			// TODO optimize here to compress data only once
			_, err := operation.UploadData(u.String(), string(n.Name), false, n.Data, n.IsCompressed(), string(n.Mime), pairMap, jwt)
			return err
		}); err != nil {
			err = fmt.Errorf("failed to write to replicas for volume %d: %v", volumeId, err)
			glog.V(0).Infoln(err)
		}
	}
	return
}

func ReplicatedDelete(masterFn operation.GetMasterFn, store *storage.Store,
	volumeId needle.VolumeId, n *needle.Needle,
	r *http.Request) (size types.Size, err error) {

	//check JWT
	jwt := security.GetJwt(r)

	var remoteLocations []operation.Location
	if r.FormValue("type") != "replicate" {
		remoteLocations, err = getWritableRemoteReplications(store, volumeId, masterFn)
		if err != nil {
			glog.V(0).Infoln(err)
			return
		}
	}

	size, err = store.DeleteVolumeNeedle(volumeId, n)
	if err != nil {
		glog.V(0).Infoln("delete error:", err)
		return
	}

	if len(remoteLocations) > 0 { //send to other replica locations
		if err = distributedOperation(remoteLocations, store, func(location operation.Location) error {
			return util.Delete("http://"+location.Url+r.URL.Path+"?type=replicate", string(jwt))
		}); err != nil {
			size = 0
		}
	}
	return
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

func distributedOperation(locations []operation.Location, store *storage.Store, op func(location operation.Location) error) error {
	length := len(locations)
	results := make(chan RemoteResult)
	for _, location := range locations {
		go func(location operation.Location, results chan RemoteResult) {
			results <- RemoteResult{location.Url, op(location)}
		}(location, results)
	}
	ret := DistributedOperationResult(make(map[string]error))
	for i := 0; i < length; i++ {
		result := <-results
		ret[result.Host] = result.Error
	}

	return ret.Error()
}

func getWritableRemoteReplications(s *storage.Store, volumeId needle.VolumeId, masterFn operation.GetMasterFn) (
	remoteLocations []operation.Location, err error) {

	v := s.GetVolume(volumeId)
	if v != nil && v.ReplicaPlacement.GetCopyCount() == 1 {
		return
	}

	// not on local store, or has replications
	lookupResult, lookupErr := operation.Lookup(masterFn, volumeId.String())
	if lookupErr == nil {
		selfUrl := s.Ip + ":" + strconv.Itoa(s.Port)
		for _, location := range lookupResult.Locations {
			if location.Url != selfUrl {
				remoteLocations = append(remoteLocations, location)
			}
		}
	} else {
		err = fmt.Errorf("failed to lookup for %d: %v", volumeId, lookupErr)
		return
	}

	if v != nil {
		// has one local and has remote replications
		copyCount := v.ReplicaPlacement.GetCopyCount()
		if len(lookupResult.Locations) < copyCount {
			err = fmt.Errorf("replicating opetations [%d] is less than volume %d replication copy count [%d]",
				len(lookupResult.Locations), volumeId, copyCount)
		}
	}

	return
}
