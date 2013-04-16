package replication

import (
	"bytes"
	"code.google.com/p/weed-fs/go/operation"
  "code.google.com/p/weed-fs/go/storage"
	"log"
	"net/http"
	"strconv"
)

func ReplicatedWrite(masterNode string, s *storage.Store, volumeId storage.VolumeId, needle *storage.Needle, r *http.Request) (size uint32, errorStatus string) {
	ret, err := s.Write(volumeId, needle)
	needToReplicate := !s.HasVolume(volumeId)
	if err != nil {
		errorStatus = "Failed to write to local disk (" + err.Error() + ")"
	} else if ret > 0 {
		needToReplicate = needToReplicate || s.GetVolume(volumeId).NeedToReplicate()
	} else {
		errorStatus = "Failed to write to local disk"
	}
	if !needToReplicate && ret > 0 {
		needToReplicate = s.GetVolume(volumeId).NeedToReplicate()
	}
	if needToReplicate { //send to other replica locations
		if r.FormValue("type") != "standard" {
			if !distributedOperation(masterNode, s, volumeId, func(location operation.Location) bool {
				_, err := operation.Upload("http://"+location.Url+r.URL.Path+"?type=standard", string(needle.Name), bytes.NewReader(needle.Data))
				return err == nil
			}) {
				ret = 0
				errorStatus = "Failed to write to replicas for volume " + volumeId.String()
			}
		}
	}
	if errorStatus != "" {
		if _, err = s.Delete(volumeId, needle); err != nil {
			errorStatus += "\nCannot delete " + strconv.FormatUint(needle.Id, 10) + " from " +
				strconv.FormatUint(uint64(volumeId), 10) + ": " + err.Error()
		} else {
			distributedOperation(masterNode, s, volumeId, func(location operation.Location) bool {
				return nil == operation.Delete("http://"+location.Url+r.URL.Path+"?type=standard")
			})
		}
	}
	size = ret
	return
}

func ReplicatedDelete(masterNode string, store *storage.Store, volumeId storage.VolumeId, n *storage.Needle, r *http.Request) (ret uint32) {
	ret, err := store.Delete(volumeId, n)
	if err != nil {
		log.Println("delete error:", err)
		return
	}

	needToReplicate := !store.HasVolume(volumeId)
	if !needToReplicate && ret > 0 {
		needToReplicate = store.GetVolume(volumeId).NeedToReplicate()
	}
	if needToReplicate { //send to other replica locations
		if r.FormValue("type") != "standard" {
			if !distributedOperation(masterNode, store, volumeId, func(location operation.Location) bool {
				return nil == operation.Delete("http://"+location.Url+r.URL.Path+"?type=standard")
			}) {
				ret = 0
			}
		}
	}
	return
}

func distributedOperation(masterNode string, store *storage.Store, volumeId storage.VolumeId, op func(location operation.Location) bool) bool {
	if lookupResult, lookupErr := operation.Lookup(masterNode, volumeId); lookupErr == nil {
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
		return ret
	} else {
		log.Println("Failed to lookup for", volumeId, lookupErr.Error())
	}
	return false
}
