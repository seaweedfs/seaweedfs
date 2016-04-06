package topology

import (
	"encoding/json"
	"errors"
	"net/url"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func batchVacuumVolumeCheck(vl *VolumeLayout, vid storage.VolumeId, locationlist *VolumeLocationList, garbageThreshold string) bool {
	ch := make(chan bool, locationlist.Length())
	for index, dn := range locationlist.AllDataNode() {
		go func(index int, url string, vid storage.VolumeId) {
			//glog.V(0).Infoln(index, "Check vacuuming", vid, "on", dn.Url())
			if e, ret := vacuumVolume_Check(url, vid, garbageThreshold); e != nil {
				//glog.V(0).Infoln(index, "Error when checking vacuuming", vid, "on", url, e)
				ch <- false
			} else {
				//glog.V(0).Infoln(index, "Checked vacuuming", vid, "on", url, "needVacuum", ret)
				ch <- ret
			}
		}(index, dn.Url(), vid)
	}
	isCheckSuccess := true
	for range locationlist.AllDataNode() {
		select {
		case canVacuum := <-ch:
			isCheckSuccess = isCheckSuccess && canVacuum
		case <-time.After(30 * time.Minute):
			isCheckSuccess = false
			break
		}
	}
	return isCheckSuccess
}
func batchVacuumVolumeCompact(vl *VolumeLayout, vid storage.VolumeId, locationlist *VolumeLocationList) bool {
	vl.RemoveFromWritable(vid)
	ch := make(chan bool, locationlist.Length())
	for index, dn := range locationlist.AllDataNode() {
		go func(index int, url string, vid storage.VolumeId) {
			glog.V(0).Infoln(index, "Start vacuuming", vid, "on", url)
			if e := vacuumVolume_Compact(url, vid); e != nil {
				glog.V(0).Infoln(index, "Error when vacuuming", vid, "on", url, e)
				ch <- false
			} else {
				glog.V(0).Infoln(index, "Complete vacuuming", vid, "on", url)
				ch <- true
			}
		}(index, dn.Url(), vid)
	}
	isVacuumSuccess := true
	for range locationlist.AllDataNode() {
		select {
		case _ = <-ch:
		case <-time.After(30 * time.Minute):
			isVacuumSuccess = false
			break
		}
	}
	return isVacuumSuccess
}
func batchVacuumVolumeCommit(vl *VolumeLayout, vid storage.VolumeId, locationlist *VolumeLocationList) bool {
	isCommitSuccess := true
	for _, dn := range locationlist.AllDataNode() {
		glog.V(0).Infoln("Start Commiting vacuum", vid, "on", dn.Url())
		if e := vacuumVolume_Commit(dn.Url(), vid); e != nil {
			glog.V(0).Infoln("Error when committing vacuum", vid, "on", dn.Url(), e)
			isCommitSuccess = false
		} else {
			glog.V(0).Infoln("Complete Commiting vacuum", vid, "on", dn.Url())
		}
		if isCommitSuccess {
			vl.SetVolumeAvailable(dn, vid)
		}
	}
	return isCommitSuccess
}
func (t *Topology) Vacuum(garbageThreshold string) int {
	glog.V(0).Infoln("Start vacuum on demand")
	for item := range t.collectionMap.IterItems() {
		c := item.Value.(*Collection)
		gcThreshold := garbageThreshold
		if gcThreshold == "" {
			gcThreshold = t.CollectionSettings.GetGarbageThreshold(c.Name)
		}
		glog.V(0).Infoln("vacuum on collection:", c.Name)
		for item1 := range c.storageType2VolumeLayout.IterItems() {
			if item1.Value == nil {
				continue
			}
			volumeLayout := item1.Value.(*VolumeLayout)
			for _, vid := range volumeLayout.ListVolumeId() {
				locationList := volumeLayout.Lookup(vid)
				if locationList == nil {
					continue
				}
				glog.V(0).Infoln("vacuum on collection:", c.Name, "volume", vid)
				if batchVacuumVolumeCheck(volumeLayout, vid, locationList, gcThreshold) {
					if batchVacuumVolumeCompact(volumeLayout, vid, locationList) {
						batchVacuumVolumeCommit(volumeLayout, vid, locationList)
					}
				}
			}
		}
	}
	return 0
}

type VacuumVolumeResult struct {
	Result bool
	Error  string
}

func vacuumVolume_Check(urlLocation string, vid storage.VolumeId, garbageThreshold string) (error, bool) {
	values := make(url.Values)
	values.Add("volume", vid.String())
	values.Add("garbageThreshold", garbageThreshold)
	jsonBlob, err := util.Post(urlLocation, "/admin/vacuum/check", values)
	if err != nil {
		glog.V(0).Infoln("parameters:", values)
		return err, false
	}
	var ret VacuumVolumeResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return err, false
	}
	if ret.Error != "" {
		return errors.New(ret.Error), false
	}
	return nil, ret.Result
}
func vacuumVolume_Compact(urlLocation string, vid storage.VolumeId) error {
	values := make(url.Values)
	values.Add("volume", vid.String())
	jsonBlob, err := util.Post(urlLocation, "/admin/vacuum/compact", values)
	if err != nil {
		return err
	}
	var ret VacuumVolumeResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return err
	}
	if ret.Error != "" {
		return errors.New(ret.Error)
	}
	return nil
}
func vacuumVolume_Commit(urlLocation string, vid storage.VolumeId) error {
	values := make(url.Values)
	values.Add("volume", vid.String())
	jsonBlob, err := util.Post(urlLocation, "/admin/vacuum/commit", values)
	if err != nil {
		return err
	}
	var ret VacuumVolumeResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return err
	}
	if ret.Error != "" {
		return errors.New(ret.Error)
	}
	return nil
}
