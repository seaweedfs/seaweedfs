package topology

import (
	"context"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

func batchVacuumVolumeCheck(vl *VolumeLayout, vid storage.VolumeId, locationlist *VolumeLocationList, garbageThreshold float64) bool {
	ch := make(chan bool, locationlist.Length())
	for index, dn := range locationlist.list {
		go func(index int, url string, vid storage.VolumeId) {
			err := operation.WithVolumeServerClient(url, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
				resp, err := volumeServerClient.VacuumVolumeCheck(context.Background(), &volume_server_pb.VacuumVolumeCheckRequest{
					VolumdId: uint32(vid),
				})
				if err != nil {
					ch <- false
					return err
				}
				isNeeded := resp.GarbageRatio > garbageThreshold
				ch <- isNeeded
				return nil
			})
			if err != nil {
				glog.V(0).Infof("Checking vacuuming %d on %s: %v", vid, url, err)
			}
		}(index, dn.Url(), vid)
	}
	isCheckSuccess := true
	for _ = range locationlist.list {
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
func batchVacuumVolumeCompact(vl *VolumeLayout, vid storage.VolumeId, locationlist *VolumeLocationList, preallocate int64) bool {
	vl.removeFromWritable(vid)
	ch := make(chan bool, locationlist.Length())
	for index, dn := range locationlist.list {
		go func(index int, url string, vid storage.VolumeId) {
			glog.V(0).Infoln(index, "Start vacuuming", vid, "on", url)
			err := operation.WithVolumeServerClient(url, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
				_, err := volumeServerClient.VacuumVolumeCompact(context.Background(), &volume_server_pb.VacuumVolumeCompactRequest{
					VolumdId: uint32(vid),
				})
				return err
			})
			if err != nil {
				glog.Errorf("Error when vacuuming %d on %s: %v", vid, url, err)
				ch <- false
			} else {
				glog.V(0).Infof("Complete vacuuming %d on %s", vid, url)
				ch <- true
			}
		}(index, dn.Url(), vid)
	}
	isVacuumSuccess := true
	for _ = range locationlist.list {
		select {
		case canCommit := <-ch:
			isVacuumSuccess = isVacuumSuccess && canCommit
		case <-time.After(30 * time.Minute):
			isVacuumSuccess = false
			break
		}
	}
	return isVacuumSuccess
}
func batchVacuumVolumeCommit(vl *VolumeLayout, vid storage.VolumeId, locationlist *VolumeLocationList) bool {
	isCommitSuccess := true
	for _, dn := range locationlist.list {
		glog.V(0).Infoln("Start Commiting vacuum", vid, "on", dn.Url())
		err := operation.WithVolumeServerClient(dn.Url(), func(volumeServerClient volume_server_pb.VolumeServerClient) error {
			_, err := volumeServerClient.VacuumVolumeCommit(context.Background(), &volume_server_pb.VacuumVolumeCommitRequest{
				VolumdId: uint32(vid),
			})
			return err
		})
		if err != nil {
			glog.Errorf("Error when committing vacuum %d on %s: %v", vid, dn.Url(), err)
			isCommitSuccess = false
		} else {
			glog.V(0).Infof("Complete Commiting vacuum %d on %s", vid, dn.Url())
		}
		if isCommitSuccess {
			vl.SetVolumeAvailable(dn, vid)
		}
	}
	return isCommitSuccess
}
func batchVacuumVolumeCleanup(vl *VolumeLayout, vid storage.VolumeId, locationlist *VolumeLocationList) {
	for _, dn := range locationlist.list {
		glog.V(0).Infoln("Start cleaning up", vid, "on", dn.Url())
		err := operation.WithVolumeServerClient(dn.Url(), func(volumeServerClient volume_server_pb.VolumeServerClient) error {
			_, err := volumeServerClient.VacuumVolumeCleanup(context.Background(), &volume_server_pb.VacuumVolumeCleanupRequest{
				VolumdId: uint32(vid),
			})
			return err
		})
		if err != nil {
			glog.Errorf("Error when cleaning up vacuum %d on %s: %v", vid, dn.Url(), err)
		} else {
			glog.V(0).Infof("Complete cleaning up vacuum %d on %s", vid, dn.Url())
		}
	}
}

func (t *Topology) Vacuum(garbageThreshold float64, preallocate int64) int {
	glog.V(0).Infof("Start vacuum on demand with threshold: %f", garbageThreshold)
	for _, col := range t.collectionMap.Items() {
		c := col.(*Collection)
		for _, vl := range c.storageType2VolumeLayout.Items() {
			if vl != nil {
				volumeLayout := vl.(*VolumeLayout)
				vacuumOneVolumeLayout(volumeLayout, c, garbageThreshold, preallocate)
			}
		}
	}
	return 0
}

func vacuumOneVolumeLayout(volumeLayout *VolumeLayout, c *Collection, garbageThreshold float64, preallocate int64) {

	volumeLayout.accessLock.RLock()
	tmpMap := make(map[storage.VolumeId]*VolumeLocationList)
	for vid, locationlist := range volumeLayout.vid2location {
		tmpMap[vid] = locationlist
	}
	volumeLayout.accessLock.RUnlock()

	for vid, locationlist := range tmpMap {

		volumeLayout.accessLock.RLock()
		isReadOnly, hasValue := volumeLayout.readonlyVolumes[vid]
		volumeLayout.accessLock.RUnlock()

		if hasValue && isReadOnly {
			continue
		}

		glog.V(0).Infof("check vacuum on collection:%s volume:%d", c.Name, vid)
		if batchVacuumVolumeCheck(volumeLayout, vid, locationlist, garbageThreshold) {
			if batchVacuumVolumeCompact(volumeLayout, vid, locationlist, preallocate) {
				batchVacuumVolumeCommit(volumeLayout, vid, locationlist)
			} else {
				batchVacuumVolumeCleanup(volumeLayout, vid, locationlist)
			}
		}
	}
}
