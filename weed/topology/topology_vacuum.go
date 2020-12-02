package topology

import (
	"context"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/storage/needle"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
)

func batchVacuumVolumeCheck(grpcDialOption grpc.DialOption, vl *VolumeLayout, vid needle.VolumeId,
	locationlist *VolumeLocationList, garbageThreshold float64) (*VolumeLocationList, bool) {
	ch := make(chan int, locationlist.Length())
	errCount := int32(0)
	for index, dn := range locationlist.list {
		go func(index int, url string, vid needle.VolumeId) {
			err := operation.WithVolumeServerClient(url, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
				resp, err := volumeServerClient.VacuumVolumeCheck(context.Background(), &volume_server_pb.VacuumVolumeCheckRequest{
					VolumeId: uint32(vid),
				})
				if err != nil {
					atomic.AddInt32(&errCount, 1)
					ch <- -1
					return err
				}
				if resp.GarbageRatio >= garbageThreshold {
					ch <- index
				} else {
					ch <- -1
				}
				return nil
			})
			if err != nil {
				glog.V(0).Infof("Checking vacuuming %d on %s: %v", vid, url, err)
			}
		}(index, dn.Url(), vid)
	}
	vacuumLocationList := NewVolumeLocationList()

	waitTimeout := time.NewTimer(30 * time.Minute)
	defer waitTimeout.Stop()

	for range locationlist.list {
		select {
		case index := <-ch:
			if index != -1 {
				vacuumLocationList.list = append(vacuumLocationList.list, locationlist.list[index])
			}
		case <-waitTimeout.C:
			return vacuumLocationList, false
		}
	}
	return vacuumLocationList, errCount == 0 && len(vacuumLocationList.list) > 0
}
func batchVacuumVolumeCompact(grpcDialOption grpc.DialOption, vl *VolumeLayout, vid needle.VolumeId,
	locationlist *VolumeLocationList, preallocate int64) bool {
	vl.accessLock.Lock()
	vl.removeFromWritable(vid)
	vl.accessLock.Unlock()

	ch := make(chan bool, locationlist.Length())
	for index, dn := range locationlist.list {
		go func(index int, url string, vid needle.VolumeId) {
			glog.V(0).Infoln(index, "Start vacuuming", vid, "on", url)
			err := operation.WithVolumeServerClient(url, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
				_, err := volumeServerClient.VacuumVolumeCompact(context.Background(), &volume_server_pb.VacuumVolumeCompactRequest{
					VolumeId:    uint32(vid),
					Preallocate: preallocate,
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

	waitTimeout := time.NewTimer(30 * time.Minute)
	defer waitTimeout.Stop()

	for range locationlist.list {
		select {
		case canCommit := <-ch:
			isVacuumSuccess = isVacuumSuccess && canCommit
		case <-waitTimeout.C:
			return false
		}
	}
	return isVacuumSuccess
}
func batchVacuumVolumeCommit(grpcDialOption grpc.DialOption, vl *VolumeLayout, vid needle.VolumeId, locationlist *VolumeLocationList) bool {
	isCommitSuccess := true
	isReadOnly := false
	for _, dn := range locationlist.list {
		glog.V(0).Infoln("Start Committing vacuum", vid, "on", dn.Url())
		err := operation.WithVolumeServerClient(dn.Url(), grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
			resp, err := volumeServerClient.VacuumVolumeCommit(context.Background(), &volume_server_pb.VacuumVolumeCommitRequest{
				VolumeId: uint32(vid),
			})
			if resp != nil && resp.IsReadOnly {
				isReadOnly = true
			}
			return err
		})
		if err != nil {
			glog.Errorf("Error when committing vacuum %d on %s: %v", vid, dn.Url(), err)
			isCommitSuccess = false
		} else {
			glog.V(0).Infof("Complete Committing vacuum %d on %s", vid, dn.Url())
		}
	}
	if isCommitSuccess {
		for _, dn := range locationlist.list {
			vl.SetVolumeAvailable(dn, vid, isReadOnly)
		}
	}
	return isCommitSuccess
}
func batchVacuumVolumeCleanup(grpcDialOption grpc.DialOption, vl *VolumeLayout, vid needle.VolumeId, locationlist *VolumeLocationList) {
	for _, dn := range locationlist.list {
		glog.V(0).Infoln("Start cleaning up", vid, "on", dn.Url())
		err := operation.WithVolumeServerClient(dn.Url(), grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
			_, err := volumeServerClient.VacuumVolumeCleanup(context.Background(), &volume_server_pb.VacuumVolumeCleanupRequest{
				VolumeId: uint32(vid),
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

func (t *Topology) Vacuum(grpcDialOption grpc.DialOption, garbageThreshold float64, preallocate int64) {

	// if there is vacuum going on, return immediately
	swapped := atomic.CompareAndSwapInt64(&t.vacuumLockCounter, 0, 1)
	if !swapped {
		return
	}
	defer atomic.StoreInt64(&t.vacuumLockCounter, 0)

	// now only one vacuum process going on

	glog.V(1).Infof("Start vacuum on demand with threshold: %f", garbageThreshold)
	for _, col := range t.collectionMap.Items() {
		c := col.(*Collection)
		for _, vl := range c.storageType2VolumeLayout.Items() {
			if vl != nil {
				volumeLayout := vl.(*VolumeLayout)
				vacuumOneVolumeLayout(grpcDialOption, volumeLayout, c, garbageThreshold, preallocate)
			}
		}
	}
}

func vacuumOneVolumeLayout(grpcDialOption grpc.DialOption, volumeLayout *VolumeLayout, c *Collection, garbageThreshold float64, preallocate int64) {

	volumeLayout.accessLock.RLock()
	tmpMap := make(map[needle.VolumeId]*VolumeLocationList)
	for vid, locationList := range volumeLayout.vid2location {
		tmpMap[vid] = locationList.Copy()
	}
	volumeLayout.accessLock.RUnlock()

	for vid, locationList := range tmpMap {

		volumeLayout.accessLock.RLock()
		isReadOnly := volumeLayout.readonlyVolumes.IsTrue(vid)
		volumeLayout.accessLock.RUnlock()

		if isReadOnly {
			continue
		}

		glog.V(2).Infof("check vacuum on collection:%s volume:%d", c.Name, vid)
		if vacuumLocationList, needVacuum := batchVacuumVolumeCheck(
			grpcDialOption, volumeLayout, vid, locationList, garbageThreshold); needVacuum {
			if batchVacuumVolumeCompact(grpcDialOption, volumeLayout, vid, vacuumLocationList, preallocate) {
				batchVacuumVolumeCommit(grpcDialOption, volumeLayout, vid, vacuumLocationList)
			} else {
				batchVacuumVolumeCleanup(grpcDialOption, volumeLayout, vid, vacuumLocationList)
			}
		}
	}
}
