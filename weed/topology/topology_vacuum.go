package topology

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/seaweedfs/seaweedfs/weed/pb"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
)

func (t *Topology) batchVacuumVolumeCheck(grpcDialOption grpc.DialOption, vid needle.VolumeId,
	locationlist *VolumeLocationList, garbageThreshold float64) (*VolumeLocationList, bool) {
	ch := make(chan int, locationlist.Length())
	errCount := int32(0)
	for index, dn := range locationlist.list {
		go func(index int, url pb.ServerAddress, vid needle.VolumeId) {
			err := operation.WithVolumeServerClient(false, url, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
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
		}(index, dn.ServerAddress(), vid)
	}
	vacuumLocationList := NewVolumeLocationList()

	waitTimeout := time.NewTimer(time.Minute * time.Duration(t.volumeSizeLimit/1024/1024/1000+1))
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

func (t *Topology) batchVacuumVolumeCompact(grpcDialOption grpc.DialOption, vl *VolumeLayout, vid needle.VolumeId,
	locationlist *VolumeLocationList, preallocate int64) bool {
	vl.accessLock.Lock()
	vl.removeFromWritable(vid)
	vl.accessLock.Unlock()

	ch := make(chan bool, locationlist.Length())
	for index, dn := range locationlist.list {
		go func(index int, url pb.ServerAddress, vid needle.VolumeId) {
			glog.V(0).Infoln(index, "Start vacuuming", vid, "on", url)
			err := operation.WithVolumeServerClient(true, url, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
				stream, err := volumeServerClient.VacuumVolumeCompact(context.Background(), &volume_server_pb.VacuumVolumeCompactRequest{
					VolumeId:    uint32(vid),
					Preallocate: preallocate,
				})
				if err != nil {
					return err
				}

				for {
					resp, recvErr := stream.Recv()
					if recvErr != nil {
						if recvErr == io.EOF {
							break
						} else {
							return recvErr
						}
					}
					glog.V(0).Infof("%d vacuum %d on %s processed %d bytes, loadAvg %.02f%%",
						index, vid, url, resp.ProcessedBytes, resp.LoadAvg_1M*100)
				}
				return nil
			})
			if err != nil {
				glog.Errorf("Error when vacuuming %d on %s: %v", vid, url, err)
				ch <- false
			} else {
				glog.V(0).Infof("Complete vacuuming %d on %s", vid, url)
				ch <- true
			}
		}(index, dn.ServerAddress(), vid)
	}
	isVacuumSuccess := true

	waitTimeout := time.NewTimer(3 * time.Minute * time.Duration(t.volumeSizeLimit/1024/1024/1000+1))
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

func (t *Topology) batchVacuumVolumeCommit(grpcDialOption grpc.DialOption, vl *VolumeLayout, vid needle.VolumeId, vacuumLocationList, locationList *VolumeLocationList) bool {
	isCommitSuccess := true
	isReadOnly := false
	isFullCapacity := false
	for _, dn := range vacuumLocationList.list {
		glog.V(0).Infoln("Start Committing vacuum", vid, "on", dn.Url())
		err := operation.WithVolumeServerClient(false, dn.ServerAddress(), grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
			resp, err := volumeServerClient.VacuumVolumeCommit(context.Background(), &volume_server_pb.VacuumVolumeCommitRequest{
				VolumeId: uint32(vid),
			})
			if resp != nil {
				if resp.IsReadOnly {
					isReadOnly = true
				}
				if resp.VolumeSize > t.volumeSizeLimit {
					isFullCapacity = true
				}
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

	//we should check the status of all replicas
	if len(locationList.list) > len(vacuumLocationList.list) {
		for _, dn := range locationList.list {
			isFound := false
			for _, dnVaccum := range vacuumLocationList.list {
				if dn.id == dnVaccum.id {
					isFound = true
					break
				}
			}
			if !isFound {
				err := operation.WithVolumeServerClient(false, dn.ServerAddress(), grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
					resp, err := volumeServerClient.VolumeStatus(context.Background(), &volume_server_pb.VolumeStatusRequest{
						VolumeId: uint32(vid),
					})
					if resp != nil {
						if resp.IsReadOnly {
							isReadOnly = true
						}
						if resp.VolumeSize > t.volumeSizeLimit {
							isFullCapacity = true
						}
					}
					return err
				})
				if err != nil {
					glog.Errorf("Error when checking volume %d status on %s: %v", vid, dn.Url(), err)
					//we mark volume read-only, since the volume state is unknown
					isReadOnly = true
				}
			}
		}
	}

	if isCommitSuccess {

		//record vacuum time of volume
		vl.accessLock.Lock()
		vl.vacuumedVolumes[vid] = time.Now()
		vl.accessLock.Unlock()

		for _, dn := range vacuumLocationList.list {
			vl.SetVolumeAvailable(dn, vid, isReadOnly, isFullCapacity)
		}
	}
	return isCommitSuccess
}

func (t *Topology) batchVacuumVolumeCleanup(grpcDialOption grpc.DialOption, vl *VolumeLayout, vid needle.VolumeId, locationlist *VolumeLocationList) {
	for _, dn := range locationlist.list {
		glog.V(0).Infoln("Start cleaning up", vid, "on", dn.Url())
		err := operation.WithVolumeServerClient(false, dn.ServerAddress(), grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
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

func (t *Topology) Vacuum(grpcDialOption grpc.DialOption, garbageThreshold float64, maxParallelVacuumPerServer int, volumeId uint32, collection string, preallocate int64, automatic bool) {

	// if there is vacuum going on, return immediately
	swapped := atomic.CompareAndSwapInt64(&t.vacuumLockCounter, 0, 1)
	if !swapped {
		glog.V(0).Infof("Vacuum is already running")
		return
	}
	defer atomic.StoreInt64(&t.vacuumLockCounter, 0)

	// now only one vacuum process going on

	glog.V(1).Infof("Start vacuum on demand with threshold: %f collection: %s volumeId: %d",
		garbageThreshold, collection, volumeId)
	for _, col := range t.collectionMap.Items() {
		c := col.(*Collection)
		if collection != "" && collection != c.Name {
			continue
		}
		for _, vl := range c.storageType2VolumeLayout.Items() {
			if vl != nil {
				volumeLayout := vl.(*VolumeLayout)
				if volumeId > 0 {
					vid := needle.VolumeId(volumeId)
					volumeLayout.accessLock.RLock()
					locationList, ok := volumeLayout.vid2location[vid]
					volumeLayout.accessLock.RUnlock()
					if ok {
						t.vacuumOneVolumeId(grpcDialOption, volumeLayout, c, garbageThreshold, locationList, vid, preallocate)
					}
				} else {
					t.vacuumOneVolumeLayout(grpcDialOption, volumeLayout, c, garbageThreshold, maxParallelVacuumPerServer, preallocate, automatic)
				}
			}
			if automatic && t.isDisableVacuum {
				break
			}
		}
		if automatic && t.isDisableVacuum {
			glog.V(0).Infof("Vacuum is disabled")
			break
		}
	}
}

func (t *Topology) vacuumOneVolumeLayout(grpcDialOption grpc.DialOption, volumeLayout *VolumeLayout, c *Collection, garbageThreshold float64, maxParallelVacuumPerServer int, preallocate int64, automatic bool) {

	volumeLayout.accessLock.RLock()
	todoVolumeMap := make(map[needle.VolumeId]*VolumeLocationList)
	for vid, locationList := range volumeLayout.vid2location {
		todoVolumeMap[vid] = locationList.Copy()
	}
	volumeLayout.accessLock.RUnlock()

	// limiter for each volume server
	limiter := make(map[NodeId]int)
	var limiterLock sync.Mutex
	for _, locationList := range todoVolumeMap {
		for _, dn := range locationList.list {
			if _, ok := limiter[dn.Id()]; !ok {
				limiter[dn.Id()] = maxParallelVacuumPerServer
			}
		}
	}

	executor := util.NewLimitedConcurrentExecutor(100)

	var wg sync.WaitGroup

	for len(todoVolumeMap) > 0 {
		pendingVolumeMap := make(map[needle.VolumeId]*VolumeLocationList)
		for vid, locationList := range todoVolumeMap {
			hasEnoughQuota := true
			for _, dn := range locationList.list {
				limiterLock.Lock()
				quota := limiter[dn.Id()]
				limiterLock.Unlock()
				if quota <= 0 {
					hasEnoughQuota = false
					break
				}
			}
			if !hasEnoughQuota {
				pendingVolumeMap[vid] = locationList
				continue
			}

			// debit the quota
			for _, dn := range locationList.list {
				limiterLock.Lock()
				limiter[dn.Id()]--
				limiterLock.Unlock()
			}

			wg.Add(1)
			executor.Execute(func() {
				defer wg.Done()
				t.vacuumOneVolumeId(grpcDialOption, volumeLayout, c, garbageThreshold, locationList, vid, preallocate)
				// credit the quota
				for _, dn := range locationList.list {
					limiterLock.Lock()
					limiter[dn.Id()]++
					limiterLock.Unlock()
				}
			})
			if automatic && t.isDisableVacuum {
				break
			}
		}
		if automatic && t.isDisableVacuum {
			break
		}
		if len(todoVolumeMap) == len(pendingVolumeMap) {
			time.Sleep(10 * time.Second)
		}
		todoVolumeMap = pendingVolumeMap
	}

	wg.Wait()

}

func (t *Topology) vacuumOneVolumeId(grpcDialOption grpc.DialOption, volumeLayout *VolumeLayout, c *Collection, garbageThreshold float64, locationList *VolumeLocationList, vid needle.VolumeId, preallocate int64) {
	volumeLayout.accessLock.RLock()
	isReadOnly := volumeLayout.readonlyVolumes.IsTrue(vid)
	isEnoughCopies := volumeLayout.enoughCopies(vid)
	volumeLayout.accessLock.RUnlock()

	if isReadOnly {
		return
	}
	if !isEnoughCopies {
		glog.Warningf("skip vacuuming: not enough copies for volume:%d", vid)
		return
	}

	glog.V(1).Infof("check vacuum on collection:%s volume:%d", c.Name, vid)
	if vacuumLocationList, needVacuum := t.batchVacuumVolumeCheck(
		grpcDialOption, vid, locationList, garbageThreshold); needVacuum {
		if t.batchVacuumVolumeCompact(grpcDialOption, volumeLayout, vid, vacuumLocationList, preallocate) {
			t.batchVacuumVolumeCommit(grpcDialOption, volumeLayout, vid, vacuumLocationList, locationList)
		} else {
			t.batchVacuumVolumeCleanup(grpcDialOption, volumeLayout, vid, vacuumLocationList)
		}
	}
}
