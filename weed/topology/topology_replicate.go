package topology

import (
	"container/list"
	"fmt"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

var (
	isReplicateCheckerRunning = false
)

const ReplicateTaskTimeout = time.Hour

type ReplicateTask struct {
	Vid        storage.VolumeId
	Collection string
	SrcDN      *DataNode
	DstDN      *DataNode
}

func (t *ReplicateTask) Run(topo *Topology) error {
	//is lookup thread safe?
	locationList := topo.Lookup(t.Collection, t.Vid)
	rp := topo.CollectionSettings.GetReplicaPlacement(t.Collection)
	if locationList.CalcReplicaPlacement().Compare(rp) >= 0 {
		glog.V(0).Infof("volume [%v] has right replica placement, rp: %s", t.Vid, rp.String())
		return nil
	}
	if !SetVolumeReadonly(locationList, t.Vid.String(), true) {
		return fmt.Errorf("set volume readonly failed, vid=%v", t.Vid)
	}
	defer SetVolumeReadonly(locationList, t.Vid.String(), false)
	tc, e := storage.NewTaskCli(t.DstDN.Url(), storage.TaskReplicate, storage.TaskParams{
		"volume":     t.Vid.String(),
		"source":     t.SrcDN.Url(),
		"collection": t.Collection,
	})
	if e != nil {
		return e
	}
	if e = tc.WaitAndQueryResult(ReplicateTaskTimeout); e != nil {
		tc.Clean()
		return e
	}
	e = tc.Commit()
	return e
}

func (t *ReplicateTask) WorkingDataNodes() []*DataNode {
	return []*DataNode{
		t.SrcDN,
		t.DstDN,
	}
}

func (t *ReplicateTask) String() string {
	return fmt.Sprintf("<Replicate> vid: %v, src: %s, dst: %s", t.Vid, t.SrcDN.Url(), t.DstDN.Url())
}

func planReplicateTasks(t *Topology) (tasks []*ReplicateTask) {
	for i1 := range t.collectionMap.IterItems() {
		c := i1.Value.(*Collection)
		glog.V(0).Infoln("checking replicate on collection:", c.Name)
		growOption := &VolumeGrowOption{ReplicaPlacement: c.rp}
		for i2 := range c.storageType2VolumeLayout.IterItems() {
			if i2.Value == nil {
				continue
			}
			volumeLayout := i2.Value.(*VolumeLayout)
			for _, vid := range volumeLayout.ListVolumeId() {
				locationList := volumeLayout.Lookup(vid)
				if locationList == nil {
					continue
				}
				rp1 := locationList.CalcReplicaPlacement()
				if rp1.Compare(volumeLayout.rp) >= 0 {
					continue
				}
				if additionServers, e := FindEmptySlotsForOneVolume(t, growOption, locationList); e == nil {
					for _, s := range additionServers {
						s.UpAdjustPlannedVolumeCountDelta(1)
						rt := &ReplicateTask{
							Vid:        vid,
							Collection: c.Name,
							SrcDN:      locationList.PickForRead(),
							DstDN:      s,
						}
						tasks = append(tasks, rt)
						glog.V(0).Infof("add replicate task, vid: %v, src: %s, dst: %s", vid, rt.SrcDN.Url(), rt.DstDN.Url())
					}
				} else {
					glog.V(0).Infof("find empty slots error, vid: %v, rp: %s => %s, %v", vid, rp1.String(), volumeLayout.rp.String(), e)
				}
			}
		}
	}
	return
}

func (topo *Topology) CheckReplicate() {
	isReplicateCheckerRunning = true
	defer func() {
		isReplicateCheckerRunning = false
	}()
	glog.V(1).Infoln("Start replicate checker on demand")
	busyDataNodes := make(map[*DataNode]int)
	taskCount := 0
	taskQueue := list.New()
	for _, t := range planReplicateTasks(topo) {
		taskQueue.PushBack(t)
		taskCount++
	}
	taskChan := make(chan *ReplicateTask)
	for taskCount > 0 {
	TaskQueueLoop:
		for e := taskQueue.Front(); e != nil; e = e.Next() {
			task := e.Value.(*ReplicateTask)
			//only one task will run on the data node
			dns := task.WorkingDataNodes()
			for _, dn := range dns {
				if busyDataNodes[dn] > 0 {
					continue TaskQueueLoop
				}
			}
			for _, dn := range dns {
				busyDataNodes[dn]++
			}
			go func(t *ReplicateTask) {
				if e := t.Run(topo); e != nil {
					glog.V(0).Infof("ReplicateTask run error, vid: %v, dst: %s. %v", t.Vid, t.DstDN.Url(), e)
				} else {
					glog.V(2).Infof("ReplicateTask finished, vid: %v, dst: %s", t.Vid, t.DstDN.Url())

				}
				taskChan <- t
			}(task)
			taskQueue.Remove(e)

		}

		finishedTask := <-taskChan
		for _, dn := range finishedTask.WorkingDataNodes() {
			if busyDataNodes[dn] > 0 {
				busyDataNodes[dn]--
			}
		}
		taskCount--
		finishedTask.DstDN.UpAdjustPlannedVolumeCountDelta(-1)
	}
	glog.V(0).Infoln("finish replicate check.")
}

func (topo *Topology) StartCheckReplicate() {
	if isReplicateCheckerRunning {
		return
	}
	go topo.CheckReplicate()
}
