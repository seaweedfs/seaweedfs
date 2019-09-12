package storage

import (
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"fmt"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

type DiskLocation struct {
	Directory      string
	MaxVolumeCount int
	volumes        map[needle.VolumeId]*Volume
	sync.RWMutex

	// erasure coding
	ecVolumes     map[needle.VolumeId]*erasure_coding.EcVolume
	ecVolumesLock sync.RWMutex
}

func NewDiskLocation(dir string, maxVolumeCount int) *DiskLocation {
	location := &DiskLocation{Directory: dir, MaxVolumeCount: maxVolumeCount}
	location.volumes = make(map[needle.VolumeId]*Volume)
	location.ecVolumes = make(map[needle.VolumeId]*erasure_coding.EcVolume)
	return location
}

func (l *DiskLocation) volumeIdFromPath(dir os.FileInfo) (needle.VolumeId, string, error) {
	name := dir.Name()
	if !dir.IsDir() && strings.HasSuffix(name, ".dat") {
		base := name[:len(name)-len(".dat")]
		collection, volumeId, err := parseCollectionVolumeId(base)
		return volumeId, collection, err
	}

	return 0, "", fmt.Errorf("Path is not a volume: %s", name)
}

func parseCollectionVolumeId(base string) (collection string, vid needle.VolumeId, err error) {
	i := strings.LastIndex(base, "_")
	if i > 0 {
		collection, base = base[0:i], base[i+1:]
	}
	vol, err := needle.NewVolumeId(base)
	return collection, vol, err
}

func (l *DiskLocation) loadExistingVolume(fileInfo os.FileInfo, needleMapKind NeedleMapType) {
	name := fileInfo.Name()
	if !fileInfo.IsDir() && strings.HasSuffix(name, ".dat") {
		vid, collection, err := l.volumeIdFromPath(fileInfo)
		if err == nil {
			l.RLock()
			_, found := l.volumes[vid]
			l.RUnlock()
			if !found {
				if v, e := NewVolume(l.Directory, collection, vid, needleMapKind, nil, nil, 0, 0); e == nil {
					l.Lock()
					l.volumes[vid] = v
					l.Unlock()
					size, _, _ := v.FileStat()
					glog.V(0).Infof("data file %s, replicaPlacement=%s v=%d size=%d ttl=%s",
						l.Directory+"/"+name, v.ReplicaPlacement, v.Version(), size, v.Ttl.String())
					// println("volume", vid, "last append at", v.lastAppendAtNs)
				} else {
					glog.V(0).Infof("new volume %s error %s", name, e)
				}

			}
		}
	}
}

func (l *DiskLocation) concurrentLoadingVolumes(needleMapKind NeedleMapType, concurrency int) {

	task_queue := make(chan os.FileInfo, 10*concurrency)
	go func() {
		if dirs, err := ioutil.ReadDir(l.Directory); err == nil {
			for _, dir := range dirs {
				task_queue <- dir
			}
		}
		close(task_queue)
	}()

	var wg sync.WaitGroup
	for workerNum := 0; workerNum < concurrency; workerNum++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dir := range task_queue {
				l.loadExistingVolume(dir, needleMapKind)
			}
		}()
	}
	wg.Wait()

}

func (l *DiskLocation) loadExistingVolumes(needleMapKind NeedleMapType) {

	l.concurrentLoadingVolumes(needleMapKind, 10)
	glog.V(0).Infof("Store started on dir: %s with %d volumes max %d", l.Directory, len(l.volumes), l.MaxVolumeCount)

	l.loadAllEcShards()
	glog.V(0).Infof("Store started on dir: %s with %d ec shards", l.Directory, len(l.ecVolumes))

}

func (l *DiskLocation) DeleteCollectionFromDiskLocation(collection string) (e error) {

	l.Lock()
	for k, v := range l.volumes {
		if v.Collection == collection {
			e = l.deleteVolumeById(k)
			if e != nil {
				l.Unlock()
				return
			}
		}
	}
	l.Unlock()

	l.ecVolumesLock.Lock()
	for k, v := range l.ecVolumes {
		if v.Collection == collection {
			e = l.deleteEcVolumeById(k)
			if e != nil {
				l.ecVolumesLock.Unlock()
				return
			}
		}
	}
	l.ecVolumesLock.Unlock()

	return
}

func (l *DiskLocation) deleteVolumeById(vid needle.VolumeId) (e error) {
	v, ok := l.volumes[vid]
	if !ok {
		return
	}
	e = v.Destroy()
	if e != nil {
		return
	}
	delete(l.volumes, vid)
	return
}

func (l *DiskLocation) LoadVolume(vid needle.VolumeId, needleMapKind NeedleMapType) bool {
	if fileInfos, err := ioutil.ReadDir(l.Directory); err == nil {
		for _, fileInfo := range fileInfos {
			volId, _, err := l.volumeIdFromPath(fileInfo)
			if vid == volId && err == nil {
				l.loadExistingVolume(fileInfo, needleMapKind)
				return true
			}
		}
	}

	return false
}

func (l *DiskLocation) DeleteVolume(vid needle.VolumeId) error {
	l.Lock()
	defer l.Unlock()

	_, ok := l.volumes[vid]
	if !ok {
		return fmt.Errorf("Volume not found, VolumeId: %d", vid)
	}
	return l.deleteVolumeById(vid)
}

func (l *DiskLocation) UnloadVolume(vid needle.VolumeId) error {
	l.Lock()
	defer l.Unlock()

	v, ok := l.volumes[vid]
	if !ok {
		return fmt.Errorf("Volume not loaded, VolumeId: %d", vid)
	}
	v.Close()
	delete(l.volumes, vid)
	return nil
}

func (l *DiskLocation) SetVolume(vid needle.VolumeId, volume *Volume) {
	l.Lock()
	defer l.Unlock()

	l.volumes[vid] = volume
}

func (l *DiskLocation) FindVolume(vid needle.VolumeId) (*Volume, bool) {
	l.RLock()
	defer l.RUnlock()

	v, ok := l.volumes[vid]
	return v, ok
}

func (l *DiskLocation) VolumesLen() int {
	l.RLock()
	defer l.RUnlock()

	return len(l.volumes)
}

func (l *DiskLocation) Close() {
	l.Lock()
	for _, v := range l.volumes {
		v.Close()
	}
	l.Unlock()

	l.ecVolumesLock.Lock()
	for _, ecVolume := range l.ecVolumes {
		ecVolume.Close()
	}
	l.ecVolumesLock.Unlock()

	return
}
