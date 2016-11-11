package storage

import (
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

type DiskLocation struct {
	Directory      string
	MaxVolumeCount int
	volumes        map[VolumeId]*Volume
	sync.RWMutex
}

func NewDiskLocation(dir string, maxVolumeCount int) *DiskLocation {
	location := &DiskLocation{Directory: dir, MaxVolumeCount: maxVolumeCount}
	location.volumes = make(map[VolumeId]*Volume)
	return location
}

func (l *DiskLocation) loadExistingVolumes(needleMapKind NeedleMapType) {
	l.Lock()
	defer l.Unlock()

	task_queue := make(chan os.FileInfo, 100)
	go func() {
		if dirs, err := ioutil.ReadDir(l.Directory); err == nil {
			for _, dir := range dirs {
				task_queue <- dir
			}
		}
		close(task_queue)
	}()

	const concurrency int = 10
	var wg sync.WaitGroup
	var mutex sync.RWMutex
	for workerNum := 0; workerNum < concurrency; workerNum++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dir := range task_queue {
				name := dir.Name()
				if !dir.IsDir() && strings.HasSuffix(name, ".dat") {
					collection := ""
					base := name[:len(name)-len(".dat")]
					i := strings.LastIndex(base, "_")
					if i > 0 {
						collection, base = base[0:i], base[i+1:]
					}
					if vid, err := NewVolumeId(base); err == nil {
						mutex.RLock()
						_, found := l.volumes[vid]
						mutex.RUnlock()
						if !found {
							if v, e := NewVolume(l.Directory, collection, vid, needleMapKind, nil, nil); e == nil {
								mutex.Lock()
								l.volumes[vid] = v
								mutex.Unlock()
								glog.V(0).Infof("data file %s, replicaPlacement=%s v=%d size=%d ttl=%s", l.Directory+"/"+name, v.ReplicaPlacement, v.Version(), v.Size(), v.Ttl.String())
							} else {
								glog.V(0).Infof("new volume %s error %s", name, e)
							}
						}
					}
				}
			}
		}()
	}
	wg.Wait()

	glog.V(0).Infoln("Store started on dir:", l.Directory, "with", len(l.volumes), "volumes", "max", l.MaxVolumeCount)
}

func (l *DiskLocation) DeleteCollectionFromDiskLocation(collection string) (e error) {
	l.Lock()
	defer l.Unlock()

	for k, v := range l.volumes {
		if v.Collection == collection {
			e = l.deleteVolumeById(k)
			if e != nil {
				return
			}
		}
	}
	return
}

func (l *DiskLocation) deleteVolumeById(vid VolumeId) (e error) {
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

func (l *DiskLocation) SetVolume(vid VolumeId, volume *Volume) {
	l.Lock()
	defer l.Unlock()

	l.volumes[vid] = volume
}

func (l *DiskLocation) FindVolume(vid VolumeId) (*Volume, bool) {
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
	defer l.Unlock()

	for _, v := range l.volumes {
		v.Close()
	}
	return
}
