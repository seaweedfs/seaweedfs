package storage

import (
	"io/ioutil"
	"strings"
	"sync"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

// DiskLocation is concurrent safe
type DiskLocation struct {
	Directory      string
	MaxVolumeCount int
	volumes        map[VolumeId]*Volume
	mutex          sync.RWMutex
}

func NewDiskLocation(dir string, maxVolCount int) *DiskLocation {
	return &DiskLocation{
		Directory:      dir,
		MaxVolumeCount: maxVolCount,
		volumes:        make(map[VolumeId]*Volume),
	}
}

func (l *DiskLocation) LoadExistingVolumes(needleMapKind NeedleMapType) {
	if dirs, err := ioutil.ReadDir(l.Directory); err == nil {
		for _, dir := range dirs {
			name := dir.Name()
			if !dir.IsDir() && strings.HasSuffix(name, ".dat") {
				collection := ""
				base := name[:len(name)-len(".dat")]
				i := strings.LastIndex(base, "_")
				if i > 0 {
					collection, base = base[0:i], base[i+1:]
				}
				if vid, err := NewVolumeId(base); err == nil {
					if !l.HasVolume(vid) {
						if v, e := NewVolume(l.Directory, collection, vid, needleMapKind, nil); e == nil {
							l.AddVolume(vid, v)
							glog.V(1).Infof("data file %s, v=%d size=%d ttl=%s", l.Directory+"/"+name, v.Version(), v.Size(), v.Ttl.String())
						} else {
							glog.V(0).Infof("new volume %s error %s", name, e)
						}
					}
				}
			}
		}
	}
	glog.V(0).Infoln("Store started on dir:", l.Directory, "with", l.VolumeCount(), "volumes", "max", l.MaxVolumeCount)
}

func (l *DiskLocation) AddVolume(vid VolumeId, v *Volume) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.volumes[vid] = v
}

func (l *DiskLocation) DeleteVolume(vid VolumeId) (e error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if v, ok := l.volumes[vid]; ok {
		e = v.Destroy()
	}
	delete(l.volumes, vid)
	return
}

func (l *DiskLocation) DeleteCollection(collection string) (e error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	for k, v := range l.volumes {
		if v.Collection == collection {
			e = v.Destroy()
			if e != nil {
				return
			}
			delete(l.volumes, k)
		}
	}
	return
}

func (l *DiskLocation) HasVolume(vid VolumeId) bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	_, ok := l.volumes[vid]
	return ok
}

func (l *DiskLocation) GetVolume(vid VolumeId) (v *Volume, ok bool) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	v, ok = l.volumes[vid]
	return
}

func (l *DiskLocation) VolumeCount() int {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return len(l.volumes)
}

func (l *DiskLocation) CloseAllVolume() {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	for _, v := range l.volumes {
		v.Close()
	}
}

// break walk when walker fuc return an error
type VolumeWalker func(v *Volume) (e error)

// must not add or delete volume in walker
func (l *DiskLocation) WalkVolume(vw VolumeWalker) (e error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	for _, v := range l.volumes {
		if e = vw(v); e != nil {
			return e
		}
	}
	return
}
