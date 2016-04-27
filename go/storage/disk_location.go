package storage

import (
	"io/ioutil"
	"strings"

	"github.com/chrislusf/seaweedfs/go/glog"
)

type DiskLocation struct {
	Directory      string
	MaxVolumeCount int
	volumes        map[VolumeId]*Volume
}

func NewDiskLocation(dir string, maxVolumeCount int) *DiskLocation {
	location := &DiskLocation{Directory: dir, MaxVolumeCount: maxVolumeCount}
	location.volumes = make(map[VolumeId]*Volume)
	return location
}

func (l *DiskLocation) loadExistingVolumes(needleMapKind NeedleMapType) {

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
					if l.volumes[vid] == nil {
						if v, e := NewVolume(l.Directory, collection, vid, needleMapKind, nil, nil); e == nil {
							l.volumes[vid] = v
							glog.V(0).Infof("data file %s, replicaPlacement=%s v=%d size=%d ttl=%s", l.Directory+"/"+name, v.ReplicaPlacement, v.Version(), v.Size(), v.Ttl.String())
						} else {
							glog.V(0).Infof("new volume %s error %s", name, e)
						}
					}
				}
			}
		}
	}
	glog.V(0).Infoln("Store started on dir:", l.Directory, "with", len(l.volumes), "volumes", "max", l.MaxVolumeCount)
}

func (l *DiskLocation) DeleteCollectionFromDiskLocation(collection string) (e error) {
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
