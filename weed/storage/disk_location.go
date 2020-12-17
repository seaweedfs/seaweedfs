package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type DiskLocation struct {
	Directory              string
	IdxDirectory           string
	MaxVolumeCount         int
	OriginalMaxVolumeCount int
	MinFreeSpacePercent    float32
	volumes                map[needle.VolumeId]*Volume
	volumesLock            sync.RWMutex

	// erasure coding
	ecVolumes     map[needle.VolumeId]*erasure_coding.EcVolume
	ecVolumesLock sync.RWMutex

	isDiskSpaceLow bool
}

func NewDiskLocation(dir string, maxVolumeCount int, minFreeSpacePercent float32, idxDir string) *DiskLocation {
	dir = util.ResolvePath(dir)
	if idxDir == "" {
		idxDir = dir
	} else {
		idxDir = util.ResolvePath(idxDir)
	}
	location := &DiskLocation{
		Directory:              dir,
		IdxDirectory:           idxDir,
		MaxVolumeCount:         maxVolumeCount,
		OriginalMaxVolumeCount: maxVolumeCount,
		MinFreeSpacePercent:    minFreeSpacePercent,
	}
	location.volumes = make(map[needle.VolumeId]*Volume)
	location.ecVolumes = make(map[needle.VolumeId]*erasure_coding.EcVolume)
	go location.CheckDiskSpace()
	return location
}

func volumeIdFromFileName(filename string) (needle.VolumeId, string, error) {
	if isValidVolume(filename) {
		base := filename[:len(filename)-4]
		collection, volumeId, err := parseCollectionVolumeId(base)
		return volumeId, collection, err
	}

	return 0, "", fmt.Errorf("file is not a volume: %s", filename)
}

func parseCollectionVolumeId(base string) (collection string, vid needle.VolumeId, err error) {
	i := strings.LastIndex(base, "_")
	if i > 0 {
		collection, base = base[0:i], base[i+1:]
	}
	vol, err := needle.NewVolumeId(base)
	return collection, vol, err
}

func isValidVolume(basename string) bool {
	return strings.HasSuffix(basename, ".idx") || strings.HasSuffix(basename, ".vif")
}

func getValidVolumeName(basename string) string {
	if isValidVolume(basename) {
		return basename[:len(basename)-4]
	}
	return ""
}

func (l *DiskLocation) loadExistingVolume(fileInfo os.FileInfo, needleMapKind NeedleMapType) bool {
	basename := fileInfo.Name()
	if fileInfo.IsDir() {
		return false
	}
	volumeName := getValidVolumeName(basename)
	if volumeName == "" {
		return false
	}

	// check for incomplete volume
	noteFile := l.Directory + "/" + volumeName + ".note"
	if util.FileExists(noteFile) {
		note, _ := ioutil.ReadFile(noteFile)
		glog.Warningf("volume %s was not completed: %s", volumeName, string(note))
		removeVolumeFiles(l.Directory + "/" + volumeName)
		removeVolumeFiles(l.IdxDirectory + "/" + volumeName)
		return false
	}

	// parse out collection, volume id
	vid, collection, err := volumeIdFromFileName(basename)
	if err != nil {
		glog.Warningf("get volume id failed, %s, err : %s", volumeName, err)
		return false
	}

	// avoid loading one volume more than once
	l.volumesLock.RLock()
	_, found := l.volumes[vid]
	l.volumesLock.RUnlock()
	if found {
		glog.V(1).Infof("loaded volume, %v", vid)
		return true
	}

	// load the volume
	v, e := NewVolume(l.Directory, l.IdxDirectory, collection, vid, needleMapKind, nil, nil, 0, 0)
	if e != nil {
		glog.V(0).Infof("new volume %s error %s", volumeName, e)
		return false
	}

	l.SetVolume(vid, v)

	size, _, _ := v.FileStat()
	glog.V(0).Infof("data file %s, replicaPlacement=%s v=%d size=%d ttl=%s",
		l.Directory+"/"+volumeName+".dat", v.ReplicaPlacement, v.Version(), size, v.Ttl.String())
	return true
}

func (l *DiskLocation) concurrentLoadingVolumes(needleMapKind NeedleMapType, concurrency int) {

	task_queue := make(chan os.FileInfo, 10*concurrency)
	go func() {
		foundVolumeNames := make(map[string]bool)
		if fileInfos, err := ioutil.ReadDir(l.Directory); err == nil {
			for _, fi := range fileInfos {
				volumeName := getValidVolumeName(fi.Name())
				if volumeName == "" {
					continue
				}
				if _, found := foundVolumeNames[volumeName]; !found {
					foundVolumeNames[volumeName] = true
					task_queue <- fi
				}
			}
		}
		close(task_queue)
	}()

	var wg sync.WaitGroup
	for workerNum := 0; workerNum < concurrency; workerNum++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fi := range task_queue {
				_ = l.loadExistingVolume(fi, needleMapKind)
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

	l.volumesLock.Lock()
	delVolsMap := l.unmountVolumeByCollection(collection)
	l.volumesLock.Unlock()

	l.ecVolumesLock.Lock()
	delEcVolsMap := l.unmountEcVolumeByCollection(collection)
	l.ecVolumesLock.Unlock()

	errChain := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		for _, v := range delVolsMap {
			if err := v.Destroy(); err != nil {
				errChain <- err
			}
		}
		wg.Done()
	}()

	go func() {
		for _, v := range delEcVolsMap {
			v.Destroy()
		}
		wg.Done()
	}()

	go func() {
		wg.Wait()
		close(errChain)
	}()

	errBuilder := strings.Builder{}
	for err := range errChain {
		errBuilder.WriteString(err.Error())
		errBuilder.WriteString("; ")
	}
	if errBuilder.Len() > 0 {
		e = fmt.Errorf(errBuilder.String())
	}

	return
}

func (l *DiskLocation) deleteVolumeById(vid needle.VolumeId) (found bool, e error) {
	v, ok := l.volumes[vid]
	if !ok {
		return
	}
	e = v.Destroy()
	if e != nil {
		return
	}
	found = true
	delete(l.volumes, vid)
	return
}

func (l *DiskLocation) LoadVolume(vid needle.VolumeId, needleMapKind NeedleMapType) bool {
	if fileInfo, found := l.LocateVolume(vid); found {
		return l.loadExistingVolume(fileInfo, needleMapKind)
	}
	return false
}

func (l *DiskLocation) DeleteVolume(vid needle.VolumeId) error {
	l.volumesLock.Lock()
	defer l.volumesLock.Unlock()

	_, ok := l.volumes[vid]
	if !ok {
		return fmt.Errorf("Volume not found, VolumeId: %d", vid)
	}
	_, err := l.deleteVolumeById(vid)
	return err
}

func (l *DiskLocation) UnloadVolume(vid needle.VolumeId) error {
	l.volumesLock.Lock()
	defer l.volumesLock.Unlock()

	v, ok := l.volumes[vid]
	if !ok {
		return fmt.Errorf("Volume not loaded, VolumeId: %d", vid)
	}
	v.Close()
	delete(l.volumes, vid)
	return nil
}

func (l *DiskLocation) unmountVolumeByCollection(collectionName string) map[needle.VolumeId]*Volume {
	deltaVols := make(map[needle.VolumeId]*Volume, 0)
	for k, v := range l.volumes {
		if v.Collection == collectionName && !v.isCompacting {
			deltaVols[k] = v
		}
	}

	for k := range deltaVols {
		delete(l.volumes, k)
	}
	return deltaVols
}

func (l *DiskLocation) SetVolume(vid needle.VolumeId, volume *Volume) {
	l.volumesLock.Lock()
	defer l.volumesLock.Unlock()

	l.volumes[vid] = volume
	volume.location = l
}

func (l *DiskLocation) FindVolume(vid needle.VolumeId) (*Volume, bool) {
	l.volumesLock.RLock()
	defer l.volumesLock.RUnlock()

	v, ok := l.volumes[vid]
	return v, ok
}

func (l *DiskLocation) VolumesLen() int {
	l.volumesLock.RLock()
	defer l.volumesLock.RUnlock()

	return len(l.volumes)
}

func (l *DiskLocation) Close() {
	l.volumesLock.Lock()
	for _, v := range l.volumes {
		v.Close()
	}
	l.volumesLock.Unlock()

	l.ecVolumesLock.Lock()
	for _, ecVolume := range l.ecVolumes {
		ecVolume.Close()
	}
	l.ecVolumesLock.Unlock()

	return
}

func (l *DiskLocation) LocateVolume(vid needle.VolumeId) (os.FileInfo, bool) {
	if fileInfos, err := ioutil.ReadDir(l.Directory); err == nil {
		for _, fileInfo := range fileInfos {
			volId, _, err := volumeIdFromFileName(fileInfo.Name())
			if vid == volId && err == nil {
				return fileInfo, true
			}
		}
	}

	return nil, false
}

func (l *DiskLocation) UnUsedSpace(volumeSizeLimit uint64) (unUsedSpace uint64) {

	l.volumesLock.RLock()
	defer l.volumesLock.RUnlock()

	for _, vol := range l.volumes {
		if vol.IsReadOnly() {
			continue
		}
		datSize, idxSize, _ := vol.FileStat()
		unUsedSpace += volumeSizeLimit - (datSize + idxSize)
	}

	return
}

func (l *DiskLocation) CheckDiskSpace() {
	for {
		if dir, e := filepath.Abs(l.Directory); e == nil {
			s := stats.NewDiskStatus(dir)
			stats.VolumeServerResourceGauge.WithLabelValues(l.Directory, "all").Set(float64(s.All))
			stats.VolumeServerResourceGauge.WithLabelValues(l.Directory, "used").Set(float64(s.Used))
			stats.VolumeServerResourceGauge.WithLabelValues(l.Directory, "free").Set(float64(s.Free))
			if (s.PercentFree < l.MinFreeSpacePercent) != l.isDiskSpaceLow {
				l.isDiskSpaceLow = !l.isDiskSpaceLow
			}
			if l.isDiskSpaceLow {
				glog.V(0).Infof("dir %s freePercent %.2f%% < min %.2f%%, isLowDiskSpace: %v", dir, s.PercentFree, l.MinFreeSpacePercent, l.isDiskSpaceLow)
			} else {
				glog.V(4).Infof("dir %s freePercent %.2f%% < min %.2f%%, isLowDiskSpace: %v", dir, s.PercentFree, l.MinFreeSpacePercent, l.isDiskSpaceLow)
			}
		}
		time.Sleep(time.Minute)
	}

}
