package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"

	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

var (
	re = regexp.MustCompile("\\.ec[0-9][0-9]")
)

func (l *DiskLocation) FindEcVolume(vid needle.VolumeId) (*erasure_coding.EcVolume, bool) {
	l.ecVolumesLock.RLock()
	defer l.ecVolumesLock.RUnlock()

	ecVolume, ok := l.ecVolumes[vid]
	if ok {
		return ecVolume, true
	}
	return nil, false
}

func (l *DiskLocation) DestroyEcVolume(vid needle.VolumeId) {
	l.ecVolumesLock.Lock()
	defer l.ecVolumesLock.Unlock()

	ecVolume, found := l.ecVolumes[vid]
	if found {
		ecVolume.Destroy()
		delete(l.ecVolumes, vid)
	}
}

func (l *DiskLocation) FindEcShard(vid needle.VolumeId, shardId erasure_coding.ShardId) (*erasure_coding.EcVolumeShard, bool) {
	l.ecVolumesLock.RLock()
	defer l.ecVolumesLock.RUnlock()

	ecVolume, ok := l.ecVolumes[vid]
	if !ok {
		return nil, false
	}
	for _, ecShard := range ecVolume.Shards {
		if ecShard.ShardId == shardId {
			return ecShard, true
		}
	}
	return nil, false
}

func (l *DiskLocation) LoadEcShard(collection string, vid needle.VolumeId, shardId erasure_coding.ShardId) (err error) {

	ecVolumeShard, err := erasure_coding.NewEcVolumeShard(l.Directory, collection, vid, shardId)
	if err != nil {
		if err == os.ErrNotExist {
			return os.ErrNotExist
		}
		return fmt.Errorf("failed to create ec shard %d.%d: %v", vid, shardId, err)
	}
	l.ecVolumesLock.Lock()
	defer l.ecVolumesLock.Unlock()
	ecVolume, found := l.ecVolumes[vid]
	if !found {
		ecVolume, err = erasure_coding.NewEcVolume(l.Directory, l.IdxDirectory, collection, vid)
		if err != nil {
			return fmt.Errorf("failed to create ec volume %d: %v", vid, err)
		}
		l.ecVolumes[vid] = ecVolume
	}
	ecVolume.AddEcVolumeShard(ecVolumeShard)

	return nil
}

func (l *DiskLocation) UnloadEcShard(vid needle.VolumeId, shardId erasure_coding.ShardId) bool {

	l.ecVolumesLock.Lock()
	defer l.ecVolumesLock.Unlock()

	ecVolume, found := l.ecVolumes[vid]
	if !found {
		return false
	}
	if _, deleted := ecVolume.DeleteEcVolumeShard(shardId); deleted {
		if len(ecVolume.Shards) == 0 {
			delete(l.ecVolumes, vid)
			ecVolume.Close()
		}
		return true
	}

	return true
}

func (l *DiskLocation) loadEcShards(shards []string, collection string, vid needle.VolumeId) (err error) {

	for _, shard := range shards {
		shardId, err := strconv.ParseInt(path.Ext(shard)[3:], 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse ec shard name %v: %v", shard, err)
		}

		err = l.LoadEcShard(collection, vid, erasure_coding.ShardId(shardId))
		if err != nil {
			return fmt.Errorf("failed to load ec shard %v: %v", shard, err)
		}
	}

	return nil
}

func (l *DiskLocation) loadAllEcShards() (err error) {

	fileInfos, err := ioutil.ReadDir(l.Directory)
	if err != nil {
		return fmt.Errorf("load all ec shards in dir %s: %v", l.Directory, err)
	}
	if l.IdxDirectory != l.Directory {
		indexFileInfos, err := ioutil.ReadDir(l.IdxDirectory)
		if err != nil {
			return fmt.Errorf("load all ec shards in dir %s: %v", l.IdxDirectory, err)
		}
		fileInfos = append(fileInfos, indexFileInfos...)
	}

	sort.Slice(fileInfos, func(i, j int) bool {
		return fileInfos[i].Name() < fileInfos[j].Name()
	})

	var sameVolumeShards []string
	var prevVolumeId needle.VolumeId
	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() {
			continue
		}
		ext := path.Ext(fileInfo.Name())
		name := fileInfo.Name()
		baseName := name[:len(name)-len(ext)]

		collection, volumeId, err := parseCollectionVolumeId(baseName)
		if err != nil {
			continue
		}

		if re.MatchString(ext) {
			if prevVolumeId == 0 || volumeId == prevVolumeId {
				sameVolumeShards = append(sameVolumeShards, fileInfo.Name())
			} else {
				sameVolumeShards = []string{fileInfo.Name()}
			}
			prevVolumeId = volumeId
			continue
		}

		if ext == ".ecx" && volumeId == prevVolumeId {
			if err = l.loadEcShards(sameVolumeShards, collection, volumeId); err != nil {
				return fmt.Errorf("loadEcShards collection:%v volumeId:%d : %v", collection, volumeId, err)
			}
			prevVolumeId = volumeId
			continue
		}

	}
	return nil
}

func (l *DiskLocation) deleteEcVolumeById(vid needle.VolumeId) (e error) {
	ecVolume, ok := l.ecVolumes[vid]
	if !ok {
		return
	}
	ecVolume.Destroy()
	delete(l.ecVolumes, vid)
	return
}

func (l *DiskLocation) unmountEcVolumeByCollection(collectionName string) map[needle.VolumeId]*erasure_coding.EcVolume {
	deltaVols := make(map[needle.VolumeId]*erasure_coding.EcVolume, 0)
	for k, v := range l.ecVolumes {
		if v.Collection == collectionName {
			deltaVols[k] = v
		}
	}

	for k, _ := range deltaVols {
		delete(l.ecVolumes, k)
	}
	return deltaVols
}

func (l *DiskLocation) EcVolumesLen() int {
	l.ecVolumesLock.RLock()
	defer l.ecVolumesLock.RUnlock()

	return len(l.ecVolumes)
}
