package storage

import (
	"fmt"
	"io/ioutil"
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

func (l *DiskLocation) HasEcShard(vid needle.VolumeId) (erasure_coding.EcVolumeShards, bool) {
	l.ecShardsLock.RLock()
	defer l.ecShardsLock.RUnlock()

	ecShards, ok := l.ecShards[vid]
	if ok {
		return ecShards, true
	}
	return nil, false
}

func (l *DiskLocation) FindEcShard(vid needle.VolumeId, shardId erasure_coding.ShardId) (*erasure_coding.EcVolumeShard, bool) {
	l.ecShardsLock.RLock()
	defer l.ecShardsLock.RUnlock()

	ecShards, ok := l.ecShards[vid]
	if !ok {
		return nil, false
	}
	for _, ecShard := range ecShards {
		if ecShard.ShardId == shardId {
			return ecShard, true
		}
	}
	return nil, false
}

func (l *DiskLocation) LoadEcShard(collection string, vid needle.VolumeId, shardId erasure_coding.ShardId) (err error) {

	ecVolumeShard, err := erasure_coding.NewEcVolumeShard(l.Directory, collection, vid, shardId)
	if err != nil {
		return fmt.Errorf("failed to create ec shard %d.%d: %v", vid, shardId, err)
	}
	l.ecShardsLock.Lock()
	l.ecShards[vid] = append(l.ecShards[vid], ecVolumeShard)
	l.ecShardsLock.Unlock()

	return nil
}

func (l *DiskLocation) UnloadEcShard(vid needle.VolumeId, shardId erasure_coding.ShardId) bool {

	l.ecShardsLock.Lock()
	defer l.ecShardsLock.Unlock()

	vidShards, found := l.ecShards[vid]
	if !found {
		return false
	}
	shardIndex := -1
	for i, shard := range vidShards {
		if shard.ShardId == shardId {
			shardIndex = i
			break
		}
	}
	if shardIndex < 0 {
		return false
	}

	if len(vidShards) == 1 {
		delete(l.ecShards, vid)
		return true
	}

	l.ecShards[vid] = append(vidShards[:shardIndex], vidShards[shardIndex+1:]...)

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
