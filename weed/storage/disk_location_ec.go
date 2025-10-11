package storage

import (
	"fmt"
	"math"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"

	"slices"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

var (
	re = regexp.MustCompile(`\.ec[0-9][0-9]`)
)

// EcVolumeGenerationKey represents a unique key for EC volume with generation
type EcVolumeGenerationKey struct {
	VolumeId   needle.VolumeId
	Generation uint32
}

func (k EcVolumeGenerationKey) String() string {
	return fmt.Sprintf("v%d-g%d", k.VolumeId, k.Generation)
}

func (l *DiskLocation) FindEcVolume(vid needle.VolumeId) (*erasure_coding.EcVolume, bool) {
	l.ecVolumesLock.RLock()
	defer l.ecVolumesLock.RUnlock()

	// Search for any generation of this volume ID
	for key, ecVolume := range l.ecVolumes {
		if key.VolumeId == vid {
			return ecVolume, true
		}
	}
	return nil, false
}

func (l *DiskLocation) DestroyEcVolume(vid needle.VolumeId) {
	l.ecVolumesLock.Lock()
	defer l.ecVolumesLock.Unlock()

	// Find and destroy all generations of this volume
	keysToDelete := make([]EcVolumeGenerationKey, 0)
	for key, ecVolume := range l.ecVolumes {
		if key.VolumeId == vid {
			ecVolume.Destroy()
			keysToDelete = append(keysToDelete, key)
		}
	}
	for _, key := range keysToDelete {
		delete(l.ecVolumes, key)
	}
}

func (l *DiskLocation) CollectEcShards(vid needle.VolumeId, shardFileNames []string) (ecVolume *erasure_coding.EcVolume, found bool) {
	l.ecVolumesLock.RLock()
	defer l.ecVolumesLock.RUnlock()

	// Search for any generation of this volume ID
	for key, vol := range l.ecVolumes {
		if key.VolumeId == vid {
			ecVolume = vol
			found = true
			break
		}
	}
	if !found {
		return
	}
	for _, ecShard := range ecVolume.Shards {
		if ecShard.ShardId < erasure_coding.ShardId(len(shardFileNames)) {
			shardFileNames[ecShard.ShardId] = erasure_coding.EcShardFileName(ecVolume.Collection, l.Directory, int(ecVolume.VolumeId)) + erasure_coding.ToExt(int(ecShard.ShardId))
		}
	}
	return
}

func (l *DiskLocation) FindEcShard(vid needle.VolumeId, shardId erasure_coding.ShardId) (*erasure_coding.EcVolumeShard, bool) {
	l.ecVolumesLock.RLock()
	defer l.ecVolumesLock.RUnlock()

	// Search for any generation of this volume ID
	for key, ecVolume := range l.ecVolumes {
		if key.VolumeId == vid {
			for _, ecShard := range ecVolume.Shards {
				if ecShard.ShardId == shardId {
					return ecShard, true
				}
			}
		}
	}
	return nil, false
}

func (l *DiskLocation) FindEcShardWithGeneration(vid needle.VolumeId, shardId erasure_coding.ShardId, generation uint32) (*erasure_coding.EcVolumeShard, bool) {
	l.ecVolumesLock.RLock()
	defer l.ecVolumesLock.RUnlock()

	// Search for specific generation of this volume ID
	key := EcVolumeGenerationKey{VolumeId: vid, Generation: generation}
	ecVolume, ok := l.ecVolumes[key]
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

func (l *DiskLocation) LoadEcShard(collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, generation uint32) (*erasure_coding.EcVolume, error) {

	ecVolumeShard, err := erasure_coding.NewEcVolumeShard(l.DiskType, l.Directory, collection, vid, shardId, generation)
	if err != nil {
		if err == os.ErrNotExist {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("failed to create ec shard %d.%d: %v", vid, shardId, err)
	}
	l.ecVolumesLock.Lock()
	defer l.ecVolumesLock.Unlock()
	key := EcVolumeGenerationKey{VolumeId: vid, Generation: generation}
	ecVolume, found := l.ecVolumes[key]
	if !found {
		ecVolume, err = erasure_coding.NewEcVolume(l.DiskType, l.Directory, l.IdxDirectory, collection, vid, generation)
		if err != nil {
			return nil, fmt.Errorf("failed to create ec volume %d: %v", vid, err)
		}
		l.ecVolumes[key] = ecVolume
	}
	ecVolume.AddEcVolumeShard(ecVolumeShard)

	return ecVolume, nil
}

func (l *DiskLocation) UnloadEcShard(vid needle.VolumeId, shardId erasure_coding.ShardId) bool {

	l.ecVolumesLock.Lock()
	defer l.ecVolumesLock.Unlock()

	// Search for any generation of this volume ID
	for key, ecVolume := range l.ecVolumes {
		if key.VolumeId == vid {
			if _, deleted := ecVolume.DeleteEcVolumeShard(shardId); deleted {
				if len(ecVolume.Shards) == 0 {
					delete(l.ecVolumes, key)
					ecVolume.Close()
				}
				return true
			}
		}
	}

	return false
}

func (l *DiskLocation) loadEcShards(shards []string, collection string, vid needle.VolumeId, generation uint32) (err error) {

	for _, shard := range shards {
		shardId, err := strconv.ParseInt(path.Ext(shard)[3:], 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse ec shard name %v: %w", shard, err)
		}

		// Bounds check for uint8 (ShardId)
		if shardId < 0 || shardId > int64(math.MaxUint8) {
			return fmt.Errorf("ec shard id %v out of bounds for uint8 in shard name %v", shardId, shard)
		}

		_, err = l.LoadEcShard(collection, vid, erasure_coding.ShardId(shardId), generation)
		if err != nil {
			return fmt.Errorf("failed to load ec shard %v: %w", shard, err)
		}
	}

	return nil
}

func (l *DiskLocation) loadAllEcShards() (err error) {

	dirEntries, err := os.ReadDir(l.Directory)
	if err != nil {
		return fmt.Errorf("load all ec shards in dir %s: %v", l.Directory, err)
	}
	if l.IdxDirectory != l.Directory {
		indexDirEntries, err := os.ReadDir(l.IdxDirectory)
		if err != nil {
			return fmt.Errorf("load all ec shards in dir %s: %v", l.IdxDirectory, err)
		}
		dirEntries = append(dirEntries, indexDirEntries...)
	}
	slices.SortFunc(dirEntries, func(a, b os.DirEntry) int {
		return strings.Compare(a.Name(), b.Name())
	})
	var sameVolumeShards []string
	var prevVolumeId needle.VolumeId
	for _, fileInfo := range dirEntries {
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

		info, err := fileInfo.Info()

		if err != nil {
			continue
		}

		// 0 byte files should be only appearing erroneously for ec data files
		// so we ignore them
		if re.MatchString(ext) && info.Size() > 0 {
			if prevVolumeId == 0 || volumeId == prevVolumeId {
				sameVolumeShards = append(sameVolumeShards, fileInfo.Name())
			} else {
				sameVolumeShards = []string{fileInfo.Name()}
			}
			prevVolumeId = volumeId
			continue
		}

		if ext == ".ecx" && volumeId == prevVolumeId {
			// Parse generation from the first shard filename
			generation := uint32(0)
			if len(sameVolumeShards) > 0 {
				generation = erasure_coding.ParseGenerationFromFileName(sameVolumeShards[0])
			}
			if err = l.loadEcShards(sameVolumeShards, collection, volumeId, generation); err != nil {
				return fmt.Errorf("loadEcShards collection:%v volumeId:%d generation:%d : %v", collection, volumeId, generation, err)
			}
			prevVolumeId = volumeId
			continue
		}

	}
	return nil
}

func (l *DiskLocation) deleteEcVolumeById(vid needle.VolumeId) (e error) {
	// Add write lock since we're modifying the ecVolumes map
	l.ecVolumesLock.Lock()
	defer l.ecVolumesLock.Unlock()

	// Find and delete all generations of this volume
	keysToDelete := make([]EcVolumeGenerationKey, 0)
	for key, ecVolume := range l.ecVolumes {
		if key.VolumeId == vid {
			ecVolume.Destroy()
			keysToDelete = append(keysToDelete, key)
		}
	}
	for _, key := range keysToDelete {
		delete(l.ecVolumes, key)
	}
	return
}

func (l *DiskLocation) unmountEcVolumeByCollection(collectionName string) map[needle.VolumeId]*erasure_coding.EcVolume {
	deltaVols := make(map[needle.VolumeId]*erasure_coding.EcVolume, 0)
	keysToDelete := make([]EcVolumeGenerationKey, 0)
	for k, v := range l.ecVolumes {
		if v.Collection == collectionName {
			deltaVols[k.VolumeId] = v
			keysToDelete = append(keysToDelete, k)
		}
	}

	for _, key := range keysToDelete {
		delete(l.ecVolumes, key)
	}
	return deltaVols
}

func (l *DiskLocation) EcShardCount() int {
	l.ecVolumesLock.RLock()
	defer l.ecVolumesLock.RUnlock()

	shardCount := 0
	for _, ecVolume := range l.ecVolumes {
		shardCount += len(ecVolume.Shards)
	}
	return shardCount
}
