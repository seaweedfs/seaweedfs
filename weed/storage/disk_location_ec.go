package storage

import (
	"fmt"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"

	"slices"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	re = regexp.MustCompile(`\.ec[0-9][0-9]`)
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

func (l *DiskLocation) CollectEcShards(vid needle.VolumeId, shardFileNames []string) (ecVolume *erasure_coding.EcVolume, found bool) {
	l.ecVolumesLock.RLock()
	defer l.ecVolumesLock.RUnlock()

	ecVolume, found = l.ecVolumes[vid]
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

func (l *DiskLocation) LoadEcShard(collection string, vid needle.VolumeId, shardId erasure_coding.ShardId) (*erasure_coding.EcVolume, error) {

	ecVolumeShard, err := erasure_coding.NewEcVolumeShard(l.DiskType, l.Directory, collection, vid, shardId)
	if err != nil {
		if err == os.ErrNotExist {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("failed to create ec shard %d.%d: %v", vid, shardId, err)
	}
	l.ecVolumesLock.Lock()
	defer l.ecVolumesLock.Unlock()
	ecVolume, found := l.ecVolumes[vid]
	if !found {
		ecVolume, err = erasure_coding.NewEcVolume(l.DiskType, l.Directory, l.IdxDirectory, collection, vid)
		if err != nil {
			return nil, fmt.Errorf("failed to create ec volume %d: %v", vid, err)
		}
		l.ecVolumes[vid] = ecVolume
	}
	ecVolume.AddEcVolumeShard(ecVolumeShard)

	return ecVolume, nil
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
			return fmt.Errorf("failed to parse ec shard name %v: %w", shard, err)
		}

		// Validate shardId range before converting to uint8
		if shardId < 0 || shardId > 255 {
			return fmt.Errorf("shard ID out of range: %d", shardId)
		}

		_, err = l.LoadEcShard(collection, vid, erasure_coding.ShardId(shardId))
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
			// Check if this is an incomplete EC encoding (not a distributed EC volume)
			// Key distinction: if .dat file still exists, EC encoding may have failed
			// If .dat file is gone, this is likely a distributed EC volume with shards on multiple servers
			baseFileName := erasure_coding.EcShardFileName(collection, l.Directory, int(volumeId))
			datFileName := baseFileName + ".dat"
			datExists := util.FileExists(datFileName)

			// Only validate shard count if .dat file exists (incomplete EC encoding scenario)
			// If .dat is gone, EC encoding completed and shards are distributed across servers
			if datExists && len(sameVolumeShards) < erasure_coding.DataShardsCount {
				glog.Warningf("Incomplete EC encoding for volume %d: .dat exists but only %d shards found (need at least %d), cleaning up EC files...",
					volumeId, len(sameVolumeShards), erasure_coding.DataShardsCount)
				l.removeEcVolumeFiles(collection, volumeId)
				sameVolumeShards = nil
				prevVolumeId = 0
				continue
			}

			if err = l.loadEcShards(sameVolumeShards, collection, volumeId); err != nil {
				// If EC shards failed to load and .dat still exists, clean up EC files to allow .dat file to be used
				// If .dat is gone, log error but don't clean up (may be waiting for shards from other servers)
				if datExists {
					glog.Warningf("Failed to load EC shards for volume %d and .dat exists: %v, cleaning up EC files to use .dat...", volumeId, err)
					// Clean up any partially loaded in-memory state before removing files
					l.DestroyEcVolume(volumeId)
					l.removeEcVolumeFiles(collection, volumeId)
				} else {
					glog.Warningf("Failed to load EC shards for volume %d: %v (this may be normal for distributed EC volumes)", volumeId, err)
					// Clean up any partially loaded in-memory state even if we don't remove files
					l.DestroyEcVolume(volumeId)
				}
				sameVolumeShards = nil
				prevVolumeId = 0
				continue
			}
			prevVolumeId = 0
			sameVolumeShards = nil
			continue
		}

	}

	// Check for orphaned EC shards without .ecx file (incomplete EC encoding)
	// This happens when encoding is interrupted after writing shards but before writing .ecx
	if len(sameVolumeShards) > 0 && prevVolumeId != 0 {
		// We have collected EC shards but never found .ecx file
		// Need to determine the collection name from the shard filenames
		baseName := sameVolumeShards[0][:len(sameVolumeShards[0])-len(path.Ext(sameVolumeShards[0]))]
		collection, volumeId, err := parseCollectionVolumeId(baseName)
		if err == nil && volumeId == prevVolumeId {
			baseFileName := erasure_coding.EcShardFileName(collection, l.Directory, int(volumeId))
			datFileName := baseFileName + ".dat"
			// Only clean up if .dat file exists (incomplete encoding, not distributed EC)
			if util.FileExists(datFileName) {
				glog.Warningf("Found %d EC shards without .ecx file for volume %d (incomplete encoding interrupted before .ecx creation), cleaning up...",
					len(sameVolumeShards), volumeId)
				// Clean up any in-memory state before removing files
				l.DestroyEcVolume(volumeId)
				l.removeEcVolumeFiles(collection, volumeId)
			}
		}
	}

	return nil
}

func (l *DiskLocation) deleteEcVolumeById(vid needle.VolumeId) (e error) {
	// Add write lock since we're modifying the ecVolumes map
	l.ecVolumesLock.Lock()
	defer l.ecVolumesLock.Unlock()

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

func (l *DiskLocation) EcShardCount() int {
	l.ecVolumesLock.RLock()
	defer l.ecVolumesLock.RUnlock()

	shardCount := 0
	for _, ecVolume := range l.ecVolumes {
		shardCount += len(ecVolume.Shards)
	}
	return shardCount
}

// validateEcVolume checks if EC volume has enough shards to be functional
// For distributed EC volumes (where .dat is deleted), any number of shards is valid
// For incomplete EC encoding (where .dat still exists), we need at least DataShardsCount shards
func (l *DiskLocation) validateEcVolume(collection string, vid needle.VolumeId) bool {
	baseFileName := erasure_coding.EcShardFileName(collection, l.Directory, int(vid))
	datFileName := baseFileName + ".dat"
	datExists := util.FileExists(datFileName)
	shardCount := 0

	// Count existing EC shard files
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		shardFileName := baseFileName + erasure_coding.ToExt(i)
		if fi, err := os.Stat(shardFileName); err == nil {
			// Check if file has non-zero size
			if fi.Size() > 0 {
				shardCount++
			}
		} else if !os.IsNotExist(err) {
			glog.Warningf("Failed to stat shard file %s: %v", shardFileName, err)
		}
	}

	// If .dat file is gone, this is a distributed EC volume - any shard count is valid
	if !datExists {
		glog.V(1).Infof("EC volume %d has %d shards (distributed EC, .dat removed)", vid, shardCount)
		return true
	}

	// If .dat file exists, we need at least DataShardsCount shards locally
	// Otherwise it's an incomplete EC encoding that should be cleaned up
	if shardCount < erasure_coding.DataShardsCount {
		glog.V(0).Infof("EC volume %d has .dat file but only %d shards (need at least %d for local EC)",
			vid, shardCount, erasure_coding.DataShardsCount)
		return false
	}

	return true
}

// removeEcVolumeFiles removes all EC-related files for a volume
func (l *DiskLocation) removeEcVolumeFiles(collection string, vid needle.VolumeId) {
	baseFileName := erasure_coding.EcShardFileName(collection, l.Directory, int(vid))
	indexBaseFileName := erasure_coding.EcShardFileName(collection, l.IdxDirectory, int(vid))

	// Remove all EC shard files (.ec00 ~ .ec13)
	for i := 0; i < erasure_coding.TotalShardsCount; i++ {
		shardFileName := baseFileName + erasure_coding.ToExt(i)
		if err := os.Remove(shardFileName); err != nil {
			if !os.IsNotExist(err) {
				glog.Warningf("Failed to remove incomplete EC shard file %s: %v", shardFileName, err)
			}
		} else {
			glog.V(2).Infof("Removed incomplete EC shard file: %s", shardFileName)
		}
	}

	// Remove index files
	if err := os.Remove(indexBaseFileName + ".ecx"); err != nil {
		if !os.IsNotExist(err) {
			glog.Warningf("Failed to remove incomplete EC index file %s.ecx: %v", indexBaseFileName, err)
		}
	} else {
		glog.V(2).Infof("Removed incomplete EC index file: %s.ecx", indexBaseFileName)
	}
	if err := os.Remove(indexBaseFileName + ".ecj"); err != nil {
		if !os.IsNotExist(err) {
			glog.Warningf("Failed to remove incomplete EC journal file %s.ecj: %v", indexBaseFileName, err)
		}
	} else {
		glog.V(2).Infof("Removed incomplete EC journal file: %s.ecj", indexBaseFileName)
	}
}
