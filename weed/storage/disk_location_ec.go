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
)

var (
	// Match .ec00 through .ec999 (currently only .ec00-.ec31 are used)
	// Using \d{2,3} for future-proofing if MaxShardCount is ever increased beyond 99
	re = regexp.MustCompile(`\.ec\d{2,3}`)
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

// unloadEcVolume removes an EC volume from memory without deleting its files on disk.
// This is useful for distributed EC volumes where shards may be on other servers.
func (l *DiskLocation) unloadEcVolume(vid needle.VolumeId) {
	var toClose *erasure_coding.EcVolume
	l.ecVolumesLock.Lock()
	if ecVolume, found := l.ecVolumes[vid]; found {
		toClose = ecVolume
		delete(l.ecVolumes, vid)
	}
	l.ecVolumesLock.Unlock()

	// Close outside the lock to avoid holding write lock during I/O
	if toClose != nil {
		toClose.Close()
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
	return l.loadEcShardsWithCallback(shards, collection, vid, nil)
}

func (l *DiskLocation) loadEcShardsWithCallback(shards []string, collection string, vid needle.VolumeId, onShardLoad func(collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, ecVolume *erasure_coding.EcVolume)) (err error) {

	for _, shard := range shards {
		shardId, err := strconv.ParseInt(path.Ext(shard)[3:], 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse ec shard name %v: %w", shard, err)
		}

		// Validate shardId range before converting to uint8
		if shardId < 0 || shardId > 255 {
			return fmt.Errorf("shard ID out of range: %d", shardId)
		}

		ecVolume, err := l.LoadEcShard(collection, vid, erasure_coding.ShardId(shardId))
		if err != nil {
			return fmt.Errorf("failed to load ec shard %v: %w", shard, err)
		}
		if onShardLoad != nil {
			onShardLoad(collection, vid, erasure_coding.ShardId(shardId), ecVolume)
		}
	}

	return nil
}

func (l *DiskLocation) loadAllEcShards() (err error) {
	return l.loadAllEcShardsWithCallback(nil)
}

func (l *DiskLocation) loadAllEcShardsWithCallback(onShardLoad func(collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, ecVolume *erasure_coding.EcVolume)) (err error) {

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
	var prevCollection string

	// Helper to reset state between volume processing
	reset := func() {
		sameVolumeShards = nil
		prevVolumeId = 0
		prevCollection = ""
	}

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
			// Group shards by both collection and volumeId to avoid mixing collections
			if prevVolumeId == 0 || (volumeId == prevVolumeId && collection == prevCollection) {
				sameVolumeShards = append(sameVolumeShards, fileInfo.Name())
			} else {
				// Before starting a new group, check if previous group had orphaned shards
				l.checkOrphanedShards(sameVolumeShards, prevCollection, prevVolumeId)
				sameVolumeShards = []string{fileInfo.Name()}
			}
			prevVolumeId = volumeId
			prevCollection = collection
			continue
		}

		if ext == ".ecx" && volumeId == prevVolumeId && collection == prevCollection {
			l.handleFoundEcxFile(sameVolumeShards, collection, volumeId, onShardLoad)
			reset()
			continue
		}

	}

	// Check for orphaned EC shards without .ecx file at the end of the directory scan
	// This handles the last group of shards in the directory
	l.checkOrphanedShards(sameVolumeShards, prevCollection, prevVolumeId)

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

// handleFoundEcxFile processes a complete group of EC shards when their .ecx file is found.
// This includes validation, loading, and cleanup of incomplete/invalid EC volumes.
func (l *DiskLocation) handleFoundEcxFile(shards []string, collection string, volumeId needle.VolumeId, onShardLoad func(collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, ecVolume *erasure_coding.EcVolume)) {
	// Check if this is an incomplete EC encoding (not a distributed EC volume)
	// Key distinction: if .dat file still exists, EC encoding may have failed
	// If .dat file is gone, this is likely a distributed EC volume with shards on multiple servers
	baseFileName := erasure_coding.EcShardFileName(collection, l.Directory, int(volumeId))
	datFileName := baseFileName + ".dat"

	// Determine .dat presence robustly; unexpected errors are treated as "exists"
	datExists := l.checkDatFileExists(datFileName)

	// Validate EC volume if .dat file exists (incomplete EC encoding scenario)
	// This checks shard count, shard size consistency, and expected size vs .dat file
	// If .dat is gone, EC encoding completed and shards are distributed across servers
	if datExists && !l.validateEcVolume(collection, volumeId) {
		glog.Warningf("Incomplete or invalid EC volume %d: .dat exists but validation failed, cleaning up EC files...", volumeId)
		l.removeEcVolumeFiles(collection, volumeId)
		return
	}

	// Attempt to load the EC shards
	if err := l.loadEcShardsWithCallback(shards, collection, volumeId, onShardLoad); err != nil {
		// If EC shards failed to load and .dat still exists, clean up EC files to allow .dat file to be used
		// If .dat is gone, log error but don't clean up (may be waiting for shards from other servers)
		if datExists {
			glog.Warningf("Failed to load EC shards for volume %d and .dat exists: %v, cleaning up EC files to use .dat...", volumeId, err)
			// Unload first to release FDs, then remove files
			l.unloadEcVolume(volumeId)
			l.removeEcVolumeFiles(collection, volumeId)
		} else {
			glog.Warningf("Failed to load EC shards for volume %d: %v (this may be normal for distributed EC volumes)", volumeId, err)
			// Clean up any partially loaded in-memory state. This does not delete files.
			l.unloadEcVolume(volumeId)
		}
		return
	}
}

// checkDatFileExists checks if .dat file exists with robust error handling.
// Unexpected errors (permission, I/O) are treated as "exists" to avoid misclassifying
// local EC as distributed EC, which is the safer fallback.
func (l *DiskLocation) checkDatFileExists(datFileName string) bool {
	if _, err := os.Stat(datFileName); err == nil {
		return true
	} else if !os.IsNotExist(err) {
		glog.Warningf("Failed to stat .dat file %s: %v", datFileName, err)
		// Safer to assume local .dat exists to avoid misclassifying as distributed EC
		return true
	}
	return false
}

// checkOrphanedShards checks if the given shards are orphaned (no .ecx file) and cleans them up if needed.
// Returns true if orphaned shards were found and cleaned up.
// This handles the case where EC encoding was interrupted before creating the .ecx file.
func (l *DiskLocation) checkOrphanedShards(shards []string, collection string, volumeId needle.VolumeId) bool {
	if len(shards) == 0 || volumeId == 0 {
		return false
	}

	// Check if .dat file exists (incomplete encoding, not distributed EC)
	baseFileName := erasure_coding.EcShardFileName(collection, l.Directory, int(volumeId))
	datFileName := baseFileName + ".dat"

	if l.checkDatFileExists(datFileName) {
		glog.Warningf("Found %d EC shards without .ecx file for volume %d (incomplete encoding interrupted before .ecx creation), cleaning up...",
			len(shards), volumeId)
		l.removeEcVolumeFiles(collection, volumeId)
		return true
	}
	return false
}

// calculateExpectedShardSize computes the exact expected shard size based on .dat file size
// The EC encoding process is deterministic:
// 1. Data is processed in batches of (LargeBlockSize * DataShardsCount) for large blocks
// 2. Remaining data is processed in batches of (SmallBlockSize * DataShardsCount) for small blocks
// 3. Each shard gets exactly its portion, with zero-padding applied to incomplete blocks
func calculateExpectedShardSize(datFileSize int64) int64 {
	var shardSize int64

	// Process large blocks (1GB * 10 = 10GB batches)
	largeBatchSize := int64(erasure_coding.ErasureCodingLargeBlockSize) * int64(erasure_coding.DataShardsCount)
	numLargeBatches := datFileSize / largeBatchSize
	shardSize = numLargeBatches * int64(erasure_coding.ErasureCodingLargeBlockSize)
	remainingSize := datFileSize - (numLargeBatches * largeBatchSize)

	// Process remaining data in small blocks (1MB * 10 = 10MB batches)
	if remainingSize > 0 {
		smallBatchSize := int64(erasure_coding.ErasureCodingSmallBlockSize) * int64(erasure_coding.DataShardsCount)
		numSmallBatches := (remainingSize + smallBatchSize - 1) / smallBatchSize // Ceiling division
		shardSize += numSmallBatches * int64(erasure_coding.ErasureCodingSmallBlockSize)
	}

	return shardSize
}

// validateEcVolume checks if EC volume has enough shards to be functional
// For distributed EC volumes (where .dat is deleted), any number of shards is valid
// For incomplete EC encoding (where .dat still exists), we need at least DataShardsCount shards
// Also validates that all shards have the same size (required for Reed-Solomon EC)
// If .dat exists, it also validates shards match the expected size based on .dat file size
func (l *DiskLocation) validateEcVolume(collection string, vid needle.VolumeId) bool {
	baseFileName := erasure_coding.EcShardFileName(collection, l.Directory, int(vid))
	datFileName := baseFileName + ".dat"

	var expectedShardSize int64 = -1
	datExists := false

	// If .dat file exists, compute exact expected shard size from it
	if datFileInfo, err := os.Stat(datFileName); err == nil {
		datExists = true
		expectedShardSize = calculateExpectedShardSize(datFileInfo.Size())
	} else if !os.IsNotExist(err) {
		// If stat fails with unexpected error (permission, I/O), fail validation
		// Don't treat this as "distributed EC" - it could be a temporary error
		glog.Warningf("Failed to stat .dat file %s: %v", datFileName, err)
		return false
	}

	shardCount := 0
	var actualShardSize int64 = -1

	// Count shards and validate they all have the same size (required for Reed-Solomon EC)
	// Check up to MaxShardCount (32) to support custom EC ratios
	for i := 0; i < erasure_coding.MaxShardCount; i++ {
		shardFileName := baseFileName + erasure_coding.ToExt(i)
		fi, err := os.Stat(shardFileName)

		if err == nil {
			// Check if file has non-zero size
			if fi.Size() > 0 {
				// Validate all shards are the same size (required for Reed-Solomon EC)
				if actualShardSize == -1 {
					actualShardSize = fi.Size()
				} else if fi.Size() != actualShardSize {
					glog.Warningf("EC volume %d shard %d has size %d, expected %d (all EC shards must be same size)",
						vid, i, fi.Size(), actualShardSize)
					return false
				}
				shardCount++
			}
		} else if !os.IsNotExist(err) {
			// If stat fails with unexpected error (permission, I/O), fail validation
			// This is consistent with .dat file error handling
			glog.Warningf("Failed to stat shard file %s: %v", shardFileName, err)
			return false
		}
	}

	// If .dat file exists, validate shard size matches expected size
	if datExists && actualShardSize > 0 && expectedShardSize > 0 {
		if actualShardSize != expectedShardSize {
			glog.Warningf("EC volume %d: shard size %d doesn't match expected size %d (based on .dat file size)",
				vid, actualShardSize, expectedShardSize)
			return false
		}
	}

	// If .dat file is gone, this is a distributed EC volume - any shard count is valid
	if !datExists {
		glog.V(1).Infof("EC volume %d: distributed EC (.dat removed) with %d shards", vid, shardCount)
		return true
	}

	// If .dat file exists, we need at least DataShardsCount shards locally
	// Otherwise it's an incomplete EC encoding that should be cleaned up
	if shardCount < erasure_coding.DataShardsCount {
		glog.Warningf("EC volume %d has .dat file but only %d shards (need at least %d for local EC)",
			vid, shardCount, erasure_coding.DataShardsCount)
		return false
	}

	return true
}

// removeEcVolumeFiles removes all EC-related files for a volume
func (l *DiskLocation) removeEcVolumeFiles(collection string, vid needle.VolumeId) {
	baseFileName := erasure_coding.EcShardFileName(collection, l.Directory, int(vid))
	indexBaseFileName := erasure_coding.EcShardFileName(collection, l.IdxDirectory, int(vid))

	// Helper to remove a file with consistent error handling
	removeFile := func(filePath, description string) {
		if err := os.Remove(filePath); err != nil {
			if !os.IsNotExist(err) {
				glog.Warningf("Failed to remove incomplete %s %s: %v", description, filePath, err)
			}
		} else {
			glog.V(2).Infof("Removed incomplete %s: %s", description, filePath)
		}
	}

	// Remove index files first (.ecx, .ecj) before shard files
	// This ensures that if cleanup is interrupted, the .ecx file won't trigger
	// EC loading for incomplete/missing shards on next startup
	removeFile(indexBaseFileName+".ecx", "EC index file")
	removeFile(indexBaseFileName+".ecj", "EC journal file")

	// Remove all EC shard files (.ec00 ~ .ec31) from data directory
	// Use MaxShardCount (32) to support custom EC ratios
	for i := 0; i < erasure_coding.MaxShardCount; i++ {
		removeFile(baseFileName+erasure_coding.ToExt(i), "EC shard file")
	}
}
