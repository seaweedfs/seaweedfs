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
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
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

// UnloadEcVolume drops the in-memory EcVolume for vid from this one disk without
// deleting files. Exported for the generation-fenced teardown, which unloads only
// the strictly-older disks rather than node-wide.
func (l *DiskLocation) UnloadEcVolume(vid needle.VolumeId) {
	l.unloadEcVolume(vid)
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

// HasEcxFileOnDisk reports whether this disk has a sealed .ecx index file
// for the given (collection, vid). Unlike FindEcVolume this does not
// require the EC volume to be mounted in memory, which makes it the right
// primitive for placement decisions during ec.balance / ec.rebuild flows
// where shards may arrive before any mount has happened on the receiving
// disk. Without checking the on-disk state, auto-select can split shards
// from the .ecx that travels with the first shard, which is the source of
// the orphan-shard layout reported in #9212.
func (l *DiskLocation) HasEcxFileOnDisk(collection string, vid needle.VolumeId) bool {
	idxBase := erasure_coding.EcShardFileName(collection, l.IdxDirectory, int(vid))
	// A 0-byte .ecx is a corrupt stub left by a failed EC distribute copy;
	// it cannot drive mount and must not steer placement decisions toward
	// this disk. Treat it as absent so the caller falls through to a
	// sibling disk that may hold a valid index.
	if info, err := os.Stat(idxBase + ".ecx"); err == nil && !info.IsDir() && info.Size() > 0 {
		return true
	}
	if l.IdxDirectory != l.Directory {
		dataBase := erasure_coding.EcShardFileName(collection, l.Directory, int(vid))
		if info, err := os.Stat(dataBase + ".ecx"); err == nil && !info.IsDir() && info.Size() > 0 {
			return true
		}
	}
	return false
}

func (l *DiskLocation) LoadEcShard(collection string, vid needle.VolumeId, shardId erasure_coding.ShardId) (*erasure_coding.EcVolume, error) {
	return l.loadEcShardWithIdxDir(collection, vid, shardId, l.IdxDirectory)
}

// loadEcShardWithIdxDir is like LoadEcShard but uses the supplied idxDir as
// the source of .ecx / .ecj rather than this disk's own IdxDirectory. The
// orphan-shard reconciliation calls this with a sibling disk's idx folder
// when shards live on a disk that does not own the index files itself
// (issue #9212).
func (l *DiskLocation) loadEcShardWithIdxDir(collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, idxDir string) (*erasure_coding.EcVolume, error) {

	ecVolumeShard, err := erasure_coding.NewEcVolumeShard(l.DiskType, l.Directory, collection, vid, shardId)
	if err != nil {
		if err == os.ErrNotExist {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("failed to create ec shard %d.%d: %w", vid, shardId, err)
	}
	l.ecVolumesLock.Lock()
	defer l.ecVolumesLock.Unlock()
	ecVolume, found := l.ecVolumes[vid]
	if !found {
		ecVolume, err = erasure_coding.NewEcVolume(l.DiskType, l.Directory, idxDir, collection, vid)
		if err != nil {
			// Wrap with %w so MountEcShards / startup reconcile can use
			// errors.Is(err, os.ErrNotExist) to decide whether to try the
			// next local disk vs. surface the failure.
			return nil, fmt.Errorf("failed to create ec volume %d: %w", vid, err)
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

func (l *DiskLocation) loadEcShards(shards []string, collection string, vid needle.VolumeId, onShardLoad func(collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, ecVolume *erasure_coding.EcVolume)) (err error) {

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

func (l *DiskLocation) loadAllEcShards(onShardLoad func(collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, ecVolume *erasure_coding.EcVolume)) (err error) {

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

// loadEcShardsWithIdxDir loads each shard file in shards into l.ecVolumes,
// using idxDir as the source of .ecx / .ecj / .vif (NewEcVolume falls back
// to dirIdx for .vif when the data dir does not have one). Used by the
// store-level orphan-shard reconciliation in #9212; stops on the first
// failure so the caller can log and continue with other volumes.
func (l *DiskLocation) loadEcShardsWithIdxDir(shards []string, collection string, vid needle.VolumeId, idxDir string, onShardLoad func(collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, ecVolume *erasure_coding.EcVolume)) error {

	for _, shard := range shards {
		ext := path.Ext(shard)
		if len(ext) < 4 {
			return fmt.Errorf("unexpected ec shard name %v", shard)
		}
		shardId, err := strconv.ParseInt(ext[3:], 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse ec shard name %v: %w", shard, err)
		}
		if shardId < 0 || shardId > 255 {
			return fmt.Errorf("shard ID out of range: %d", shardId)
		}

		ecVolume, err := l.loadEcShardWithIdxDir(collection, vid, erasure_coding.ShardId(shardId), idxDir)
		if err != nil {
			return fmt.Errorf("failed to load ec shard %v: %w", shard, err)
		}
		if onShardLoad != nil {
			onShardLoad(collection, vid, erasure_coding.ShardId(shardId), ecVolume)
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

	// A load failure (corrupt/locked .ecx, EMFILE, transient I/O) is not proof
	// the shards are disposable -- validateEcVolume already decided they may be
	// the only copy. Release FDs but keep the files for retry; never delete here.
	if err := l.loadEcShards(shards, collection, volumeId, onShardLoad); err != nil {
		glog.Warningf("Failed to load EC shards for volume %d: %v; keeping files for retry", volumeId, err)
		l.unloadEcVolume(volumeId)
		return
	}
}

// checkDatFileExists checks if a .dat file with actual data exists with robust
// error handling. An empty .dat (<= a superblock, zero needles) is a leftover
// stub, not an encode source, and is treated as absent so it never justifies
// deleting shards. Unexpected errors (permission, I/O) are treated as "exists"
// to avoid misclassifying local EC as distributed EC, which is the safer fallback.
func (l *DiskLocation) checkDatFileExists(datFileName string) bool {
	if fi, err := os.Stat(datFileName); err == nil {
		return fi.Size() > int64(super_block.SuperBlockSize)
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
// 1. Data is processed in batches of (LargeBlockSize * dataShardCount) for large blocks
// 2. Remaining data is processed in batches of (SmallBlockSize * dataShardCount) for small blocks
// 3. Each shard gets exactly its portion, with zero-padding applied to incomplete blocks
//
// dataShardCount is taken as a parameter rather than read from
// erasure_coding.DataShardsCount so that tests writing a custom layout
// to .vif compute the matching shard size, and so custom-ratio builds
// (e.g. enterprise) can swap the default without touching this helper.
func calculateExpectedShardSize(datFileSize int64, dataShardCount int) int64 {
	if dataShardCount <= 0 {
		return 0
	}
	var shardSize int64

	// Process large blocks (1GB * dataShardCount per batch)
	largeBatchSize := int64(erasure_coding.ErasureCodingLargeBlockSize) * int64(dataShardCount)
	numLargeBatches := datFileSize / largeBatchSize
	shardSize = numLargeBatches * int64(erasure_coding.ErasureCodingLargeBlockSize)
	remainingSize := datFileSize - (numLargeBatches * largeBatchSize)

	// Process remaining data in small blocks (1MB * dataShardCount per batch)
	if remainingSize > 0 {
		smallBatchSize := int64(erasure_coding.ErasureCodingSmallBlockSize) * int64(dataShardCount)
		numSmallBatches := (remainingSize + smallBatchSize - 1) / smallBatchSize // Ceiling division
		shardSize += numSmallBatches * int64(erasure_coding.ErasureCodingSmallBlockSize)
	}

	return shardSize
}

// validateEcVolume reports whether the EC files for (collection, vid) on this
// disk may be deleted to reclaim the local .dat. It returns false (delete)
// only when that provably loses no data; every ambiguity returns true (keep),
// since the shards may be the only copy of distributed-EC data.
func (l *DiskLocation) validateEcVolume(collection string, vid needle.VolumeId) bool {
	baseFileName := erasure_coding.EcShardFileName(collection, l.Directory, int(vid))
	datFileName := baseFileName + ".dat"

	// Custom ratio comes from the volume's own .vif; the server holds no
	// cluster EC config in memory.
	dataShards := l.ecDataShardsFromVif(collection, vid)

	// On-disk .dat size, or -1 when absent (an empty <= superblock .dat is a
	// stub). A transient stat error keeps the shards rather than deleting.
	var expectedShardSize int64 = -1
	datExists := false
	if datFileInfo, err := os.Stat(datFileName); err == nil {
		if datFileInfo.Size() > int64(super_block.SuperBlockSize) {
			datExists = true
			expectedShardSize = calculateExpectedShardSize(datFileInfo.Size(), dataShards)
		}
	} else if !os.IsNotExist(err) {
		glog.Warningf("EC volume %d: cannot stat .dat %s (%v); keeping EC shards", vid, datFileName, err)
		return true
	}

	// Count local shards; a transient stat error or inconsistent sizes -> keep.
	shardCount := 0
	var actualShardSize int64 = -1
	for i := 0; i < erasure_coding.MaxShardCount; i++ {
		shardFileName := baseFileName + erasure_coding.ToExt(i)
		fi, err := os.Stat(shardFileName)
		if err == nil {
			if fi.Size() > 0 {
				if actualShardSize == -1 {
					actualShardSize = fi.Size()
				} else if fi.Size() != actualShardSize {
					glog.Warningf("EC volume %d shard %d size %d != %d; keeping EC shards", vid, i, fi.Size(), actualShardSize)
					return true
				}
				shardCount++
			}
		} else if !os.IsNotExist(err) {
			glog.Warningf("EC volume %d: cannot stat shard %s (%v); keeping EC shards", vid, shardFileName, err)
			return true
		}
	}

	if !datExists {
		return true // distributed EC; any shard count is valid
	}

	// Reclaim only when it loses no data. Shards smaller than this .dat's full
	// encode are an interrupted encode whose .dat is the complete source ->
	// reclaim. Shards >= expected (valid/distributing EC, or a stale/partial
	// .dat beside larger real shards) may be the only copy -> keep.
	if shardCount == 0 {
		return false
	}
	if expectedShardSize > 0 && actualShardSize > 0 && actualShardSize < expectedShardSize {
		glog.Warningf("EC volume %d: %d shards of %d bytes are smaller than the .dat's full encode (%d bytes); reclaiming the complete .dat",
			vid, shardCount, actualShardSize, expectedShardSize)
		return false
	}
	return true
}

// ecDataShardsFromVif resolves the data-shard count for an EC volume from
// its own .vif (EcShardConfig), checking the data dir then the idx dir. The
// .vif is the source of truth for custom ratios on the volume server, which
// never holds the cluster EC config in memory. Falls back to the default
// ratio when the .vif carries no EC shard config.
func (l *DiskLocation) ecDataShardsFromVif(collection string, vid needle.VolumeId) int {
	// At most two dirs to check; avoid slice/map allocations on this
	// per-volume startup path.
	if l.Directory != "" {
		if ds := ecDataShardsFromVifDir(collection, l.Directory, vid); ds > 0 {
			return ds
		}
	}
	if l.IdxDirectory != "" && l.IdxDirectory != l.Directory {
		if ds := ecDataShardsFromVifDir(collection, l.IdxDirectory, vid); ds > 0 {
			return ds
		}
	}
	return erasure_coding.DataShardsCount
}

// ecDataShardsFromVifDir returns the .vif EcShardConfig data-shard count for
// (collection, vid) under dir, or 0 when absent / not custom.
func ecDataShardsFromVifDir(collection, dir string, vid needle.VolumeId) int {
	vifName := erasure_coding.EcShardFileName(collection, dir, int(vid)) + ".vif"
	if vi, _, found, _ := volume_info.MaybeLoadVolumeInfo(vifName); found && vi.EcShardConfig != nil {
		if ds := int(vi.EcShardConfig.DataShards); ds > 0 {
			return ds
		}
	}
	return 0
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
	// Also try the data directory in case .ecx/.ecj were created before -dir.idx was configured
	if l.IdxDirectory != l.Directory {
		removeFile(baseFileName+".ecx", "EC index file (fallback)")
		removeFile(baseFileName+".ecj", "EC journal file (fallback)")
	}

	// Remove all EC shard files (.ec00 ~ .ec31) from data directory
	// Use MaxShardCount (32) to support custom EC ratios
	for i := 0; i < erasure_coding.MaxShardCount; i++ {
		removeFile(baseFileName+erasure_coding.ToExt(i), "EC shard file")
	}
}
