package erasure_coding

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

var (
	NotFoundError             = errors.New("needle not found")
	destroyDelaySeconds int64 = 0
)

type EcVolume struct {
	VolumeId                  needle.VolumeId
	Collection                string
	dir                       string
	dirIdx                    string
	ecxActualDir              string // directory where .ecx/.ecj were actually found (may differ from dirIdx after fallback)
	ecxFile                   *os.File
	ecxFileSize               int64
	ecxCreatedAt              time.Time
	Shards                    []*EcVolumeShard
	ShardLocations            map[ShardId][]pb.ServerAddress
	ShardLocationsRefreshTime time.Time
	ShardLocationsLock        sync.RWMutex
	Version                   needle.Version
	ecjFile                   *os.File
	ecjFileAccessLock         sync.Mutex
	diskType                  types.DiskType
	datFileSize               int64
	ExpireAtSec               uint64     //ec volume destroy time, calculated from the ec volume was created
	ECContext                 *ECContext // EC encoding parameters

	// countsLock guards the in-memory file/delete counters below. These are
	// seeded once from .ecx on volume load and then maintained incrementally
	// on every DeleteNeedleFromEcx call, analogous to how a regular volume's
	// needle map tracks its FileCount/DeletedCount. Each heartbeat reads the
	// live values directly without re-walking the index.
	countsLock  sync.Mutex
	fileCount   uint64 // total needles ever recorded in .ecx (live + tombstoned)
	deleteCount uint64 // tombstoned needles in this node's .ecx copy
}

func NewEcVolume(diskType types.DiskType, dir string, dirIdx string, collection string, vid needle.VolumeId) (ev *EcVolume, err error) {
	ev = &EcVolume{dir: dir, dirIdx: dirIdx, Collection: collection, VolumeId: vid, diskType: diskType}

	dataBaseFileName := EcShardFileName(collection, dir, int(vid))
	indexBaseFileName := EcShardFileName(collection, dirIdx, int(vid))

	// open ecx file
	ev.ecxActualDir = dirIdx
	if ev.ecxFile, err = os.OpenFile(indexBaseFileName+".ecx", os.O_RDWR, 0644); err != nil {
		if dirIdx != dir && os.IsNotExist(err) {
			// fall back to data directory if idx directory does not have the .ecx file
			firstErr := err
			glog.V(1).Infof("ecx file not found at %s.ecx, falling back to %s.ecx", indexBaseFileName, dataBaseFileName)
			if ev.ecxFile, err = os.OpenFile(dataBaseFileName+".ecx", os.O_RDWR, 0644); err != nil {
				return nil, fmt.Errorf("open ecx index %s.ecx: %v; fallback %s.ecx: %v", indexBaseFileName, firstErr, dataBaseFileName, err)
			}
			indexBaseFileName = dataBaseFileName
			ev.ecxActualDir = dir
		} else {
			return nil, fmt.Errorf("cannot open ec volume index %s.ecx: %v", indexBaseFileName, err)
		}
	}
	ecxFi, statErr := ev.ecxFile.Stat()
	if statErr != nil {
		_ = ev.ecxFile.Close()
		return nil, fmt.Errorf("can not stat ec volume index %s.ecx: %v", indexBaseFileName, statErr)
	}
	ev.ecxFileSize = ecxFi.Size()
	ev.ecxCreatedAt = ecxFi.ModTime()

	// open ecj file
	if ev.ecjFile, err = os.OpenFile(indexBaseFileName+".ecj", os.O_RDWR|os.O_CREATE, 0644); err != nil {
		return nil, fmt.Errorf("cannot open ec volume journal %s.ecj: %v", indexBaseFileName, err)
	}

	// read volume info
	ev.Version = needle.Version3
	if volumeInfo, _, found, _ := volume_info.MaybeLoadVolumeInfo(dataBaseFileName + ".vif"); found {
		ev.Version = needle.Version(volumeInfo.Version)
		ev.datFileSize = volumeInfo.DatFileSize
		ev.ExpireAtSec = volumeInfo.ExpireAtSec

		// Initialize EC context from .vif if present; fallback to defaults
		if volumeInfo.EcShardConfig != nil {
			ds := int(volumeInfo.EcShardConfig.DataShards)
			ps := int(volumeInfo.EcShardConfig.ParityShards)

			// Validate shard counts to prevent zero or invalid values
			if ds <= 0 || ps <= 0 || ds+ps > MaxShardCount {
				glog.Warningf("Invalid EC config in VolumeInfo for volume %d (data=%d, parity=%d), using defaults", vid, ds, ps)
				ev.ECContext = NewDefaultECContext(collection, vid)
			} else {
				ev.ECContext = &ECContext{
					Collection:   collection,
					VolumeId:     vid,
					DataShards:   ds,
					ParityShards: ps,
				}
				glog.V(1).Infof("Loaded EC config from VolumeInfo for volume %d: %s", vid, ev.ECContext.String())
			}
		} else {
			ev.ECContext = NewDefaultECContext(collection, vid)
		}
	} else {
		glog.Warningf("vif file not found,volumeId:%d, filename:%s", vid, dataBaseFileName)
		volume_info.SaveVolumeInfo(dataBaseFileName+".vif", &volume_server_pb.VolumeInfo{Version: uint32(ev.Version)})
		ev.ECContext = NewDefaultECContext(collection, vid)
	}

	ev.ShardLocations = make(map[ShardId][]pb.ServerAddress)

	ev.seedCountsFromEcx()

	return
}

// seedCountsFromEcx walks the .ecx index once at volume load to compute the
// initial file and delete counts. Errors are logged but non-fatal: counts
// simply stay at zero in that case, matching needle-map behaviour for
// regular volumes with corrupt indexes.
func (ev *EcVolume) seedCountsFromEcx() {
	if ev.ecxFile == nil {
		return
	}
	var total, tombstoned uint64
	if err := ev.WalkIndex(func(key types.NeedleId, offset types.Offset, size types.Size) error {
		total++
		if size.IsDeleted() {
			tombstoned++
		}
		return nil
	}); err != nil {
		glog.Warningf("ec volume %d: seed counts from .ecx: %v", ev.VolumeId, err)
		return
	}
	ev.countsLock.Lock()
	ev.fileCount = total
	ev.deleteCount = tombstoned
	ev.countsLock.Unlock()
}

func (ev *EcVolume) AddEcVolumeShard(ecVolumeShard *EcVolumeShard) bool {
	for _, s := range ev.Shards {
		if s.ShardId == ecVolumeShard.ShardId {
			return false
		}
	}
	ev.Shards = append(ev.Shards, ecVolumeShard)
	slices.SortFunc(ev.Shards, func(a, b *EcVolumeShard) int {
		if a.VolumeId != b.VolumeId {
			return int(a.VolumeId - b.VolumeId)
		}
		return int(a.ShardId - b.ShardId)
	})
	return true
}

func (ev *EcVolume) DeleteEcVolumeShard(shardId ShardId) (ecVolumeShard *EcVolumeShard, deleted bool) {
	foundPosition := -1
	for i, s := range ev.Shards {
		if s.ShardId == shardId {
			foundPosition = i
		}
	}
	if foundPosition < 0 {
		return nil, false
	}

	ecVolumeShard = ev.Shards[foundPosition]
	ecVolumeShard.Unmount()
	ev.Shards = append(ev.Shards[:foundPosition], ev.Shards[foundPosition+1:]...)
	return ecVolumeShard, true
}

func (ev *EcVolume) FindEcVolumeShard(shardId ShardId) (ecVolumeShard *EcVolumeShard, found bool) {
	for _, s := range ev.Shards {
		if s.ShardId == shardId {
			return s, true
		}
	}
	return nil, false
}

func (ev *EcVolume) Close() {
	for _, s := range ev.Shards {
		s.Close()
	}
	if ev.ecjFile != nil {
		ev.ecjFileAccessLock.Lock()
		_ = ev.ecjFile.Close()
		ev.ecjFile = nil
		ev.ecjFileAccessLock.Unlock()
	}
	if ev.ecxFile != nil {
		_ = ev.ecxFile.Sync()
		_ = ev.ecxFile.Close()
		ev.ecxFile = nil
	}
}

// Sync flushes the .ecx and .ecj files to disk without closing them.
// This ensures that deletions made via DeleteNeedleFromEcx are visible
// to other processes/file handles that may read these files.
func (ev *EcVolume) Sync() {
	if ev.ecjFile != nil {
		ev.ecjFileAccessLock.Lock()
		if err := ev.ecjFile.Sync(); err != nil {
			glog.Warningf("failed to sync ecj file for volume %d: %v", ev.VolumeId, err)
		}
		ev.ecjFileAccessLock.Unlock()
	}
	if ev.ecxFile != nil {
		if err := ev.ecxFile.Sync(); err != nil {
			glog.Warningf("failed to sync ecx file for volume %d: %v", ev.VolumeId, err)
		}
	}
}

func (ev *EcVolume) Destroy() {
	ev.Close()

	for _, s := range ev.Shards {
		s.Destroy()
	}
	os.Remove(ev.FileName(".ecx"))
	os.Remove(ev.FileName(".ecj"))
	os.Remove(ev.FileName(".vif"))
}

func (ev *EcVolume) FileName(ext string) string {
	switch ext {
	case ".ecx", ".ecj":
		return EcShardFileName(ev.Collection, ev.ecxActualDir, int(ev.VolumeId)) + ext
	}
	// .vif
	return ev.DataBaseFileName() + ext
}

func (ev *EcVolume) DataBaseFileName() string {
	return EcShardFileName(ev.Collection, ev.dir, int(ev.VolumeId))
}

func (ev *EcVolume) IndexBaseFileName() string {
	return EcShardFileName(ev.Collection, ev.dirIdx, int(ev.VolumeId))
}

func (ev *EcVolume) ShardSize() uint64 {
	if len(ev.Shards) > 0 {
		return uint64(ev.Shards[0].Size())
	}
	return 0
}

func (ev *EcVolume) Size() (size uint64) {
	for _, shard := range ev.Shards {
		if shardSize := shard.Size(); shardSize > 0 {
			size += uint64(shardSize)
		}
	}
	return
}

func (ev *EcVolume) CreatedAt() time.Time {
	return ev.ecxCreatedAt
}

func (ev *EcVolume) ShardIdList() (shardIds []ShardId) {
	for _, s := range ev.Shards {
		shardIds = append(shardIds, s.ShardId)
	}
	return
}

func (ev *EcVolume) ToVolumeEcShardInformationMessage(diskId uint32) (messages []*master_pb.VolumeEcShardInformationMessage) {
	ecInfoPerVolume := map[needle.VolumeId]*master_pb.VolumeEcShardInformationMessage{}

	fileCount, deleteCount := ev.FileAndDeleteCount()

	for _, s := range ev.Shards {
		m, ok := ecInfoPerVolume[s.VolumeId]
		if !ok {
			m = &master_pb.VolumeEcShardInformationMessage{
				Id:          uint32(s.VolumeId),
				Collection:  s.Collection,
				DiskType:    string(ev.diskType),
				ExpireAtSec: ev.ExpireAtSec,
				DiskId:      diskId,
				FileCount:   fileCount,
				DeleteCount: deleteCount,
			}
			ecInfoPerVolume[s.VolumeId] = m
		}

		// Update EC shard bits and sizes.
		si := ShardsInfoFromVolumeEcShardInformationMessage(m)
		si.Set(NewShardInfo(s.ShardId, ShardSize(s.Size())))
		m.EcIndexBits = uint32(si.Bitmap())
		m.ShardSizes = si.SizesInt64()
	}

	for _, m := range ecInfoPerVolume {
		messages = append(messages, m)
	}
	return
}

// FileAndDeleteCount returns the current (fileCount, deleteCount) for this
// EC volume. fileCount is the total number of needles recorded in .ecx
// (live + tombstoned), matching the semantics of normal volume FileCount.
// deleteCount is the number of tombstones in this node's local .ecx copy;
// because a needle delete is applied on exactly one shard holder (see
// doDeleteNeedleFromRemoteEcShardServers), the admin aggregation sums
// deleteCount across nodes to get the volume's true delete total.
func (ev *EcVolume) FileAndDeleteCount() (fileCount, deleteCount uint64) {
	ev.countsLock.Lock()
	defer ev.countsLock.Unlock()
	return ev.fileCount, ev.deleteCount
}

// recordDelete bumps the delete counter when DeleteNeedleFromEcx has just
// transitioned a live entry to a tombstone. Idempotent deletes (oldSize was
// already a tombstone) must not drift the counter.
func (ev *EcVolume) recordDelete(wasLive bool) {
	if !wasLive {
		return
	}
	ev.countsLock.Lock()
	ev.deleteCount++
	ev.countsLock.Unlock()
}

func (ev *EcVolume) LocateEcShardNeedle(needleId types.NeedleId, version needle.Version) (offset types.Offset, size types.Size, intervals []Interval, err error) {

	// find the needle from ecx file
	offset, size, err = ev.FindNeedleFromEcx(needleId)
	if err != nil {
		return types.Offset{}, 0, nil, fmt.Errorf("FindNeedleFromEcx: %w", err)
	}

	intervals = ev.LocateEcShardNeedleInterval(version, offset.ToActualOffset(), types.Size(needle.GetActualSize(size, version)))
	return
}

func (ev *EcVolume) LocateEcShardNeedleInterval(version needle.Version, offset int64, size types.Size) (intervals []Interval) {
	shard := ev.Shards[0]
	var shardSize int64
	if ev.datFileSize > 0 {
		// Use datFileSize to calculate the shardSize to match the EC encoding logic.
		// This is the authoritative value stored in .vif during EC encoding.
		shardSize = ev.datFileSize / int64(ev.ECContext.DataShards)
	} else {
		// Fallback for old EC volumes without datFileSize in .vif.
		// Subtract 1 to handle the ambiguous case where ecdFileSize is an exact
		// multiple of ErasureCodingLargeBlockSize but the data is actually in small
		// blocks (e.g., datFileSize was just under DataShards*ErasureCodingLargeBlockSize).
		shardSize = shard.ecdFileSize - 1
	}
	// calculate the locations in the ec shards
	intervals = LocateData(ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize, shardSize, offset, types.Size(needle.GetActualSize(size, version)))

	return
}

func (ev *EcVolume) FindNeedleFromEcx(needleId types.NeedleId) (offset types.Offset, size types.Size, err error) {
	return SearchNeedleFromSortedIndex(ev.ecxFile, ev.ecxFileSize, needleId, nil)
}

func SearchNeedleFromSortedIndex(ecxFile *os.File, ecxFileSize int64, needleId types.NeedleId, processNeedleFn func(file *os.File, offset int64) error) (offset types.Offset, size types.Size, err error) {
	var key types.NeedleId
	buf := make([]byte, types.NeedleMapEntrySize)
	l, h := int64(0), ecxFileSize/types.NeedleMapEntrySize
	for l < h {
		m := (l + h) / 2
		if n, err := ecxFile.ReadAt(buf, m*types.NeedleMapEntrySize); err != nil {
			if n != types.NeedleMapEntrySize {
				return types.Offset{}, types.TombstoneFileSize, fmt.Errorf("ecx file %d read at %d: %v", ecxFileSize, m*types.NeedleMapEntrySize, err)
			}
		}
		key, offset, size = idx.IdxFileEntry(buf)
		if key == needleId {
			if processNeedleFn != nil {
				err = processNeedleFn(ecxFile, m*types.NeedleMapEntrySize)
			}
			return
		}
		if key < needleId {
			l = m + 1
		} else {
			h = m
		}
	}

	err = NotFoundError
	return
}

func (ev *EcVolume) IsTimeToDestroy() bool {
	return ev.ExpireAtSec > 0 && time.Now().Unix() > (int64(ev.ExpireAtSec)+destroyDelaySeconds)
}

func (ev *EcVolume) WalkIndex(processNeedleFn func(key types.NeedleId, offset types.Offset, size types.Size) error) error {
	if ev.ecxFile == nil {
		return fmt.Errorf("no ECX file associated with EC volume %v", ev.VolumeId)
	}
	return idx.WalkIndexFile(ev.ecxFile, 0, processNeedleFn)
}
