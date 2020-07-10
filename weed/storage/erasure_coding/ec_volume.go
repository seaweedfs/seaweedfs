package erasure_coding

import (
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/idx"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
)

var (
	NotFoundError = errors.New("needle not found")
)

type EcVolume struct {
	VolumeId                  needle.VolumeId
	Collection                string
	dir                       string
	ecxFile                   *os.File
	ecxFileSize               int64
	ecxCreatedAt              time.Time
	Shards                    []*EcVolumeShard
	ShardLocations            map[ShardId][]string
	ShardLocationsRefreshTime time.Time
	ShardLocationsLock        sync.RWMutex
	Version                   needle.Version
	ecjFile                   *os.File
	ecjFileAccessLock         sync.Mutex
}

func NewEcVolume(dir string, collection string, vid needle.VolumeId) (ev *EcVolume, err error) {
	ev = &EcVolume{dir: dir, Collection: collection, VolumeId: vid}

	baseFileName := EcShardFileName(collection, dir, int(vid))

	// open ecx file
	if ev.ecxFile, err = os.OpenFile(baseFileName+".ecx", os.O_RDWR, 0644); err != nil {
		return nil, fmt.Errorf("cannot open ec volume index %s.ecx: %v", baseFileName, err)
	}
	ecxFi, statErr := ev.ecxFile.Stat()
	if statErr != nil {
		return nil, fmt.Errorf("can not stat ec volume index %s.ecx: %v", baseFileName, statErr)
	}
	ev.ecxFileSize = ecxFi.Size()
	ev.ecxCreatedAt = ecxFi.ModTime()

	// open ecj file
	if ev.ecjFile, err = os.OpenFile(baseFileName+".ecj", os.O_RDWR|os.O_CREATE, 0644); err != nil {
		return nil, fmt.Errorf("cannot open ec volume journal %s.ecj: %v", baseFileName, err)
	}

	// read volume info
	ev.Version = needle.Version3
	if volumeInfo, found, _ := pb.MaybeLoadVolumeInfo(baseFileName + ".vif"); found {
		ev.Version = needle.Version(volumeInfo.Version)
	} else {
		pb.SaveVolumeInfo(baseFileName+".vif", &volume_server_pb.VolumeInfo{Version: uint32(ev.Version)})
	}

	ev.ShardLocations = make(map[ShardId][]string)

	return
}

func (ev *EcVolume) AddEcVolumeShard(ecVolumeShard *EcVolumeShard) bool {
	for _, s := range ev.Shards {
		if s.ShardId == ecVolumeShard.ShardId {
			return false
		}
	}
	ev.Shards = append(ev.Shards, ecVolumeShard)
	sort.Slice(ev.Shards, func(i, j int) bool {
		return ev.Shards[i].VolumeId < ev.Shards[j].VolumeId ||
			ev.Shards[i].VolumeId == ev.Shards[j].VolumeId && ev.Shards[i].ShardId < ev.Shards[j].ShardId
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
		_ = ev.ecxFile.Close()
		ev.ecxFile = nil
	}
}

func (ev *EcVolume) Destroy() {

	ev.Close()

	for _, s := range ev.Shards {
		s.Destroy()
	}
	os.Remove(ev.FileName() + ".ecx")
	os.Remove(ev.FileName() + ".ecj")
	os.Remove(ev.FileName() + ".vif")
}

func (ev *EcVolume) FileName() string {

	return EcShardFileName(ev.Collection, ev.dir, int(ev.VolumeId))

}

func (ev *EcVolume) ShardSize() int64 {
	if len(ev.Shards) > 0 {
		return ev.Shards[0].Size()
	}
	return 0
}

func (ev *EcVolume) Size() (size int64) {
	for _, shard := range ev.Shards {
		size += shard.Size()
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

func (ev *EcVolume) ToVolumeEcShardInformationMessage() (messages []*master_pb.VolumeEcShardInformationMessage) {
	prevVolumeId := needle.VolumeId(math.MaxUint32)
	var m *master_pb.VolumeEcShardInformationMessage
	for _, s := range ev.Shards {
		if s.VolumeId != prevVolumeId {
			m = &master_pb.VolumeEcShardInformationMessage{
				Id:         uint32(s.VolumeId),
				Collection: s.Collection,
			}
			messages = append(messages, m)
		}
		prevVolumeId = s.VolumeId
		m.EcIndexBits = uint32(ShardBits(m.EcIndexBits).AddShardId(s.ShardId))
	}
	return
}

func (ev *EcVolume) LocateEcShardNeedle(needleId types.NeedleId, version needle.Version) (offset types.Offset, size uint32, intervals []Interval, err error) {

	// find the needle from ecx file
	offset, size, err = ev.FindNeedleFromEcx(needleId)
	if err != nil {
		return types.Offset{}, 0, nil, fmt.Errorf("FindNeedleFromEcx: %v", err)
	}

	shard := ev.Shards[0]

	// calculate the locations in the ec shards
	intervals = LocateData(ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize, DataShardsCount*shard.ecdFileSize, offset.ToAcutalOffset(), uint32(needle.GetActualSize(size, version)))

	return
}

func (ev *EcVolume) FindNeedleFromEcx(needleId types.NeedleId) (offset types.Offset, size uint32, err error) {
	return SearchNeedleFromSortedIndex(ev.ecxFile, ev.ecxFileSize, needleId, nil)
}

func SearchNeedleFromSortedIndex(ecxFile *os.File, ecxFileSize int64, needleId types.NeedleId, processNeedleFn func(file *os.File, offset int64) error) (offset types.Offset, size uint32, err error) {
	var key types.NeedleId
	buf := make([]byte, types.NeedleMapEntrySize)
	l, h := int64(0), ecxFileSize/types.NeedleMapEntrySize
	for l < h {
		m := (l + h) / 2
		if _, err := ecxFile.ReadAt(buf, m*types.NeedleMapEntrySize); err != nil {
			return types.Offset{}, types.TombstoneFileSize, fmt.Errorf("ecx file %d read at %d: %v", ecxFileSize, m*types.NeedleMapEntrySize, err)
		}
		key, offset, size = idx.IdxFileEntry(buf)
		if key == needleId {
			if processNeedleFn != nil {
				err = processNeedleFn(ecxFile, m*types.NeedleHeaderSize)
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
