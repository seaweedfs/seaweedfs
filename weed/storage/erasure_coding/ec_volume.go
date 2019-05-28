package erasure_coding

import (
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/idx"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
)

type EcVolume struct {
	VolumeId                  needle.VolumeId
	Collection                string
	dir                       string
	ecxFile                   *os.File
	ecxFileSize               int64
	Shards                    []*EcVolumeShard
	ShardLocations            map[ShardId][]string
	ShardLocationsRefreshTime time.Time
	ShardLocationsLock        sync.RWMutex
}

func NewEcVolume(dir string, collection string, vid needle.VolumeId) (ev *EcVolume, err error) {
	ev = &EcVolume{dir: dir, Collection: collection, VolumeId: vid}

	baseFileName := EcShardFileName(collection, dir, int(vid))

	// open ecx file
	if ev.ecxFile, err = os.OpenFile(baseFileName+".ecx", os.O_RDONLY, 0644); err != nil {
		return nil, fmt.Errorf("cannot read ec volume index %s.ecx: %v", baseFileName, err)
	}
	ecxFi, statErr := ev.ecxFile.Stat()
	if statErr != nil {
		return nil, fmt.Errorf("can not stat ec volume index %s.ecx: %v", baseFileName, statErr)
	}
	ev.ecxFileSize = ecxFi.Size()

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

func (ev *EcVolume) DeleteEcVolumeShard(shardId ShardId) bool {
	foundPosition := -1
	for i, s := range ev.Shards {
		if s.ShardId == shardId {
			foundPosition = i
		}
	}
	if foundPosition < 0 {
		return false
	}

	ev.Shards = append(ev.Shards[:foundPosition], ev.Shards[foundPosition+1:]...)
	return true
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
	if ev.ecxFile != nil {
		_ = ev.ecxFile.Close()
		ev.ecxFile = nil
	}
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

func (ev *EcVolume) LocateEcShardNeedle(n *needle.Needle) (offset types.Offset, size uint32, intervals []Interval, err error) {

	// find the needle from ecx file
	offset, size, err = ev.findNeedleFromEcx(n.Id)
	if err != nil {
		return types.Offset{}, 0, nil, err
	}

	shard := ev.Shards[0]

	// calculate the locations in the ec shards
	intervals = LocateData(ErasureCodingLargeBlockSize, ErasureCodingSmallBlockSize, DataShardsCount*shard.ecdFileSize, offset.ToAcutalOffset(), size)

	return
}

func (ev *EcVolume) findNeedleFromEcx(needleId types.NeedleId) (offset types.Offset, size uint32, err error) {
	var key types.NeedleId
	buf := make([]byte, types.NeedleMapEntrySize)
	l, h := int64(0), ev.ecxFileSize/types.NeedleMapEntrySize
	for l < h {
		m := (l + h) / 2
		if _, err := ev.ecxFile.ReadAt(buf, m*types.NeedleMapEntrySize); err != nil {
			return types.Offset{}, 0, err
		}
		key, offset, size = idx.IdxFileEntry(buf)
		if key == needleId {
			return
		}
		if key < needleId {
			l = m + 1
		} else {
			h = m
		}
	}

	err = fmt.Errorf("needle id %d not found", needleId)
	return
}
