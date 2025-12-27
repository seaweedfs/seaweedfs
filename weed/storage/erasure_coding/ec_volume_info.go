package erasure_coding

import (
	"fmt"
	"sort"

	"github.com/dustin/go-humanize"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// ShardsInfo encapsulates information for EC shards
type ShardSize int64
type ShardInfo struct {
	Id   ShardId
	Size ShardSize
}
type ShardsInfo struct {
	shards map[ShardId]*ShardInfo
}

func NewShardsInfo() *ShardsInfo {
	return &ShardsInfo{
		shards: map[ShardId]*ShardInfo{},
	}
}

// Initializes a ShardsInfo from a ECVolume.
func ShardsInfoFromVolume(ev *EcVolume) *ShardsInfo {
	res := &ShardsInfo{
		shards: map[ShardId]*ShardInfo{},
	}
	for _, s := range ev.Shards {
		res.Set(s.ShardId, ShardSize(s.Size()))
	}
	return res
}

// Initializes a ShardsInfo from a VolumeEcShardInformationMessage proto.
func ShardsInfoFromVolumeEcShardInformationMessage(vi *master_pb.VolumeEcShardInformationMessage) *ShardsInfo {
	res := NewShardsInfo()
	if vi == nil {
		return res
	}

	var id ShardId
	var j int
	for bitmap := vi.EcIndexBits; bitmap != 0; bitmap >>= 1 {
		if bitmap&1 != 0 {
			var size ShardSize
			if j < len(vi.ShardSizes) {
				size = ShardSize(vi.ShardSizes[j])
			}
			j++
			res.shards[id] = &ShardInfo{
				Id:   id,
				Size: size,
			}
		}
		id++
	}

	return res
}

// Returns a count of shards from a VolumeEcShardInformationMessage proto.
func ShardsCountFromVolumeEcShardInformationMessage(vi *master_pb.VolumeEcShardInformationMessage) int {
	if vi == nil {
		return 0
	}

	return ShardsInfoFromVolumeEcShardInformationMessage(vi).Count()
}

// Returns a string representation for a ShardsInfo.
func (sp *ShardsInfo) String() string {
	var res string
	ids := sp.Ids()
	for i, id := range sp.Ids() {
		res += fmt.Sprintf("%d:%s", id, humanize.Bytes(uint64(sp.shards[id].Size)))
		if i < len(ids)-1 {
			res += " "
		}
	}
	return res
}

// AsSlice converts a ShardsInfo to a slice of ShardInfo structs, ordered by shard ID.
func (si *ShardsInfo) AsSlice() []*ShardInfo {
	res := make([]*ShardInfo, len(si.shards))
	i := 0
	for _, id := range si.Ids() {
		res[i] = si.shards[id]
		i++
	}

	return res
}

// Count returns the number of EC shards.
func (si *ShardsInfo) Count() int {
	return len(si.shards)
}

// Has verifies if a shard ID is present.
func (si *ShardsInfo) Has(id ShardId) bool {
	_, ok := si.shards[id]
	return ok
}

// Ids returns a list of shard IDs, in ascending order.
func (si *ShardsInfo) Ids() []ShardId {
	ids := []ShardId{}
	for id := range si.shards {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	return ids
}

// IdsInt returns a list of shards ID as int, in ascending order.
func (si *ShardsInfo) IdsInt() []int {
	ids := si.Ids()
	res := make([]int, len(ids))
	for i, id := range ids {
		res[i] = int(id)
	}

	return res
}

// Ids returns a list of shards ID as uint32, in ascending order.
func (si *ShardsInfo) IdsUint32() []uint32 {
	return ShardIdsToUint32(si.Ids())
}

// Set sets the size for a given shard ID.
func (si *ShardsInfo) Set(id ShardId, size ShardSize) {
	if id >= MaxShardCount {
		return
	}
	si.shards[id] = &ShardInfo{
		Id:   id,
		Size: size,
	}
}

// Delete deletes a shard by ID.
func (si *ShardsInfo) Delete(id ShardId) {
	if id >= MaxShardCount {
		return
	}
	if _, ok := si.shards[id]; ok {
		delete(si.shards, id)
	}
}

// Bitmap returns a bitmap for all existing shard IDs (bit 0 = shard #0... bit 31 = shard #31), in little endian.
func (si *ShardsInfo) Bitmap() uint32 {
	var bits uint32
	for id := range si.shards {
		bits |= (1 << id)
	}
	return bits
}

// Size returns the size of a given shard ID, if present.
func (si *ShardsInfo) Size(id ShardId) ShardSize {
	if s, ok := si.shards[id]; ok {
		return s.Size
	}
	return 0
}

// TotalSize returns the size for all shards.
func (si *ShardsInfo) TotalSize() ShardSize {
	var total ShardSize
	for _, s := range si.shards {
		total += s.Size
	}
	return total
}

// Sizes returns a compact slice of present shard sizes, from first to last.
func (si *ShardsInfo) Sizes() []ShardSize {
	ids := si.Ids()

	res := make([]ShardSize, len(ids))
	if len(res) != 0 {
		var i int
		for _, id := range ids {
			res[i] = si.shards[id].Size
			i++
		}
	}

	return res
}

// SizesInt64 returns a compact slice of present shard sizes, from first to last, as int64.
func (si *ShardsInfo) SizesInt64() []int64 {
	res := make([]int64, si.Count())

	for i, s := range si.Sizes() {
		res[i] = int64(s)
	}
	return res
}

// Copy creates a copy of a ShardInfo.
func (si *ShardsInfo) Copy() *ShardsInfo {
	new := NewShardsInfo()
	for _, s := range si.shards {
		new.Set(s.Id, s.Size)
	}
	return new
}

// DeleteParityShards removes party shards from a ShardInfo.
// Assumes default 10+4 EC layout where parity shards are IDs 10-13.
func (si *ShardsInfo) DeleteParityShards() {
	for id := DataShardsCount; id < TotalShardsCount; id++ {
		si.Delete(ShardId(id))
	}
}

// MinusParityShards creates a ShardInfo copy, but with parity shards removed.
func (si *ShardsInfo) MinusParityShards() *ShardsInfo {
	new := si.Copy()
	new.DeleteParityShards()
	return new
}

// Add merges all shards from another ShardInfo into this one.
func (si *ShardsInfo) Add(other *ShardsInfo) {
	for _, s := range other.shards {
		si.Set(s.Id, s.Size)
	}
}

// Subtract removes all shards present on another ShardInfo.
func (si *ShardsInfo) Subtract(other *ShardsInfo) {
	for _, s := range other.shards {
		si.Delete(s.Id)
	}
}

// Plus returns a new ShardInfo consisting of (this + other).
func (si *ShardsInfo) Plus(other *ShardsInfo) *ShardsInfo {
	new := si.Copy()
	new.Add(other)
	return new
}

// Minus returns a new ShardInfo consisting of (this - other).
func (si *ShardsInfo) Minus(other *ShardsInfo) *ShardsInfo {
	new := si.Copy()
	new.Subtract(other)
	return new
}

// data structure used in master
type EcVolumeInfo struct {
	VolumeId    needle.VolumeId
	Collection  string
	DiskType    string
	DiskId      uint32 // ID of the disk this EC volume is on
	ExpireAtSec uint64 // ec volume destroy time, calculated from the ec volume was created
	ShardsInfo  *ShardsInfo
}

func (ecInfo *EcVolumeInfo) Minus(other *EcVolumeInfo) *EcVolumeInfo {
	return &EcVolumeInfo{
		VolumeId:    ecInfo.VolumeId,
		Collection:  ecInfo.Collection,
		ShardsInfo:  ecInfo.ShardsInfo.Minus(other.ShardsInfo),
		DiskType:    ecInfo.DiskType,
		DiskId:      ecInfo.DiskId,
		ExpireAtSec: ecInfo.ExpireAtSec,
	}
}

func (evi *EcVolumeInfo) ToVolumeEcShardInformationMessage() (ret *master_pb.VolumeEcShardInformationMessage) {
	return &master_pb.VolumeEcShardInformationMessage{
		Id:          uint32(evi.VolumeId),
		EcIndexBits: evi.ShardsInfo.Bitmap(),
		ShardSizes:  evi.ShardsInfo.SizesInt64(),
		Collection:  evi.Collection,
		DiskType:    evi.DiskType,
		ExpireAtSec: evi.ExpireAtSec,
		DiskId:      evi.DiskId,
	}
}
