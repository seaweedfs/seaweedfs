package erasure_coding

import (
	"math/bits"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// data structure used in master
type EcVolumeInfo struct {
	VolumeId    needle.VolumeId
	Collection  string
	ShardBits   ShardBits
	DiskType    string
	DiskId      uint32  // ID of the disk this EC volume is on
	ExpireAtSec uint64  // ec volume destroy time, calculated from the ec volume was created
	ShardSizes  []int64 // optimized: sizes for shards in order of set bits in ShardBits
}

func (ecInfo *EcVolumeInfo) AddShardId(id ShardId) {
	oldBits := ecInfo.ShardBits
	ecInfo.ShardBits = ecInfo.ShardBits.AddShardId(id)

	// If shard was actually added, resize ShardSizes array
	if oldBits != ecInfo.ShardBits {
		ecInfo.resizeShardSizes(oldBits)
	}
}

func (ecInfo *EcVolumeInfo) RemoveShardId(id ShardId) {
	oldBits := ecInfo.ShardBits
	ecInfo.ShardBits = ecInfo.ShardBits.RemoveShardId(id)

	// If shard was actually removed, resize ShardSizes array
	if oldBits != ecInfo.ShardBits {
		ecInfo.resizeShardSizes(oldBits)
	}
}

func (ecInfo *EcVolumeInfo) SetShardSize(id ShardId, size int64) {
	ecInfo.ensureShardSizesInitialized()
	if index, found := ecInfo.ShardBits.ShardIdToIndex(id); found && index < len(ecInfo.ShardSizes) {
		ecInfo.ShardSizes[index] = size
	}
}

func (ecInfo *EcVolumeInfo) GetShardSize(id ShardId) (int64, bool) {
	if index, found := ecInfo.ShardBits.ShardIdToIndex(id); found && index < len(ecInfo.ShardSizes) {
		return ecInfo.ShardSizes[index], true
	}
	return 0, false
}

func (ecInfo *EcVolumeInfo) GetTotalSize() int64 {
	var total int64
	for _, size := range ecInfo.ShardSizes {
		total += size
	}
	return total
}

func (ecInfo *EcVolumeInfo) HasShardId(id ShardId) bool {
	return ecInfo.ShardBits.HasShardId(id)
}

func (ecInfo *EcVolumeInfo) ShardIds() (ret []ShardId) {
	return ecInfo.ShardBits.ShardIds()
}

func (ecInfo *EcVolumeInfo) ShardIdCount() (count int) {
	return ecInfo.ShardBits.ShardIdCount()
}

func (ecInfo *EcVolumeInfo) Minus(other *EcVolumeInfo) *EcVolumeInfo {
	ret := &EcVolumeInfo{
		VolumeId:    ecInfo.VolumeId,
		Collection:  ecInfo.Collection,
		ShardBits:   ecInfo.ShardBits.Minus(other.ShardBits),
		DiskType:    ecInfo.DiskType,
		DiskId:      ecInfo.DiskId,
		ExpireAtSec: ecInfo.ExpireAtSec,
	}

	// Initialize optimized ShardSizes for the result
	ret.ensureShardSizesInitialized()

	// Copy shard sizes for remaining shards
	retIndex := 0
	for shardId := ShardId(0); shardId < ShardId(MaxShardCount) && retIndex < len(ret.ShardSizes); shardId++ {
		if ret.ShardBits.HasShardId(shardId) {
			if size, exists := ecInfo.GetShardSize(shardId); exists {
				ret.ShardSizes[retIndex] = size
			}
			retIndex++
		}
	}

	return ret
}

func (ecInfo *EcVolumeInfo) ToVolumeEcShardInformationMessage() (ret *master_pb.VolumeEcShardInformationMessage) {
	t := &master_pb.VolumeEcShardInformationMessage{
		Id:          uint32(ecInfo.VolumeId),
		EcIndexBits: uint32(ecInfo.ShardBits),
		Collection:  ecInfo.Collection,
		DiskType:    ecInfo.DiskType,
		ExpireAtSec: ecInfo.ExpireAtSec,
		DiskId:      ecInfo.DiskId,
	}

	// Directly set the optimized ShardSizes
	t.ShardSizes = make([]int64, len(ecInfo.ShardSizes))
	copy(t.ShardSizes, ecInfo.ShardSizes)

	return t
}

type ShardBits uint32 // use bits to indicate the shard id, use 32 bits just for possible future extension

func (b ShardBits) AddShardId(id ShardId) ShardBits {
	if id >= MaxShardCount {
		return b // Reject out-of-range shard IDs
	}
	return b | (1 << id)
}

func (b ShardBits) RemoveShardId(id ShardId) ShardBits {
	if id >= MaxShardCount {
		return b // Reject out-of-range shard IDs
	}
	return b &^ (1 << id)
}

func (b ShardBits) HasShardId(id ShardId) bool {
	if id >= MaxShardCount {
		return false // Out-of-range shard IDs are never present
	}
	return b&(1<<id) > 0
}

func (b ShardBits) ShardIds() (ret []ShardId) {
	for i := ShardId(0); i < ShardId(MaxShardCount); i++ {
		if b.HasShardId(i) {
			ret = append(ret, i)
		}
	}
	return
}

func (b ShardBits) ToUint32Slice() (ret []uint32) {
	for i := uint32(0); i < uint32(MaxShardCount); i++ {
		if b.HasShardId(ShardId(i)) {
			ret = append(ret, i)
		}
	}
	return
}

func (b ShardBits) ShardIdCount() (count int) {
	for count = 0; b > 0; count++ {
		b &= b - 1
	}
	return
}

func (b ShardBits) Minus(other ShardBits) ShardBits {
	return b &^ other
}

func (b ShardBits) Plus(other ShardBits) ShardBits {
	return b | other
}

func (b ShardBits) MinusParityShards() ShardBits {
	// Removes parity shards from the bit mask
	// Assumes default 10+4 EC layout where parity shards are IDs 10-13
	for i := DataShardsCount; i < TotalShardsCount; i++ {
		b = b.RemoveShardId(ShardId(i))
	}
	return b
}

// ShardIdToIndex converts a shard ID to its index position in the ShardSizes slice
// Returns the index and true if the shard is present, -1 and false if not present
func (b ShardBits) ShardIdToIndex(shardId ShardId) (index int, found bool) {
	if !b.HasShardId(shardId) {
		return -1, false
	}

	// Create a mask for bits before the shardId
	mask := uint32((1 << shardId) - 1)
	// Count set bits before the shardId using efficient bit manipulation
	index = bits.OnesCount32(uint32(b) & mask)
	return index, true
}

// EachSetIndex iterates over all set shard IDs and calls the provided function for each
// This is highly efficient using bit manipulation - only iterates over actual set bits
func (b ShardBits) EachSetIndex(fn func(shardId ShardId)) {
	bitsValue := uint32(b)
	for bitsValue != 0 {
		// Find the position of the least significant set bit
		shardId := ShardId(bits.TrailingZeros32(bitsValue))
		fn(shardId)
		// Clear the least significant set bit
		bitsValue &= bitsValue - 1
	}
}

// IndexToShardId converts an index position in ShardSizes slice to the corresponding shard ID
// Returns the shard ID and true if valid index, -1 and false if invalid index
func (b ShardBits) IndexToShardId(index int) (shardId ShardId, found bool) {
	if index < 0 {
		return 0, false
	}

	currentIndex := 0
	for i := ShardId(0); i < ShardId(MaxShardCount); i++ {
		if b.HasShardId(i) {
			if currentIndex == index {
				return i, true
			}
			currentIndex++
		}
	}
	return 0, false // index out of range
}

// Helper methods for EcVolumeInfo to manage the optimized ShardSizes slice
func (ecInfo *EcVolumeInfo) ensureShardSizesInitialized() {
	expectedLength := ecInfo.ShardBits.ShardIdCount()
	if ecInfo.ShardSizes == nil {
		ecInfo.ShardSizes = make([]int64, expectedLength)
	} else if len(ecInfo.ShardSizes) != expectedLength {
		// Resize and preserve existing data
		ecInfo.resizeShardSizes(ecInfo.ShardBits)
	}
}

func (ecInfo *EcVolumeInfo) resizeShardSizes(prevShardBits ShardBits) {
	expectedLength := ecInfo.ShardBits.ShardIdCount()
	newSizes := make([]int64, expectedLength)

	// Copy existing sizes to new positions based on current ShardBits
	if len(ecInfo.ShardSizes) > 0 {
		newIndex := 0
		for shardId := ShardId(0); shardId < ShardId(MaxShardCount) && newIndex < expectedLength; shardId++ {
			if ecInfo.ShardBits.HasShardId(shardId) {
				// Try to find the size for this shard in the old array using previous ShardBits
				if oldIndex, found := prevShardBits.ShardIdToIndex(shardId); found && oldIndex < len(ecInfo.ShardSizes) {
					newSizes[newIndex] = ecInfo.ShardSizes[oldIndex]
				}
				newIndex++
			}
		}
	}

	ecInfo.ShardSizes = newSizes
}
