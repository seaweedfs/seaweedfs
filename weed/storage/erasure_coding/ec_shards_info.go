package erasure_coding

import (
	"fmt"
	"math/bits"
	"sort"
	"strings"
	"sync"

	"github.com/dustin/go-humanize"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// ShardBits is a bitmap representing which shards are present (bit 0 = shard 0, etc.)
type ShardBits uint32

// Has checks if a shard ID is present in the bitmap
func (sb ShardBits) Has(id ShardId) bool {
	return id < MaxShardCount && sb&(1<<id) != 0
}

// Set sets a shard ID in the bitmap
func (sb ShardBits) Set(id ShardId) ShardBits {
	if id >= MaxShardCount {
		return sb
	}
	return sb | (1 << id)
}

// Clear clears a shard ID from the bitmap
func (sb ShardBits) Clear(id ShardId) ShardBits {
	if id >= MaxShardCount {
		return sb
	}
	return sb &^ (1 << id)
}

// Count returns the number of set bits using popcount
func (sb ShardBits) Count() int {
	return bits.OnesCount32(uint32(sb))
}

// ShardsInfo encapsulates information for EC shards with memory-efficient storage
type ShardsInfo struct {
	mu        sync.RWMutex
	shards    []ShardInfo // Sorted by Id
	shardBits ShardBits
}

func NewShardsInfo() *ShardsInfo {
	return &ShardsInfo{
		shards: make([]ShardInfo, 0, TotalShardsCount),
	}
}

// Initializes a ShardsInfo from a ECVolume.
func ShardsInfoFromVolume(ev *EcVolume) *ShardsInfo {
	res := &ShardsInfo{
		shards: make([]ShardInfo, len(ev.Shards)),
	}
	// Build shards directly to avoid locking in Set() since res is not yet shared
	for i, s := range ev.Shards {
		res.shards[i] = NewShardInfo(s.ShardId, ShardSize(s.Size()))
		res.shardBits = res.shardBits.Set(s.ShardId)
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
	// Build shards directly to avoid locking in Set() since res is not yet shared
	newShards := make([]ShardInfo, 0, 8)
	for bitmap := vi.EcIndexBits; bitmap != 0; bitmap >>= 1 {
		if bitmap&1 != 0 {
			var size ShardSize
			if j < len(vi.ShardSizes) {
				size = ShardSize(vi.ShardSizes[j])
			}
			j++
			newShards = append(newShards, NewShardInfo(id, size))
		}
		id++
	}
	res.shards = newShards
	res.shardBits = ShardBits(vi.EcIndexBits)

	return res
}

// Returns a count of shards from a VolumeEcShardInformationMessage proto.
func GetShardCount(vi *master_pb.VolumeEcShardInformationMessage) int {
	if vi == nil {
		return 0
	}
	return ShardBits(vi.EcIndexBits).Count()
}

// Returns a string representation for a ShardsInfo.
func (sp *ShardsInfo) String() string {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	var sb strings.Builder
	for i, s := range sp.shards {
		if i > 0 {
			sb.WriteString(" ")
		}
		fmt.Fprintf(&sb, "%d:%s", s.Id, humanize.Bytes(uint64(s.Size)))
	}
	return sb.String()
}

// AsSlice converts a ShardsInfo to a slice of ShardInfo structs, ordered by shard ID.
func (si *ShardsInfo) AsSlice() []ShardInfo {
	si.mu.RLock()
	defer si.mu.RUnlock()
	res := make([]ShardInfo, len(si.shards))
	copy(res, si.shards)
	return res
}

// Count returns the number of EC shards using popcount on the bitmap.
func (si *ShardsInfo) Count() int {
	si.mu.RLock()
	defer si.mu.RUnlock()
	return si.shardBits.Count()
}

// Has verifies if a shard ID is present using bitmap check.
func (si *ShardsInfo) Has(id ShardId) bool {
	si.mu.RLock()
	defer si.mu.RUnlock()
	return si.shardBits.Has(id)
}

// Ids returns a list of shard IDs, in ascending order.
func (si *ShardsInfo) Ids() []ShardId {
	si.mu.RLock()
	defer si.mu.RUnlock()
	ids := make([]ShardId, len(si.shards))
	for i, s := range si.shards {
		ids[i] = s.Id
	}
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

// IdsUint32 returns a list of shards ID as uint32, in ascending order.
func (si *ShardsInfo) IdsUint32() []uint32 {
	return ShardIdsToUint32(si.Ids())
}

// Set sets or updates a shard's information.
func (si *ShardsInfo) Set(shard ShardInfo) {
	if shard.Id >= MaxShardCount {
		return
	}
	si.mu.Lock()
	defer si.mu.Unlock()

	// Check if already exists
	if si.shardBits.Has(shard.Id) {
		// Find and update
		idx := si.findIndex(shard.Id)
		if idx >= 0 {
			si.shards[idx] = shard
		}
		return
	}

	// Add new shard
	si.shardBits = si.shardBits.Set(shard.Id)

	// Find insertion point to keep sorted
	idx := sort.Search(len(si.shards), func(i int) bool {
		return si.shards[i].Id > shard.Id
	})

	// Insert at idx
	si.shards = append(si.shards, ShardInfo{})
	copy(si.shards[idx+1:], si.shards[idx:])
	si.shards[idx] = shard
}

// Delete deletes a shard by ID.
func (si *ShardsInfo) Delete(id ShardId) {
	if id >= MaxShardCount {
		return
	}
	si.mu.Lock()
	defer si.mu.Unlock()

	if !si.shardBits.Has(id) {
		return // Not present
	}

	si.shardBits = si.shardBits.Clear(id)

	// Find and remove from slice
	idx := si.findIndex(id)
	if idx >= 0 {
		si.shards = append(si.shards[:idx], si.shards[idx+1:]...)
	}
}

// Bitmap returns a bitmap for all existing shard IDs.
func (si *ShardsInfo) Bitmap() uint32 {
	si.mu.RLock()
	defer si.mu.RUnlock()
	return uint32(si.shardBits)
}

// Size returns the size of a given shard ID, if present.
func (si *ShardsInfo) Size(id ShardId) ShardSize {
	if id >= MaxShardCount {
		return 0
	}
	si.mu.RLock()
	defer si.mu.RUnlock()

	if !si.shardBits.Has(id) {
		return 0
	}

	idx := si.findIndex(id)
	if idx >= 0 {
		return si.shards[idx].Size
	}
	return 0
}

// TotalSize returns the size for all shards.
func (si *ShardsInfo) TotalSize() ShardSize {
	si.mu.RLock()
	defer si.mu.RUnlock()
	var total ShardSize
	for _, s := range si.shards {
		total += s.Size
	}
	return total
}

// Sizes returns a compact slice of present shard sizes, from first to last.
func (si *ShardsInfo) Sizes() []ShardSize {
	si.mu.RLock()
	defer si.mu.RUnlock()

	res := make([]ShardSize, len(si.shards))
	for i, s := range si.shards {
		res[i] = s.Size
	}
	return res
}

// SizesInt64 returns a compact slice of present shard sizes, from first to last, as int64.
func (si *ShardsInfo) SizesInt64() []int64 {
	sizes := si.Sizes()
	res := make([]int64, len(sizes))
	for i, s := range sizes {
		res[i] = int64(s)
	}
	return res
}

// Copy creates a copy of a ShardInfo.
func (si *ShardsInfo) Copy() *ShardsInfo {
	si.mu.RLock()
	defer si.mu.RUnlock()

	newShards := make([]ShardInfo, len(si.shards))
	copy(newShards, si.shards)

	return &ShardsInfo{
		shards:    newShards,
		shardBits: si.shardBits,
	}
}

// DeleteParityShards removes parity shards from a ShardInfo.
func (si *ShardsInfo) DeleteParityShards() {
	for id := DataShardsCount; id < TotalShardsCount; id++ {
		si.Delete(ShardId(id))
	}
}

// MinusParityShards creates a ShardInfo copy, but with parity shards removed.
func (si *ShardsInfo) MinusParityShards() *ShardsInfo {
	result := si.Copy()
	result.DeleteParityShards()
	return result
}

// Add merges all shards from another ShardInfo into this one.
func (si *ShardsInfo) Add(other *ShardsInfo) {
	other.mu.RLock()
	// Copy shards to avoid holding lock on 'other' while calling si.Set, which could deadlock.
	shardsToAdd := make([]ShardInfo, len(other.shards))
	copy(shardsToAdd, other.shards)
	other.mu.RUnlock()

	for _, s := range shardsToAdd {
		si.Set(s)
	}
}

// Subtract removes all shards present on another ShardInfo.
func (si *ShardsInfo) Subtract(other *ShardsInfo) {
	other.mu.RLock()
	// Copy shards to avoid holding lock on 'other' while calling si.Delete, which could deadlock.
	shardsToRemove := make([]ShardInfo, len(other.shards))
	copy(shardsToRemove, other.shards)
	other.mu.RUnlock()

	for _, s := range shardsToRemove {
		si.Delete(s.Id)
	}
}

// Plus returns a new ShardInfo consisting of (this + other).
func (si *ShardsInfo) Plus(other *ShardsInfo) *ShardsInfo {
	result := si.Copy()
	result.Add(other)
	return result
}

// Minus returns a new ShardInfo consisting of (this - other).
func (si *ShardsInfo) Minus(other *ShardsInfo) *ShardsInfo {
	result := si.Copy()
	result.Subtract(other)
	return result
}

// findIndex finds the index of a shard by ID using binary search.
// Must be called with lock held. Returns -1 if not found.
func (si *ShardsInfo) findIndex(id ShardId) int {
	idx := sort.Search(len(si.shards), func(i int) bool {
		return si.shards[i].Id >= id
	})
	if idx < len(si.shards) && si.shards[idx].Id == id {
		return idx
	}
	return -1
}
