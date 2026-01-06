package erasure_coding

// ShardSize represents the size of a shard in bytes
type ShardSize int64

// ShardInfo holds information about a single shard
type ShardInfo struct {
	Id   ShardId
	Size ShardSize
}

// NewShardInfo creates a new ShardInfo with the given ID and size
func NewShardInfo(id ShardId, size ShardSize) ShardInfo {
	return ShardInfo{
		Id:   id,
		Size: size,
	}
}

// IsValid checks if the shard info has a valid ID
func (si ShardInfo) IsValid() bool {
	return si.Id < MaxShardCount
}
