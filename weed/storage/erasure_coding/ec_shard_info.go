package erasure_coding

// ShardSize represents the size of a shard in bytes
type ShardSize int64

// ShardInfo holds information about a single shard
type ShardInfo struct {
	Id   ShardId
	Size ShardSize
}
