package s3lifecycle

import (
	"crypto/sha256"
	"hash"
	"sync"
)

// ShardCount partitions the (bucket, key) keyspace for the per-shard
// lifecycle reader. Workers receive one READ task per owned shard;
// each shard's cursor advances independently.
const ShardCount = 16

// shardHashPool reuses sha256 hashers — ShardID is in the per-event hot
// path and a fresh allocation per call shows up under load.
var shardHashPool = sync.Pool{
	New: func() interface{} { return sha256.New() },
}

// ShardID maps (bucket, key) to a shard in [0, ShardCount). Stable across
// restarts and across processes — identical (bucket, key) always lands on
// the same shard. Implementation: top 4 bits of sha256(bucket || "/" || key).
func ShardID(bucket, key string) int {
	h := shardHashPool.Get().(hash.Hash)
	h.Reset()
	h.Write([]byte(bucket))
	h.Write([]byte{'/'})
	h.Write([]byte(key))
	var buf [sha256.Size]byte
	sum := h.Sum(buf[:0])
	shardHashPool.Put(h)
	return int(sum[0] >> 4)
}
