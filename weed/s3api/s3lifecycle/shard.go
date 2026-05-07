package s3lifecycle

import "crypto/sha256"

// ShardCount partitions the (bucket, key) keyspace for the per-shard
// lifecycle reader. Workers receive one READ task per owned shard;
// each shard's cursor advances independently.
const ShardCount = 16

// ShardID maps (bucket, key) to a shard in [0, ShardCount). Stable across
// restarts and across processes — identical (bucket, key) always lands on
// the same shard. Implementation: top 4 bits of sha256(bucket || "/" || key).
func ShardID(bucket, key string) int {
	h := sha256.New()
	h.Write([]byte(bucket))
	h.Write([]byte{'/'})
	h.Write([]byte(key))
	sum := h.Sum(nil)
	return int(sum[0] >> 4)
}
