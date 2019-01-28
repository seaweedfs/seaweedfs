package util

import "hash/fnv"

func HashBytesToInt64(x []byte) int64 {
	hash := fnv.New64()
	hash.Write(x)
	return int64(hash.Sum64())
}
