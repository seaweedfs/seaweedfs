package filer

import (
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Collection → Filer Path reverse index, shared by leveldb2/leveldb3 stores.
//
// Key:   \x01 'c' \x01 <collectionName> \x01 <filerFullPath>
// Value: (empty)
//
// Entry keys start with a 16-byte md5(dir) hash, so this prefix cannot
// collide with real entries in practice.

// ColIdxDeleteBatchSize is how many deletes are grouped per LevelDB batch
// during collection cleanup.
const ColIdxDeleteBatchSize = 1000

func ColIdxKey(collection string, filerPath util.FullPath) []byte {
	buf := make([]byte, 0, 4+len(collection)+len(filerPath))
	buf = append(buf, 1, 'c', 1)
	buf = append(buf, collection...)
	buf = append(buf, 1)
	buf = append(buf, filerPath...)
	return buf
}

func ColIdxPrefix(collection string) []byte {
	buf := make([]byte, 0, 4+len(collection))
	buf = append(buf, 1, 'c', 1)
	buf = append(buf, collection...)
	buf = append(buf, 1)
	return buf
}

// EntryCollection returns the collection recorded on the entry at write time,
// or "" if none.
func EntryCollection(entry *Entry) string {
	if entry == nil || entry.Extended == nil {
		return ""
	}
	return string(entry.Extended[ExtendedCollectionKey])
}
