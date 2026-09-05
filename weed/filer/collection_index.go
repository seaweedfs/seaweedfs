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

// ColIdxKey builds the reverse-index key. It deliberately does not pre-size
// the buffer from len(collection)+len(filerPath): that sum could overflow for
// adversarial input, so append is left to manage growth. These keys are short,
// so the extra reallocation cost is negligible.
func ColIdxKey(collection string, filerPath util.FullPath) []byte {
	var buf []byte
	buf = append(buf, 1, 'c', 1)
	buf = append(buf, collection...)
	buf = append(buf, 1)
	buf = append(buf, filerPath...)
	return buf
}

// ColIdxPrefix builds the prefix that iterates every index key of a collection.
// See ColIdxKey for why the buffer is not pre-sized.
func ColIdxPrefix(collection string) []byte {
	var buf []byte
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

// EntryCollectionFromBlob decodes only the attributes of an encoded entry blob
// (skipping chunk deserialization) and returns the collection recorded on it,
// or "" if none. Stores use this on delete/update paths where a full entry
// decode would be wasteful.
func EntryCollectionFromBlob(fullpath util.FullPath, blob []byte) (string, error) {
	entry := &Entry{FullPath: fullpath}
	if err := entry.DecodeAttributesOnly(util.MaybeDecompressData(blob)); err != nil {
		return "", err
	}
	return EntryCollection(entry), nil
}
