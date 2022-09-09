package arangodb

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"io"
	"strings"

	"github.com/arangodb/go-driver"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// convert a string into arango-key safe hex bytes hash
func hashString(dir string) string {
	h := md5.New()
	io.WriteString(h, dir)
	b := h.Sum(nil)
	return hex.EncodeToString(b)
}

// convert slice of bytes into slice of uint64
// the first uint64 indicates the length in bytes
func bytesToArray(bs []byte) []uint64 {
	out := make([]uint64, 0, 2+len(bs)/8)
	out = append(out, uint64(len(bs)))
	for len(bs)%8 != 0 {
		bs = append(bs, 0)
	}
	for i := 0; i < len(bs); i = i + 8 {
		out = append(out, binary.BigEndian.Uint64(bs[i:]))
	}
	return out
}

// convert from slice of uint64 back to bytes
// if input length is 0 or 1, will return nil
func arrayToBytes(xs []uint64) []byte {
	if len(xs) < 2 {
		return nil
	}
	first := xs[0]
	out := make([]byte, len(xs)*8) // i think this can actually be len(xs)*8-8, but i dont think an extra 8 bytes hurts...
	for i := 1; i < len(xs); i = i + 1 {
		binary.BigEndian.PutUint64(out[((i-1)*8):], xs[i])
	}
	return out[:first]
}

// gets the collection the bucket points to from filepath
func (store *ArangodbStore) extractBucketCollection(ctx context.Context, fullpath util.FullPath) (c driver.Collection, err error) {
	bucket, _ := extractBucket(fullpath)
	if bucket == "" {
		bucket = DEFAULT_COLLECTION
	}
	c, err = store.ensureBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return c, err
}

// called by extractBucketCollection
func extractBucket(fullpath util.FullPath) (string, string) {
	if !strings.HasPrefix(string(fullpath), BUCKET_PREFIX+"/") {
		return "", string(fullpath)
	}
	if strings.Count(string(fullpath), "/") < 3 {
		return "", string(fullpath)
	}
	bucketAndObjectKey := string(fullpath)[len(BUCKET_PREFIX+"/"):]
	t := strings.Index(bucketAndObjectKey, "/")
	bucket := bucketAndObjectKey
	shortPath := "/"
	if t > 0 {
		bucket = bucketAndObjectKey[:t]
		shortPath = string(util.FullPath(bucketAndObjectKey[t:]))
	}
	return bucket, shortPath
}

// get bucket collection from cache. if not exist, creates the buckets collection and grab it
func (store *ArangodbStore) ensureBucket(ctx context.Context, bucket string) (bc driver.Collection, err error) {
	var ok bool
	store.mu.RLock()
	bc, ok = store.buckets[bucket]
	store.mu.RUnlock()
	if ok && bc != nil {
		return bc, nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.buckets[bucket], err = store.ensureCollection(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return store.buckets[bucket], nil
}

// transform to an arango compliant name
func bucketToCollectionName(s string) string {
	if len(s) == 0 {
		return ""
	}
	// replace all "." with _
	s = strings.ReplaceAll(s, ".", "_")

	// if starts with number or '.' then add a special prefix
	if (s[0] >= '0' && s[0] <= '9') || (s[0] == '.' || s[0] == '_' || s[0] == '-') {
		s = "xN--" + s
	}
	return s
}

// creates collection if not exist, ensures indices if not exist
func (store *ArangodbStore) ensureCollection(ctx context.Context, bucket_name string) (c driver.Collection, err error) {
	// convert the bucket to collection name
	name := bucketToCollectionName(bucket_name)

	ok, err := store.database.CollectionExists(ctx, name)
	if err != nil {
		return
	}
	if ok {
		c, err = store.database.Collection(ctx, name)
	} else {
		c, err = store.database.CreateCollection(ctx, name, &driver.CreateCollectionOptions{})
	}
	if err != nil {
		return
	}
	// ensure indices
	if _, _, err = c.EnsurePersistentIndex(ctx, []string{"directory", "name"},
		&driver.EnsurePersistentIndexOptions{
			Name: "directory_name_multi", Unique: true,
		}); err != nil {
		return
	}
	if _, _, err = c.EnsurePersistentIndex(ctx, []string{"directory"},
		&driver.EnsurePersistentIndexOptions{Name: "IDX_directory"}); err != nil {
		return
	}
	if _, _, err = c.EnsureTTLIndex(ctx, "ttl", 1,
		&driver.EnsureTTLIndexOptions{Name: "IDX_TTL"}); err != nil {
		return
	}
	if _, _, err = c.EnsurePersistentIndex(ctx, []string{"name"}, &driver.EnsurePersistentIndexOptions{
		Name: "IDX_name",
	}); err != nil {
		return
	}
	return c, nil
}
