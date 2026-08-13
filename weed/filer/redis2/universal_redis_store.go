package redis2

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	DIR_LIST_MARKER = "\x00"
)

type UniversalRedis2Store struct {
	Client                  redis.UniversalClient
	keyPrefix               string
	superLargeDirectoryHash map[string]bool
}

func (store *UniversalRedis2Store) isSuperLargeDirectory(dir string) (isSuperLargeDirectory bool) {
	_, isSuperLargeDirectory = store.superLargeDirectoryHash[dir]
	return
}

func (store *UniversalRedis2Store) loadSuperLargeDirectories(superLargeDirectories []string) {
	// set directory hash
	store.superLargeDirectoryHash = make(map[string]bool)
	for _, dir := range superLargeDirectories {
		store.superLargeDirectoryHash[dir] = true
	}
}

func (store *UniversalRedis2Store) getKey(key string) string {
	if store.keyPrefix == "" {
		return key
	}
	return store.keyPrefix + key
}

func (store *UniversalRedis2Store) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (store *UniversalRedis2Store) CommitTransaction(ctx context.Context) error {
	return nil
}
func (store *UniversalRedis2Store) RollbackTransaction(ctx context.Context) error {
	return nil
}

func (store *UniversalRedis2Store) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {

	if err = store.doInsertEntry(ctx, entry); err != nil {
		return err
	}

	dir, name := entry.FullPath.DirAndName()
	if store.isSuperLargeDirectory(dir) {
		return nil
	}

	if name != "" {
		if err = store.Client.ZAddNX(ctx, store.getKey(genDirectoryListKey(dir)), redis.Z{Score: 0, Member: name}).Err(); err != nil {
			return fmt.Errorf("persisting %s in parent dir: %v", entry.FullPath, err)
		}
	}

	return nil
}

func (store *UniversalRedis2Store) doInsertEntry(ctx context.Context, entry *filer.Entry) error {
	value, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	if len(entry.GetChunks()) > filer.CountEntryChunksForGzip {
		value = util.MaybeGzipData(value)
	}

	if err = store.Client.Set(ctx, store.getKey(string(entry.FullPath)), value, time.Duration(entry.TtlSec)*time.Second).Err(); err != nil {
		return fmt.Errorf("persisting %s : %v", entry.FullPath, err)
	}
	return nil
}

func (store *UniversalRedis2Store) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {

	return store.doInsertEntry(ctx, entry)
}

func (store *UniversalRedis2Store) FindEntry(ctx context.Context, fullpath util.FullPath) (entry *filer.Entry, err error) {

	data, err := store.Client.Get(ctx, store.getKey(string(fullpath))).Result()
	if err == redis.Nil {
		return nil, filer_pb.ErrNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("get %s : %v", fullpath, err)
	}

	entry = &filer.Entry{
		FullPath: fullpath,
	}
	err = entry.DecodeAttributesAndChunks(util.MaybeDecompressData([]byte(data)))
	if err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}

	return entry, nil
}

func (store *UniversalRedis2Store) DeleteEntry(ctx context.Context, fullpath util.FullPath) (err error) {

	_, err = store.Client.Del(ctx, store.getKey(genDirectoryListKey(string(fullpath)))).Result()
	if err != nil {
		return fmt.Errorf("delete dir list %s : %v", fullpath, err)
	}

	_, err = store.Client.Del(ctx, store.getKey(string(fullpath))).Result()
	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	dir, name := fullpath.DirAndName()
	if store.isSuperLargeDirectory(dir) {
		return nil
	}
	if name != "" {
		_, err = store.Client.ZRem(ctx, store.getKey(genDirectoryListKey(dir)), name).Result()
		if err != nil {
			return fmt.Errorf("DeleteEntry %s in parent dir: %v", fullpath, err)
		}
	}

	return nil
}

func (store *UniversalRedis2Store) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) (err error) {

	if store.isSuperLargeDirectory(string(fullpath)) {
		return nil
	}

	members, err := store.Client.ZRangeByLex(ctx, store.getKey(genDirectoryListKey(string(fullpath))), &redis.ZRangeBy{
		Min: "-",
		Max: "+",
	}).Result()
	if err != nil {
		return fmt.Errorf("DeleteFolderChildren %s : %v", fullpath, err)
	}

	for _, fileName := range members {
		path := util.NewFullPath(string(fullpath), fileName)
		_, err = store.Client.Del(ctx, store.getKey(string(path))).Result()
		if err != nil {
			return fmt.Errorf("DeleteFolderChildren %s in parent dir: %v", fullpath, err)
		}
		// not efficient, but need to remove if it is a directory
		store.Client.Del(ctx, store.getKey(genDirectoryListKey(string(path))))
	}

	return nil
}

func (store *UniversalRedis2Store) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return lastFileName, filer.ErrUnsupportedListDirectoryPrefixed
}

func (store *UniversalRedis2Store) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {

	dirListKey := store.getKey(genDirectoryListKey(string(dirPath)))

	min := "-"
	if startFileName != "" {
		if includeStartFile {
			min = "[" + startFileName
		} else {
			min = "(" + startFileName
		}
	}

	members, err := store.Client.ZRangeByLex(ctx, dirListKey, &redis.ZRangeBy{
		Min:    min,
		Max:    "+",
		Offset: 0,
		Count:  limit,
	}).Result()
	if err != nil {
		return lastFileName, fmt.Errorf("list %s : %v", dirPath, err)
	}

	// fetch entry meta
	var entry *filer.Entry
	for _, fileName := range members {
		path := util.NewFullPath(string(dirPath), fileName)
		entry, err = store.FindEntry(ctx, path)
		lastFileName = fileName
		if err != nil {
			glog.V(0).InfofCtx(ctx, "list %s : %v", path, err)
			if err == filer_pb.ErrNotFound {
				store.removeOrphanedDirectoryListMember(ctx, dirPath, fileName)
				err = nil
				continue
			}
			break
		} else {
			if isLogicallyExpired(entry) {
				store.deleteExpiredEntry(ctx, dirPath, path, fileName)
				continue
			}

			resEachEntryFunc, resEachEntryFuncErr := eachEntryFunc(entry)
			if resEachEntryFuncErr != nil {
				err = fmt.Errorf("failed to process eachEntryFunc: %w", resEachEntryFuncErr)
				break
			}

			if !resEachEntryFunc {
				break
			}
		}
	}

	return lastFileName, err
}

func (store *UniversalRedis2Store) removeOrphanedDirectoryListMember(ctx context.Context, dirPath util.FullPath, fileName string) {
	// a directory converted to super large after accumulating members still has a legacy index
	if store.isSuperLargeDirectory(string(dirPath)) {
		return
	}

	// survive the listing request being canceled mid-repair
	ctx = context.WithoutCancel(ctx)

	dirListKey := store.getKey(genDirectoryListKey(string(dirPath)))
	path := util.NewFullPath(string(dirPath), fileName)

	if err := store.Client.ZRem(ctx, dirListKey, fileName).Err(); err != nil {
		return
	}

	// InsertEntry writes the value before adding the member, so a value present
	// again here may belong to an insert that found the member still in place
	// and whose ZAddNX was therefore a no-op.
	exists, err := store.Client.Exists(ctx, store.getKey(string(path))).Result()
	if err == nil && exists == 0 {
		// an evicted directory may still have a live child index; empty zsets self-delete,
		// so a present index holds children a recursive delete still needs to reach
		children, childrenErr := store.Client.Exists(ctx, store.getKey(genDirectoryListKey(string(path)))).Result()
		if childrenErr == nil && children == 0 {
			return
		}
	}

	if err := store.Client.ZAddNX(ctx, dirListKey, redis.Z{Score: 0, Member: fileName}).Err(); err != nil {
		glog.V(0).InfofCtx(ctx, "restore %s in %s: %v", fileName, dirPath, err)
	}
}

func isLogicallyExpired(entry *filer.Entry) bool {
	return entry.TtlSec > 0 && entry.Attr.Crtime.Add(time.Duration(entry.TtlSec)*time.Second).Before(time.Now())
}

// deletes the value only when it still holds exactly the bytes the expiry decision was made on;
// single-key, so it runs on all transports where a multi-key script would be CROSSSLOT
var deleteIfUnchangedScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
	return redis.call('DEL', KEYS[1])
end
return 0`)

func (store *UniversalRedis2Store) deleteExpiredEntry(ctx context.Context, dirPath util.FullPath, path util.FullPath, fileName string) {
	// survive the listing request being canceled mid-delete
	ctx = context.WithoutCancel(ctx)
	valueKey := store.getKey(string(path))

	// re-read so the delete can be conditioned on exactly the bytes checked
	data, err := store.Client.Get(ctx, valueKey).Bytes()
	if err == redis.Nil {
		store.removeOrphanedDirectoryListMember(ctx, dirPath, fileName)
		return
	}
	if err != nil {
		return
	}

	entry := &filer.Entry{FullPath: path}
	if err := entry.DecodeAttributesAndChunks(util.MaybeDecompressData(data)); err != nil {
		return
	}
	if !isLogicallyExpired(entry) {
		// a concurrent insert recreated it
		return
	}

	deleted, err := deleteIfUnchangedScript.Run(ctx, store.Client, []string{valueKey}, data).Int()
	if err != nil || deleted == 0 {
		return
	}
	store.removeOrphanedDirectoryListMember(ctx, dirPath, fileName)
}

func genDirectoryListKey(dir string) (dirList string) {
	return dir + DIR_LIST_MARKER
}

func (store *UniversalRedis2Store) Shutdown() {
	store.Client.Close()
}
