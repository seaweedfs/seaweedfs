package redis_lua

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/redis_lua/stored_procedure"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	DIR_LIST_MARKER = "\x00"
)

type UniversalRedisLuaStore struct {
	Client                  redis.UniversalClient
	superLargeDirectoryHash map[string]bool
}

func (store *UniversalRedisLuaStore) isSuperLargeDirectory(dir string) (isSuperLargeDirectory bool) {
	_, isSuperLargeDirectory = store.superLargeDirectoryHash[dir]
	return
}

func (store *UniversalRedisLuaStore) loadSuperLargeDirectories(superLargeDirectories []string) {
	// set directory hash
	store.superLargeDirectoryHash = make(map[string]bool)
	for _, dir := range superLargeDirectories {
		store.superLargeDirectoryHash[dir] = true
	}
}

func (store *UniversalRedisLuaStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (store *UniversalRedisLuaStore) CommitTransaction(ctx context.Context) error {
	return nil
}
func (store *UniversalRedisLuaStore) RollbackTransaction(ctx context.Context) error {
	return nil
}

func (store *UniversalRedisLuaStore) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {

	value, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	if len(entry.GetChunks()) > filer.CountEntryChunksForGzip {
		value = util.MaybeGzipData(value)
	}

	dir, name := entry.FullPath.DirAndName()

	err = stored_procedure.InsertEntryScript.Run(ctx, store.Client,
		[]string{string(entry.FullPath), genDirectoryListKey(dir)},
		value, entry.TtlSec,
		store.isSuperLargeDirectory(dir), 0, name).Err()

	if err != nil {
		return fmt.Errorf("persisting %s : %v", entry.FullPath, err)
	}

	return nil
}

func (store *UniversalRedisLuaStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {

	return store.InsertEntry(ctx, entry)
}

func (store *UniversalRedisLuaStore) FindEntry(ctx context.Context, fullpath util.FullPath) (entry *filer.Entry, err error) {

	data, err := store.Client.Get(ctx, string(fullpath)).Result()
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

func (store *UniversalRedisLuaStore) DeleteEntry(ctx context.Context, fullpath util.FullPath) (err error) {

	dir, name := fullpath.DirAndName()

	err = stored_procedure.DeleteEntryScript.Run(ctx, store.Client,
		[]string{string(fullpath), genDirectoryListKey(string(fullpath)), genDirectoryListKey(dir)},
		store.isSuperLargeDirectory(dir), name).Err()

	if err != nil {
		return fmt.Errorf("DeleteEntry %s : %v", fullpath, err)
	}

	return nil
}

func (store *UniversalRedisLuaStore) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) (err error) {

	if store.isSuperLargeDirectory(string(fullpath)) {
		return nil
	}

	err = stored_procedure.DeleteFolderChildrenScript.Run(ctx, store.Client,
		[]string{string(fullpath)}).Err()

	if err != nil {
		return fmt.Errorf("DeleteFolderChildren %s : %v", fullpath, err)
	}

	return nil
}

func (store *UniversalRedisLuaStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return lastFileName, filer.ErrUnsupportedListDirectoryPrefixed
}

func (store *UniversalRedisLuaStore) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {

	dirListKey := genDirectoryListKey(string(dirPath))

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
	for _, fileName := range members {
		path := util.NewFullPath(string(dirPath), fileName)
		entry, err := store.FindEntry(ctx, path)
		lastFileName = fileName
		if err != nil {
			glog.V(0).Infof("list %s : %v", path, err)
			if err == filer_pb.ErrNotFound {
				continue
			}
		} else {
			if entry.TtlSec > 0 {
				if entry.Attr.Crtime.Add(time.Duration(entry.TtlSec) * time.Second).Before(time.Now()) {
					store.DeleteEntry(ctx, path)
					continue
				}
			}
			if !eachEntryFunc(entry) {
				break
			}
		}
	}

	return lastFileName, err
}

func genDirectoryListKey(dir string) (dirList string) {
	return dir + DIR_LIST_MARKER
}

func (store *UniversalRedisLuaStore) Shutdown() {
	store.Client.Close()
}
