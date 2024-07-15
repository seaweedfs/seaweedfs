package redis3

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	redsync "github.com/go-redsync/redsync/v4"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	DIR_LIST_MARKER = "\x00"
)

type UniversalRedis3Store struct {
	Client  redis.UniversalClient
	redsync *redsync.Redsync
}

func (store *UniversalRedis3Store) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (store *UniversalRedis3Store) CommitTransaction(ctx context.Context) error {
	return nil
}
func (store *UniversalRedis3Store) RollbackTransaction(ctx context.Context) error {
	return nil
}

func (store *UniversalRedis3Store) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {

	if err = store.doInsertEntry(ctx, entry); err != nil {
		return err
	}

	dir, name := entry.FullPath.DirAndName()

	if name != "" {
		if err = insertChild(ctx, store, genDirectoryListKey(dir), name); err != nil {
			return fmt.Errorf("persisting %s in parent dir: %v", entry.FullPath, err)
		}
	}

	return nil
}

func (store *UniversalRedis3Store) doInsertEntry(ctx context.Context, entry *filer.Entry) error {
	value, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	if len(entry.GetChunks()) > filer.CountEntryChunksForGzip {
		value = util.MaybeGzipData(value)
	}

	if err = store.Client.Set(ctx, string(entry.FullPath), value, time.Duration(entry.TtlSec)*time.Second).Err(); err != nil {
		return fmt.Errorf("persisting %s : %v", entry.FullPath, err)
	}
	return nil
}

func (store *UniversalRedis3Store) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {

	return store.doInsertEntry(ctx, entry)
}

func (store *UniversalRedis3Store) FindEntry(ctx context.Context, fullpath util.FullPath) (entry *filer.Entry, err error) {

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

func (store *UniversalRedis3Store) DeleteEntry(ctx context.Context, fullpath util.FullPath) (err error) {

	_, err = store.Client.Del(ctx, genDirectoryListKey(string(fullpath))).Result()
	if err != nil {
		return fmt.Errorf("delete dir list %s : %v", fullpath, err)
	}

	_, err = store.Client.Del(ctx, string(fullpath)).Result()
	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	dir, name := fullpath.DirAndName()

	if name != "" {
		if err = removeChild(ctx, store, genDirectoryListKey(dir), name); err != nil {
			return fmt.Errorf("DeleteEntry %s in parent dir: %v", fullpath, err)
		}
	}

	return nil
}

func (store *UniversalRedis3Store) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) (err error) {

	return removeChildren(ctx, store, genDirectoryListKey(string(fullpath)), func(name string) error {
		path := util.NewFullPath(string(fullpath), name)
		_, err = store.Client.Del(ctx, string(path)).Result()
		if err != nil {
			return fmt.Errorf("DeleteFolderChildren %s in parent dir: %v", fullpath, err)
		}
		// not efficient, but need to remove if it is a directory
		store.Client.Del(ctx, genDirectoryListKey(string(path)))
		return nil
	})

}

func (store *UniversalRedis3Store) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return lastFileName, filer.ErrUnsupportedListDirectoryPrefixed
}

func (store *UniversalRedis3Store) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {

	dirListKey := genDirectoryListKey(string(dirPath))
	counter := int64(0)

	err = listChildren(ctx, store, dirListKey, startFileName, func(fileName string) bool {
		if startFileName != "" {
			if !includeStartFile && startFileName == fileName {
				return true
			}
		}

		path := util.NewFullPath(string(dirPath), fileName)
		entry, err := store.FindEntry(ctx, path)
		lastFileName = fileName
		if err != nil {
			glog.V(0).Infof("list %s : %v", path, err)
			if err == filer_pb.ErrNotFound {
				return true
			}
		} else {
			if entry.TtlSec > 0 {
				if entry.Attr.Crtime.Add(time.Duration(entry.TtlSec) * time.Second).Before(time.Now()) {
					store.Client.Del(ctx, string(path)).Result()
					store.Client.ZRem(ctx, dirListKey, fileName).Result()
					return true
				}
			}
			counter++
			if !eachEntryFunc(entry) {
				return false
			}
			if counter >= limit {
				return false
			}
		}
		return true
	})

	return lastFileName, err
}

func genDirectoryListKey(dir string) (dirList string) {
	return dir + DIR_LIST_MARKER
}

func (store *UniversalRedis3Store) Shutdown() {
	store.Client.Close()
}
