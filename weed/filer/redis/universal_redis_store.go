package redis

import (
	"context"
	"fmt"
	"slices"
	"strings"
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

type UniversalRedisStore struct {
	Client redis.UniversalClient
}

func (store *UniversalRedisStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (store *UniversalRedisStore) CommitTransaction(ctx context.Context) error {
	return nil
}
func (store *UniversalRedisStore) RollbackTransaction(ctx context.Context) error {
	return nil
}

func (store *UniversalRedisStore) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {

	if err = store.doInsertEntry(ctx, entry); err != nil {
		return err
	}

	dir, name := entry.FullPath.DirAndName()
	if name != "" {
		_, err = store.Client.SAdd(ctx, genDirectoryListKey(dir), name).Result()
		if err != nil {
			return fmt.Errorf("persisting %s in parent dir: %v", entry.FullPath, err)
		}
	}

	return nil
}

func (store *UniversalRedisStore) doInsertEntry(ctx context.Context, entry *filer.Entry) error {
	value, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	if len(entry.GetChunks()) > filer.CountEntryChunksForGzip {
		value = util.MaybeGzipData(value)
	}

	_, err = store.Client.Set(ctx, string(entry.FullPath), value, time.Duration(entry.TtlSec)*time.Second).Result()

	if err != nil {
		return fmt.Errorf("persisting %s : %v", entry.FullPath, err)
	}
	return nil
}

func (store *UniversalRedisStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {

	return store.doInsertEntry(ctx, entry)
}

func (store *UniversalRedisStore) FindEntry(ctx context.Context, fullpath util.FullPath) (entry *filer.Entry, err error) {

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

func (store *UniversalRedisStore) DeleteEntry(ctx context.Context, fullpath util.FullPath) (err error) {

	_, err = store.Client.Del(ctx, string(fullpath)).Result()

	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	dir, name := fullpath.DirAndName()
	if name != "" {
		_, err = store.Client.SRem(ctx, genDirectoryListKey(dir), name).Result()
		if err != nil {
			return fmt.Errorf("delete %s in parent dir: %v", fullpath, err)
		}
	}

	return nil
}

func (store *UniversalRedisStore) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) (err error) {

	members, err := store.Client.SMembers(ctx, genDirectoryListKey(string(fullpath))).Result()
	if err != nil {
		return fmt.Errorf("delete folder %s : %v", fullpath, err)
	}

	for _, fileName := range members {
		path := util.NewFullPath(string(fullpath), fileName)
		_, err = store.Client.Del(ctx, string(path)).Result()
		if err != nil {
			return fmt.Errorf("delete %s in parent dir: %v", fullpath, err)
		}
		// not efficient, but need to remove if it is a directory
		store.Client.Del(ctx, genDirectoryListKey(string(path)))
	}

	return nil
}

func (store *UniversalRedisStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return lastFileName, filer.ErrUnsupportedListDirectoryPrefixed
}

func (store *UniversalRedisStore) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {

	dirListKey := genDirectoryListKey(string(dirPath))
	members, err := store.Client.SMembers(ctx, dirListKey).Result()
	if err != nil {
		return lastFileName, fmt.Errorf("list %s : %v", dirPath, err)
	}

	// skip
	if startFileName != "" {
		var t []string
		for _, m := range members {
			if strings.Compare(m, startFileName) >= 0 {
				if m == startFileName {
					if includeStartFile {
						t = append(t, m)
					}
				} else {
					t = append(t, m)
				}
			}
		}
		members = t
	}

	// sort
	slices.SortFunc(members, func(a, b string) int {
		return strings.Compare(a, b)
	})

	// limit
	if limit < int64(len(members)) {
		members = members[:limit]
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
					store.Client.Del(ctx, string(path)).Result()
					store.Client.SRem(ctx, dirListKey, fileName).Result()
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

func (store *UniversalRedisStore) Shutdown() {
	store.Client.Close()
}
