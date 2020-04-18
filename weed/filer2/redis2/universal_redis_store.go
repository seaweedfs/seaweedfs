package redis2

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

const (
	DIR_LIST_MARKER = "\x00"
)

type UniversalRedis2Store struct {
	Client redis.UniversalClient
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

func (store *UniversalRedis2Store) InsertEntry(ctx context.Context, entry *filer2.Entry) (err error) {

	value, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	if err = store.Client.Set(string(entry.FullPath), value, time.Duration(entry.TtlSec)*time.Second).Err(); err != nil {
		return fmt.Errorf("persisting %s : %v", entry.FullPath, err)
	}

	dir, name := entry.FullPath.DirAndName()
	if name != "" {
		if err = store.Client.ZAddNX(genDirectoryListKey(dir), redis.Z{Score: 0, Member: name}).Err(); err != nil {
			return fmt.Errorf("persisting %s in parent dir: %v", entry.FullPath, err)
		}
	}

	return nil
}

func (store *UniversalRedis2Store) UpdateEntry(ctx context.Context, entry *filer2.Entry) (err error) {

	return store.InsertEntry(ctx, entry)
}

func (store *UniversalRedis2Store) FindEntry(ctx context.Context, fullpath util.FullPath) (entry *filer2.Entry, err error) {

	data, err := store.Client.Get(string(fullpath)).Result()
	if err == redis.Nil {
		return nil, filer_pb.ErrNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("get %s : %v", fullpath, err)
	}

	entry = &filer2.Entry{
		FullPath: fullpath,
	}
	err = entry.DecodeAttributesAndChunks([]byte(data))
	if err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}

	return entry, nil
}

func (store *UniversalRedis2Store) DeleteEntry(ctx context.Context, fullpath util.FullPath) (err error) {

	_, err = store.Client.Del(string(fullpath)).Result()

	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	dir, name := fullpath.DirAndName()
	if name != "" {
		_, err = store.Client.ZRem(genDirectoryListKey(dir), name).Result()
		if err != nil {
			return fmt.Errorf("delete %s in parent dir: %v", fullpath, err)
		}
	}

	return nil
}

func (store *UniversalRedis2Store) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) (err error) {

	members, err := store.Client.ZRange(genDirectoryListKey(string(fullpath)), 0, -1).Result()
	if err != nil {
		return fmt.Errorf("delete folder %s : %v", fullpath, err)
	}

	for _, fileName := range members {
		path := util.NewFullPath(string(fullpath), fileName)
		_, err = store.Client.Del(string(path)).Result()
		if err != nil {
			return fmt.Errorf("delete %s in parent dir: %v", fullpath, err)
		}
	}

	return nil
}

func (store *UniversalRedis2Store) ListDirectoryEntries(ctx context.Context, fullpath util.FullPath, startFileName string, inclusive bool,
	limit int) (entries []*filer2.Entry, err error) {

	dirListKey := genDirectoryListKey(string(fullpath))
	start := int64(0)
	if startFileName != "" {
		start, _ = store.Client.ZRank(dirListKey, startFileName).Result()
		if !inclusive {
			start++
		}
	}
	members, err := store.Client.ZRange(dirListKey, start, start+int64(limit)-1).Result()
	if err != nil {
		return nil, fmt.Errorf("list %s : %v", fullpath, err)
	}

	// fetch entry meta
	for _, fileName := range members {
		path := util.NewFullPath(string(fullpath), fileName)
		entry, err := store.FindEntry(ctx, path)
		if err != nil {
			glog.V(0).Infof("list %s : %v", path, err)
		} else {
			if entry.TtlSec > 0 {
				if entry.Attr.Crtime.Add(time.Duration(entry.TtlSec) * time.Second).Before(time.Now()) {
					store.Client.Del(string(path)).Result()
					store.Client.ZRem(dirListKey, fileName).Result()
					continue
				}
			}
			entries = append(entries, entry)
		}
	}

	return entries, err
}

func genDirectoryListKey(dir string) (dirList string) {
	return dir + DIR_LIST_MARKER
}

func (store *UniversalRedis2Store) Shutdown() {
	store.Client.Close()
}
