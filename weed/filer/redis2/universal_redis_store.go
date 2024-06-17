package redis2

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	DirListMarker = "\x00"
)

type UniversalRedis2Store struct {
	Client                  redis.UniversalClient
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

func (store *UniversalRedis2Store) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func (store *UniversalRedis2Store) CommitTransaction(context.Context) error {
	return nil
}

func (store *UniversalRedis2Store) RollbackTransaction(context.Context) error {
	return nil
}

func (store *UniversalRedis2Store) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {
	var insertCmd *redis.StatusCmd
	var addCmd *redis.IntCmd

	_, err = store.Client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		insertCmd, err = store.insertOrUpdate(ctx, pipe, entry)
		if err != nil {
			return err
		}

		dir, name := entry.FullPath.DirAndName()
		if store.isSuperLargeDirectory(dir) {
			return nil
		}

		if name != "" {
			addCmd = pipe.ZAddNX(ctx, genDirectoryListKey(dir), &redis.Z{Score: 0, Member: name})
		}
		return err
	})
	if err != nil {
		return err
	}
	if insertCmd.Err() != nil {
		return fmt.Errorf("persisting %s : %v", entry.FullPath, err)
	}
	if addCmd.Err() != nil {
		return fmt.Errorf("persisting %s in parent dir: %v", entry.FullPath, err)
	}
	return nil
}

func (store *UniversalRedis2Store) insertOrUpdate(ctx context.Context, client redis.Cmdable, entry *filer.Entry) (*redis.StatusCmd, error) {
	data, err := encodeEntry(entry)
	if err != nil {
		return nil, fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	// Don't use Redis' native TTL support because we need to clean the entry from its parent directory set as well
	return client.Set(ctx, string(entry.FullPath), data, 0), nil
}

func (store *UniversalRedis2Store) UpdateEntry(ctx context.Context, entry *filer.Entry) error {
	cmd, err := store.insertOrUpdate(ctx, store.Client, entry)
	if err != nil {
		return err
	}
	if cmd.Err() != nil {
		err = fmt.Errorf("persisting %s : %v", entry.FullPath, err)
	}
	return err
}

func (store *UniversalRedis2Store) FindEntry(ctx context.Context, fullpath util.FullPath) (*filer.Entry, error) {
	res, err := store.Client.Get(ctx, string(fullpath)).Result()
	if err == redis.Nil {
		return nil, filer_pb.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get %s : %v", fullpath, err)
	}

	return store.dataToEntry(res, fullpath)
}

func (store *UniversalRedis2Store) FindEntries(ctx context.Context, fullpaths []util.FullPath) ([]*filer.Entry, error) {
	paths := make([]string, len(fullpaths))
	for i, fullpath := range fullpaths {
		paths[i] = string(fullpath)
	}

	cmds := make([]*redis.StringCmd, len(fullpaths))
	_, err := store.Client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i, fullpath := range fullpaths {
			cmds[i] = pipe.Get(ctx, string(fullpath))
		}
		return nil
	})
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("get %s : %w", fullpaths, err)
	}

	// Convert results to filer entries
	entries := make([]*filer.Entry, 0, len(fullpaths))
	for i, cmd := range cmds {
		if cmd.Err() == redis.Nil {
			glog.V(0).Infof("get %s : %v", fullpaths[i], filer_pb.ErrNotFound)
			continue
		}

		entry, err := store.dataToEntry(cmd.Val(), fullpaths[i])
		if err != nil {
			return nil, fmt.Errorf("get %s : %w", fullpaths[i], err)
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func (store *UniversalRedis2Store) dataToEntry(data string, fullpath util.FullPath) (entry *filer.Entry, err error) {
	entry = &filer.Entry{
		FullPath: fullpath,
	}
	err = entry.DecodeAttributesAndChunks(util.MaybeDecompressData([]byte(data)))
	return
}

func (store *UniversalRedis2Store) DeleteEntry(ctx context.Context, fullpath util.FullPath) error {
	cmds := make([]*redis.IntCmd, 3)
	_, err := store.Client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		cmds[0] = pipe.Del(ctx, genDirectoryListKey(string(fullpath)))
		cmds[1] = pipe.Del(ctx, string(fullpath))

		dir, name := fullpath.DirAndName()
		if store.isSuperLargeDirectory(dir) {
			return nil
		}

		if name != "" {
			cmds[2] = pipe.ZRem(ctx, genDirectoryListKey(dir), name)
		}
		return nil
	})
	if err != nil && err != redis.Nil {
		return fmt.Errorf("delete entry %s : %v", fullpath, err)
	}

	if cmds[0].Err() != nil && cmds[0].Err() != redis.Nil {
		return fmt.Errorf("delete dir list %s : %v", fullpath, err)
	}
	if cmds[1].Err() != nil && cmds[1].Err() != redis.Nil {
		return fmt.Errorf("delete entry %s : %v", fullpath, err)
	}
	if cmds[2] != nil && cmds[2].Err() != nil && cmds[2].Err() != redis.Nil {
		return fmt.Errorf("DeleteEntry %s in parent dir: %v", fullpath, err)
	}
	return nil
}

func (store *UniversalRedis2Store) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) (err error) {
	if store.isSuperLargeDirectory(string(fullpath)) {
		return nil
	}

	members, err := store.Client.ZRangeByLex(ctx, genDirectoryListKey(string(fullpath)), &redis.ZRangeBy{
		Min: "-",
		Max: "+",
	}).Result()
	if err != nil {
		return fmt.Errorf("DeleteFolderChildren %s : %v", fullpath, err)
	}
	if len(members) == 0 {
		return
	}

	cmds := make([]*redis.IntCmd, len(members)*2)
	_, err = store.Client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i, fileName := range members {
			path := string(util.NewFullPath(string(fullpath), fileName))
			cmds[i*2] = pipe.Del(ctx, path)
			// not efficient, but need to remove if it is a directory
			cmds[i*2+1] = pipe.Del(ctx, genDirectoryListKey(path))
		}
		return nil
	})
	if err != nil && err != redis.Nil {
		return fmt.Errorf("DeleteFolderChildren %s in parent dir: %v", fullpath, err)
	}
	for _, cmd := range cmds {
		if cmd.Err() != nil && cmd.Err() != redis.Nil {
			return fmt.Errorf("DeleteFolderChildren %s in parent dir: %v", fullpath, err)
		}
	}

	return
}

func (store *UniversalRedis2Store) ListDirectoryPrefixedEntries(context.Context, util.FullPath, string, bool, int64, string, filer.ListEachEntryFunc) (lastFileName string, err error) {
	return lastFileName, filer.ErrUnsupportedListDirectoryPrefixed
}

func (store *UniversalRedis2Store) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {

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
	if len(members) == 0 {
		return lastFileName, nil
	}

	// fetch entry meta
	var paths []util.FullPath
	for _, fileName := range members {
		path := util.NewFullPath(string(dirPath), fileName)
		paths = append(paths, path)
	}

	entries, err := store.FindEntries(ctx, paths)
	if err != nil {
		glog.V(0).Infof("list %s : %v", dirPath, err)
		return lastFileName, err
	}

	// Clean expired keys
	var expiredEntries []*filer.Entry
	for _, entry := range entries {
		if entry.TtlSec > 0 && entry.Attr.Crtime.Add(time.Duration(entry.TtlSec)*time.Second).Before(time.Now()) {
			expiredEntries = append(expiredEntries, entry)
		} else {
			lastFileName = entry.Name()
			if !eachEntryFunc(entry) {
				break
			}
		}
	}
	if len(expiredEntries) == 0 {
		return lastFileName, nil
	}

	_, _ = store.Client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, entry := range expiredEntries {
			pipe.Del(ctx, string(entry.FullPath))
			pipe.ZRem(ctx, dirListKey, entry.Name())
		}
		return nil
	})
	return lastFileName, nil
}

func (store *UniversalRedis2Store) Shutdown() {
	_ = store.Client.Close()
}

func genDirectoryListKey(dir string) (dirList string) {
	return dir + DirListMarker
}

func encodeEntry(entry *filer.Entry) ([]byte, error) {
	data, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return nil, fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	if len(entry.GetChunks()) > filer.CountEntryChunksForGzip {
		data = util.MaybeGzipData(data)
	}
	return data, nil
}
