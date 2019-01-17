package redis

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/go-redis/redis"
	"sort"
	"strings"
	"time"
)

const (
	DIR_LIST_MARKER = "\x00"
)

type UniversalRedisStore struct {
	Client redis.UniversalClient
}

func (store *UniversalRedisStore) InsertEntry(entry *filer2.Entry) (err error) {

	value, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	_, err = store.Client.Set(string(entry.FullPath), value, time.Duration(entry.TtlSec)*time.Second).Result()

	if err != nil {
		return fmt.Errorf("persisting %s : %v", entry.FullPath, err)
	}

	dir, name := entry.FullPath.DirAndName()
	if name != "" {
		_, err = store.Client.SAdd(genDirectoryListKey(dir), name).Result()
		if err != nil {
			return fmt.Errorf("persisting %s in parent dir: %v", entry.FullPath, err)
		}
	}

	return nil
}

func (store *UniversalRedisStore) UpdateEntry(entry *filer2.Entry) (err error) {

	return store.InsertEntry(entry)
}

func (store *UniversalRedisStore) FindEntry(fullpath filer2.FullPath) (entry *filer2.Entry, err error) {

	data, err := store.Client.Get(string(fullpath)).Result()
	if err == redis.Nil {
		return nil, filer2.ErrNotFound
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

func (store *UniversalRedisStore) DeleteEntry(fullpath filer2.FullPath) (err error) {

	_, err = store.Client.Del(string(fullpath)).Result()

	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	dir, name := fullpath.DirAndName()
	if name != "" {
		_, err = store.Client.SRem(genDirectoryListKey(dir), name).Result()
		if err != nil {
			return fmt.Errorf("delete %s in parent dir: %v", fullpath, err)
		}
	}

	return nil
}

func (store *UniversalRedisStore) ListDirectoryEntries(fullpath filer2.FullPath, startFileName string, inclusive bool,
	limit int) (entries []*filer2.Entry, err error) {

	members, err := store.Client.SMembers(genDirectoryListKey(string(fullpath))).Result()
	if err != nil {
		return nil, fmt.Errorf("list %s : %v", fullpath, err)
	}

	// skip
	if startFileName != "" {
		var t []string
		for _, m := range members {
			if strings.Compare(m, startFileName) >= 0 {
				if m == startFileName {
					if inclusive {
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
	sort.Slice(members, func(i, j int) bool {
		return strings.Compare(members[i], members[j]) < 0
	})

	// limit
	if limit < len(members) {
		members = members[:limit]
	}

	// fetch entry meta
	for _, fileName := range members {
		path := filer2.NewFullPath(string(fullpath), fileName)
		entry, err := store.FindEntry(path)
		if err != nil {
			glog.V(0).Infof("list %s : %v", path, err)
		} else {
			entries = append(entries, entry)
		}
	}

	return entries, err
}

func genDirectoryListKey(dir string) (dirList string) {
	return dir + DIR_LIST_MARKER
}
