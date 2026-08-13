package redis2

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func newTestStore(t *testing.T, keyPrefix string) (*UniversalRedis2Store, util.FullPath) {
	t.Helper()

	if os.Getenv("RUN_REDIS_TESTS") != "1" {
		t.Skip("redis2 tests are disabled. Start a redis-server and set RUN_REDIS_TESTS=1 to enable, REDIS_ADDR defaults to 127.0.0.1:6379.")
	}

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close redis client: %v", err)
		}
	})
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("connect to redis at %s: %v", addr, err)
	}

	store := &UniversalRedis2Store{Client: client, keyPrefix: keyPrefix}

	dir := util.FullPath(fmt.Sprintf("/redis2_test_%d", time.Now().UnixNano()))
	t.Cleanup(func() {
		if err := store.DeleteFolderChildren(ctx, dir); err != nil {
			t.Errorf("cleanup %s children: %v", dir, err)
		}
		if err := store.DeleteEntry(ctx, dir); err != nil {
			t.Errorf("cleanup %s: %v", dir, err)
		}
	})

	return store, dir
}

func insertTestEntry(t *testing.T, store *UniversalRedis2Store, path util.FullPath, ttlSec int32) {
	t.Helper()

	now := time.Now()
	if err := store.InsertEntry(context.Background(), &filer.Entry{
		FullPath: path,
		Attr:     filer.Attr{Crtime: now, Mtime: now, Mode: 0644, TtlSec: ttlSec},
	}); err != nil {
		t.Fatalf("InsertEntry %s: %v", path, err)
	}
}

func listNames(t *testing.T, store *UniversalRedis2Store, dir util.FullPath) []string {
	t.Helper()

	names := []string{}
	if _, err := store.ListDirectoryEntries(context.Background(), dir, "", true, 100, func(entry *filer.Entry) (bool, error) {
		_, name := entry.DirAndName()
		names = append(names, name)
		return true, nil
	}); err != nil {
		t.Fatalf("ListDirectoryEntries %s: %v", dir, err)
	}
	return names
}

func indexMembers(t *testing.T, store *UniversalRedis2Store, dir util.FullPath) []string {
	t.Helper()

	members, err := store.Client.ZRangeByLex(context.Background(), store.getKey(genDirectoryListKey(string(dir))), &redis.ZRangeBy{Min: "-", Max: "+"}).Result()
	if err != nil {
		t.Fatalf("read directory index of %s: %v", dir, err)
	}
	return members
}

func TestListDirectoryEntriesRemovesOrphanedIndexMembers(t *testing.T) {
	for _, keyPrefix := range []string{"", "sw:"} {
		t.Run("keyPrefix="+keyPrefix, func(t *testing.T) {
			store, dir := newTestStore(t, keyPrefix)

			insertTestEntry(t, store, dir.Child("alive"), 0)
			insertTestEntry(t, store, dir.Child("orphan"), 0)

			if err := store.Client.Del(context.Background(), store.getKey(string(dir.Child("orphan")))).Err(); err != nil {
				t.Fatalf("drop value key: %v", err)
			}

			if names := listNames(t, store, dir); len(names) != 1 || names[0] != "alive" {
				t.Fatalf("listed %v, want [alive]", names)
			}

			if members := indexMembers(t, store, dir); len(members) != 1 || members[0] != "alive" {
				t.Fatalf("directory index holds %v, want [alive]", members)
			}
		})
	}
}

func TestRemoveOrphanedDirectoryListMemberKeepsRecreatedEntry(t *testing.T) {
	for _, keyPrefix := range []string{"", "sw:"} {
		t.Run("keyPrefix="+keyPrefix, func(t *testing.T) {
			store, dir := newTestStore(t, keyPrefix)

			path := dir.Child("recreated")
			insertTestEntry(t, store, path, 0)

			store.removeOrphanedDirectoryListMember(context.Background(), dir, "recreated")

			if members := indexMembers(t, store, dir); len(members) != 1 || members[0] != "recreated" {
				t.Fatalf("directory index holds %v, want [recreated]", members)
			}

			if names := listNames(t, store, dir); len(names) != 1 || names[0] != "recreated" {
				t.Fatalf("listed %v, want [recreated]", names)
			}
		})
	}
}

func TestRemoveOrphanedDirectoryListMemberKeepsDirectoryWithChildren(t *testing.T) {
	for _, keyPrefix := range []string{"", "sw:"} {
		t.Run("keyPrefix="+keyPrefix, func(t *testing.T) {
			store, dir := newTestStore(t, keyPrefix)

			sub := dir.Child("sub")
			insertTestEntry(t, store, sub, 0)
			insertTestEntry(t, store, sub.Child("kid"), 0)
			defer func() {
				if err := store.DeleteFolderChildren(context.Background(), sub); err != nil {
					t.Errorf("cleanup %s children: %v", sub, err)
				}
			}()

			// evict the directory's own value while its child index is live
			if err := store.Client.Del(context.Background(), store.getKey(string(sub))).Err(); err != nil {
				t.Fatalf("drop value key: %v", err)
			}

			if names := listNames(t, store, dir); len(names) != 0 {
				t.Fatalf("listed %v, want none", names)
			}

			if members := indexMembers(t, store, dir); len(members) != 1 || members[0] != "sub" {
				t.Fatalf("directory index holds %v, want [sub]", members)
			}

			if exists, err := store.Client.Exists(context.Background(), store.getKey(string(sub.Child("kid")))).Result(); err != nil || exists != 1 {
				t.Fatalf("child value key exists=%d err=%v, want it kept", exists, err)
			}
		})
	}
}

func TestRemoveOrphanedDirectoryListMemberSkipsSuperLargeDirectory(t *testing.T) {
	store, dir := newTestStore(t, "")
	store.loadSuperLargeDirectories([]string{string(dir)})

	// a member left from before the directory became super large
	if err := store.Client.ZAdd(context.Background(), store.getKey(genDirectoryListKey(string(dir))), redis.Z{Score: 0, Member: "legacy"}).Err(); err != nil {
		t.Fatalf("plant legacy member: %v", err)
	}

	store.removeOrphanedDirectoryListMember(context.Background(), dir, "legacy")

	if members := indexMembers(t, store, dir); len(members) != 1 || members[0] != "legacy" {
		t.Fatalf("directory index holds %v, want [legacy] untouched", members)
	}
}

func TestListDirectoryEntriesRemovesIndexMembersExpiredByRedis(t *testing.T) {
	store, dir := newTestStore(t, "")

	insertTestEntry(t, store, dir.Child("ttl"), 1)

	time.Sleep(1500 * time.Millisecond)

	if exists, err := store.Client.Exists(context.Background(), store.getKey(string(dir.Child("ttl")))).Result(); err != nil {
		t.Fatalf("check value key: %v", err)
	} else if exists != 0 {
		t.Fatal("redis did not expire the value key, the logical expiry path is not being bypassed")
	}

	if names := listNames(t, store, dir); len(names) != 0 {
		t.Fatalf("listed %v, want none", names)
	}

	if members := indexMembers(t, store, dir); len(members) != 0 {
		t.Fatalf("directory index holds %v, want none", members)
	}
}

// logically expired (Crtime + TtlSec long past) while the physical key survives,
// because the redis TTL re-arms from the SET time
func insertLogicallyExpiredTestEntry(t *testing.T, store *UniversalRedis2Store, path util.FullPath) {
	t.Helper()

	created := time.Now().Add(-time.Hour)
	if err := store.InsertEntry(context.Background(), &filer.Entry{
		FullPath: path,
		Attr:     filer.Attr{Crtime: created, Mtime: created, Mode: 0644, TtlSec: 60},
	}); err != nil {
		t.Fatalf("InsertEntry %s: %v", path, err)
	}
}

func TestListDirectoryEntriesDeletesLogicallyExpiredEntries(t *testing.T) {
	for _, keyPrefix := range []string{"", "sw:"} {
		t.Run("keyPrefix="+keyPrefix, func(t *testing.T) {
			store, dir := newTestStore(t, keyPrefix)

			insertLogicallyExpiredTestEntry(t, store, dir.Child("stale"))

			if names := listNames(t, store, dir); len(names) != 0 {
				t.Fatalf("listed %v, want none", names)
			}

			if exists, err := store.Client.Exists(context.Background(), store.getKey(string(dir.Child("stale")))).Result(); err != nil || exists != 0 {
				t.Fatalf("value key exists=%d err=%v, want it deleted", exists, err)
			}

			if members := indexMembers(t, store, dir); len(members) != 0 {
				t.Fatalf("directory index holds %v, want none", members)
			}
		})
	}
}

func TestDeleteExpiredEntryKeepsRecreatedValue(t *testing.T) {
	store, dir := newTestStore(t, "")

	path := dir.Child("phoenix")
	insertLogicallyExpiredTestEntry(t, store, path)
	// a recreate lands after the lister decided to expire the old value
	insertTestEntry(t, store, path, 0)

	store.deleteExpiredEntry(context.Background(), dir, path, "phoenix")

	if names := listNames(t, store, dir); len(names) != 1 || names[0] != "phoenix" {
		t.Fatalf("listed %v, want [phoenix]", names)
	}

	if members := indexMembers(t, store, dir); len(members) != 1 || members[0] != "phoenix" {
		t.Fatalf("directory index holds %v, want [phoenix]", members)
	}
}

func TestDeleteIfUnchangedScriptOnlyDeletesSameBytes(t *testing.T) {
	store, dir := newTestStore(t, "")

	key := store.getKey(string(dir.Child("guarded")))
	if err := store.Client.Set(context.Background(), key, "v1", 0).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}

	if deleted, err := deleteIfUnchangedScript.Run(context.Background(), store.Client, []string{key}, []byte("v2")).Int(); err != nil || deleted != 0 {
		t.Fatalf("deleted=%d err=%v, want no delete on changed bytes", deleted, err)
	}

	if deleted, err := deleteIfUnchangedScript.Run(context.Background(), store.Client, []string{key}, []byte("v1")).Int(); err != nil || deleted != 1 {
		t.Fatalf("deleted=%d err=%v, want delete on matching bytes", deleted, err)
	}

	if exists, err := store.Client.Exists(context.Background(), key).Result(); err != nil || exists != 0 {
		t.Fatalf("exists=%d err=%v, want key gone", exists, err)
	}
}
