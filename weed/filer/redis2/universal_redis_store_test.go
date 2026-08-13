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
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("connect to redis at %s: %v", addr, err)
	}

	store := &UniversalRedis2Store{Client: client, keyPrefix: keyPrefix}
	store.loadSuperLargeDirectories(nil)

	dir := util.FullPath(fmt.Sprintf("/redis2_test_%d", time.Now().UnixNano()))
	t.Cleanup(func() {
		store.DeleteFolderChildren(ctx, dir)
		store.DeleteEntry(ctx, dir)
		client.Close()
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
		_, name := entry.FullPath.DirAndName()
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
	store, dir := newTestStore(t, "")

	path := dir.Child("recreated")
	insertTestEntry(t, store, path, 0)

	store.removeOrphanedDirectoryListMember(context.Background(), dir, "recreated")

	if members := indexMembers(t, store, dir); len(members) != 1 || members[0] != "recreated" {
		t.Fatalf("directory index holds %v, want [recreated]", members)
	}

	if names := listNames(t, store, dir); len(names) != 1 || names[0] != "recreated" {
		t.Fatalf("listed %v, want [recreated]", names)
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
