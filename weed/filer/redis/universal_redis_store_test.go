package redis

import (
	"context"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func newTestStore(t *testing.T) (*UniversalRedisStore, util.FullPath) {
	t.Helper()

	if os.Getenv("RUN_REDIS_TESTS") != "1" {
		t.Skip("redis tests are disabled. Start a redis-server and set RUN_REDIS_TESTS=1 to enable, REDIS_ADDR defaults to 127.0.0.1:6379.")
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

	store := &UniversalRedisStore{Client: client}

	dir := util.FullPath(fmt.Sprintf("/redis_test_%d", time.Now().UnixNano()))
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

func insertTestEntry(t *testing.T, store *UniversalRedisStore, path util.FullPath) {
	t.Helper()

	now := time.Now()
	if err := store.InsertEntry(context.Background(), &filer.Entry{
		FullPath: path,
		Attr:     filer.Attr{Crtime: now, Mtime: now, Mode: 0644},
	}); err != nil {
		t.Fatalf("InsertEntry %s: %v", path, err)
	}
}

func listNames(t *testing.T, store *UniversalRedisStore, dir util.FullPath) []string {
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

func indexMembers(t *testing.T, store *UniversalRedisStore, dir util.FullPath) []string {
	t.Helper()

	members, err := store.Client.SMembers(context.Background(), genDirectoryListKey(string(dir))).Result()
	if err != nil {
		t.Fatalf("read directory index of %s: %v", dir, err)
	}
	slices.Sort(members)
	return members
}

func TestListDirectoryEntriesRemovesOrphanedIndexMembers(t *testing.T) {
	store, dir := newTestStore(t)

	insertTestEntry(t, store, dir.Child("alive"))
	insertTestEntry(t, store, dir.Child("orphan"))

	if err := store.Client.Del(context.Background(), string(dir.Child("orphan"))).Err(); err != nil {
		t.Fatalf("drop value key: %v", err)
	}

	if names := listNames(t, store, dir); len(names) != 1 || names[0] != "alive" {
		t.Fatalf("listed %v, want [alive]", names)
	}

	if members := indexMembers(t, store, dir); len(members) != 1 || members[0] != "alive" {
		t.Fatalf("directory index holds %v, want [alive]", members)
	}
}

func TestRemoveOrphanedDirectoryListMemberKeepsRecreatedEntry(t *testing.T) {
	store, dir := newTestStore(t)

	insertTestEntry(t, store, dir.Child("recreated"))

	store.removeOrphanedDirectoryListMember(context.Background(), dir, "recreated")

	if members := indexMembers(t, store, dir); len(members) != 1 || members[0] != "recreated" {
		t.Fatalf("directory index holds %v, want [recreated]", members)
	}

	if names := listNames(t, store, dir); len(names) != 1 || names[0] != "recreated" {
		t.Fatalf("listed %v, want [recreated]", names)
	}
}

func TestRemoveOrphanedDirectoryListMemberKeepsDirectoryWithChildren(t *testing.T) {
	store, dir := newTestStore(t)

	sub := dir.Child("sub")
	insertTestEntry(t, store, sub)
	insertTestEntry(t, store, sub.Child("kid"))
	defer func() {
		if err := store.DeleteFolderChildren(context.Background(), sub); err != nil {
			t.Errorf("cleanup %s children: %v", sub, err)
		}
	}()

	// evict the directory's own value while its child index is live
	if err := store.Client.Del(context.Background(), string(sub)).Err(); err != nil {
		t.Fatalf("drop value key: %v", err)
	}

	if names := listNames(t, store, dir); len(names) != 0 {
		t.Fatalf("listed %v, want none", names)
	}

	if members := indexMembers(t, store, dir); len(members) != 1 || members[0] != "sub" {
		t.Fatalf("directory index holds %v, want [sub]", members)
	}

	if exists, err := store.Client.Exists(context.Background(), string(sub.Child("kid"))).Result(); err != nil || exists != 1 {
		t.Fatalf("child value key exists=%d err=%v, want it kept", exists, err)
	}
}

func TestDeleteFolderChildrenRemovesTheListing(t *testing.T) {
	store, dir := newTestStore(t)
	ctx := context.Background()

	insertTestEntry(t, store, dir.Child("obj"))

	// The directory entry going away must not take the listing with it; nothing
	// else records that the child is under this directory.
	if err := store.DeleteEntry(ctx, dir); err != nil {
		t.Fatalf("DeleteEntry %s: %v", dir, err)
	}
	if names := listNames(t, store, dir); len(names) != 1 || names[0] != "obj" {
		t.Errorf("child should still be listed after the directory entry is deleted, got %v", names)
	}

	// Deleting the children takes the listing with them, rather than leaving it behind.
	if err := store.DeleteFolderChildren(ctx, dir); err != nil {
		t.Fatalf("DeleteFolderChildren %s: %v", dir, err)
	}
	if n, err := store.Client.Exists(ctx, genDirectoryListKey(string(dir))).Result(); err != nil {
		t.Fatalf("exists %s: %v", dir, err)
	} else if n != 0 {
		t.Errorf("listing key should be removed with the children, still present")
	}
}
