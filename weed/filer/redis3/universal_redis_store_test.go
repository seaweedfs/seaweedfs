package redis3

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-redsync/redsync/v4"
	goredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func newTestStore(t *testing.T) (*UniversalRedis3Store, util.FullPath) {
	t.Helper()

	if os.Getenv("RUN_REDIS_TESTS") != "1" {
		t.Skip("redis3 tests are disabled. Start a redis-server and set RUN_REDIS_TESTS=1 to enable, REDIS_ADDR defaults to 127.0.0.1:6379.")
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

	store := &UniversalRedis3Store{Client: client, redsync: redsync.New(goredis.NewPool(client))}

	dir := util.FullPath(fmt.Sprintf("/redis3_test_%d", time.Now().UnixNano()))
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

func listNames(t *testing.T, store *UniversalRedis3Store, dir util.FullPath) []string {
	t.Helper()

	names := []string{}
	if _, err := store.ListDirectoryEntries(context.Background(), dir, "", true, 100, func(entry *filer.Entry) (bool, error) {
		names = append(names, entry.Name())
		return true, nil
	}); err != nil {
		t.Fatalf("list %s: %v", dir, err)
	}
	return names
}

func TestDeleteEntryKeepsChildListing(t *testing.T) {
	store, dir := newTestStore(t)
	ctx := context.Background()

	now := time.Now()
	child := dir.Child("obj")
	if err := store.InsertEntry(ctx, &filer.Entry{
		FullPath: child,
		Attr:     filer.Attr{Crtime: now, Mtime: now, Mode: 0644},
	}); err != nil {
		t.Fatalf("InsertEntry %s: %v", child, err)
	}

	// The listing is the only record that the child sits under this directory, so
	// removing the directory entry must leave it alone. An entry that arrived after
	// the caller judged the directory empty would otherwise become unreachable.
	if err := store.DeleteEntry(ctx, dir); err != nil {
		t.Fatalf("DeleteEntry %s: %v", dir, err)
	}
	if names := listNames(t, store, dir); len(names) != 1 || names[0] != "obj" {
		t.Errorf("child should still be listed after the directory entry is deleted, got %v", names)
	}

	// DeleteFolderChildren owns that cleanup, and takes the listing with it
	if err := store.DeleteFolderChildren(ctx, dir); err != nil {
		t.Fatalf("DeleteFolderChildren %s: %v", dir, err)
	}
	if names := listNames(t, store, dir); len(names) != 0 {
		t.Errorf("listing should be empty once the children are deleted, got %v", names)
	}
	if n, err := store.Client.Exists(ctx, genDirectoryListKey(string(dir))).Result(); err != nil {
		t.Fatalf("exists %s: %v", dir, err)
	} else if n != 0 {
		t.Errorf("listing key should be removed with the children, still present")
	}
}

func TestRemovingTheLastChildDropsTheListing(t *testing.T) {
	store, dir := newTestStore(t)
	ctx := context.Background()

	now := time.Now()
	child := dir.Child("obj")
	if err := store.InsertEntry(ctx, &filer.Entry{
		FullPath: child,
		Attr:     filer.Attr{Crtime: now, Mtime: now, Mode: 0644},
	}); err != nil {
		t.Fatalf("InsertEntry %s: %v", child, err)
	}

	// An emptied listing must not leave its header behind: cleanup deletes such a
	// folder without touching the listing, so the key would never be collected.
	if err := store.DeleteEntry(ctx, child); err != nil {
		t.Fatalf("DeleteEntry %s: %v", child, err)
	}
	if n, err := store.Client.Exists(ctx, genDirectoryListKey(string(dir))).Result(); err != nil {
		t.Fatalf("exists %s: %v", dir, err)
	} else if n != 0 {
		t.Errorf("listing key should be gone once the last child is removed, still present")
	}
}
