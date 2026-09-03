//go:build (linux || darwin || windows) && sqlite

package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// A listing whose callback reads a hard link needs a second connection while it
// still holds the one its rows are on, and the store allows exactly one.
func TestListDirectoryOverHardLinkDoesNotDeadlock(t *testing.T) {
	store := &SqliteStore{}
	if err := store.initialize(filepath.Join(t.TempDir(), "filer.db"),
		`CREATE TABLE IF NOT EXISTS "%s" (dirhash BIGINT, name VARCHAR(1000), directory TEXT, meta BLOB, PRIMARY KEY (dirhash, name)) WITHOUT ROWID;`,
		"INSERT INTO \"%s\"(dirhash,name,directory,meta)VALUES(?,?,?,?) ON CONFLICT(dirhash,name) DO UPDATE SET directory=excluded.directory, meta=excluded.meta;"); err != nil {
		t.Fatal(err)
	}
	defer store.Shutdown()

	wrapper := filer.NewFilerStoreWrapper(store)
	ctx := context.Background()
	entry := &filer.Entry{
		FullPath:   util.FullPath("/buckets/hlbucket/f0001"),
		Attr:       filer.Attr{Mode: 0644, Mtime: time.Now(), Crtime: time.Now()},
		HardLinkId: filer.HardLinkId("hardlink-00000001"),
	}
	if err := wrapper.InsertEntry(ctx, entry); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := wrapper.ListDirectoryEntries(ctx, util.FullPath("/buckets/hlbucket"), "", true, 10,
			func(*filer.Entry) (bool, error) { return true, nil })
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("list: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("one listing over one hard-linked entry never returned: %+v", store.DB.Stats())
	}
}
