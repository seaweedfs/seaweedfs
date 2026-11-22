//go:build (linux || darwin || windows) && sqlite

package weed_server

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/sqlite"
)

// The SQLite store has one connection and the rename's transaction holds it,
// so a check that reads outside the transaction waits for it forever.
func TestStreamRenameOntoDirectoryOnSqlite(t *testing.T) {
	store := &sqlite.SqliteStore{}
	if err := store.Initialize(testConfig{"dbFile": filepath.Join(t.TempDir(), "filer.db")}, ""); err != nil {
		t.Fatal(err)
	}
	defer store.Shutdown()
	ctx := context.Background()
	for _, entry := range []*filer.Entry{
		newDirectoryEntry("/a", 101),
		newFileEntry("/a/y.txt", 102),
		newDirectoryEntry("/e", 103),
	} {
		if err := store.InsertEntry(ctx, entry); err != nil {
			t.Fatal(err)
		}
	}

	done := make(chan error, 1)
	go func() {
		_, err := streamRename(t, store, "a", "e")
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("StreamRenameEntry: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("StreamRenameEntry onto an existing directory never returned: %+v", store.DB.Stats())
	}
	if _, err := store.FindEntry(ctx, "/e/y.txt"); err != nil {
		t.Fatalf("moved child: %v", err)
	}
}
