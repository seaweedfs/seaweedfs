//go:build (linux || darwin || windows) && sqlite

package sqlite

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	createTableSql = `CREATE TABLE IF NOT EXISTS "%s" (dirhash BIGINT, name VARCHAR(1000), directory TEXT, meta BLOB, PRIMARY KEY (dirhash, name)) WITHOUT ROWID;`
	upsertQuerySql = `INSERT INTO "%s"(dirhash,name,directory,meta)VALUES(?,?,?,?) ON CONFLICT(dirhash,name) DO UPDATE SET directory=excluded.directory, meta=excluded.meta;`
)

// A listing whose callback reads a hard link needs a second connection while it
// still holds the one its rows are on, and the store allows exactly one.
func TestListDirectoryOverHardLinkDoesNotDeadlock(t *testing.T) {
	store := &SqliteStore{}
	if err := store.initialize(filepath.Join(t.TempDir(), "filer.db"), createTableSql, upsertQuerySql); err != nil {
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

func TestSqliteDSN(t *testing.T) {
	if got, want := sqliteDSN("/data/filer.db"), "/data/filer.db?_pragma=busy_timeout(10000)"; got != want {
		t.Errorf("sqliteDSN = %q, want %q", got, want)
	}
	if got, want := sqliteDSN("file:/data/filer.db?cache=shared"), "file:/data/filer.db?cache=shared&_pragma=busy_timeout(10000)"; got != want {
		t.Errorf("sqliteDSN with options = %q, want %q", got, want)
	}
	memory := sqliteDSN(":memory:")
	if !strings.HasPrefix(memory, "file:seaweedfs") || !strings.Contains(memory, "mode=memory&cache=shared") ||
		!strings.HasSuffix(memory, "&_pragma=busy_timeout(10000)") {
		t.Errorf("sqliteDSN(:memory:) = %q, want a named shared-memory URI carrying the busy timeout", memory)
	}
}

// Both pools have to open the same database: the table is created on one and
// the key-value operations run on the other.
func TestInMemoryStoreSharesOneDatabase(t *testing.T) {
	store := &SqliteStore{}
	if err := store.initialize(":memory:", createTableSql, upsertQuerySql); err != nil {
		t.Fatal(err)
	}
	defer store.Shutdown()

	ctx := context.Background()
	key := []byte("hardlink-00000001")
	if err := store.KvPut(ctx, key, []byte("value")); err != nil {
		t.Fatalf("KvPut: %v", err)
	}
	value, err := store.KvGet(ctx, key)
	if err != nil {
		t.Fatalf("KvGet: %v", err)
	}
	if string(value) != "value" {
		t.Errorf("KvGet = %q, want value", value)
	}
}
