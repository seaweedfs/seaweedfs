//go:build (linux || darwin || windows) && sqlite

package sqlite

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// A rename committed while a scan is running is seen under exactly one of its
// names. The scan reads from one snapshot, so unlike a directory walk it can
// neither miss the entry when it moves into a directory already listed, nor
// count it twice when it moves into one not listed yet.
func TestScanEntriesSeesConcurrentRenameExactlyOnce(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "filer.db")
	reader := &SqliteStore{}
	if err := reader.initialize(dbFile, createTableSql, upsertQuerySql); err != nil {
		t.Fatal(err)
	}
	defer reader.Shutdown()
	// WAL lets the writer commit while the scan's read transaction is open.
	if _, err := reader.DB.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}
	writer := &SqliteStore{}
	if err := writer.initialize(dbFile, createTableSql, upsertQuerySql); err != nil {
		t.Fatal(err)
	}
	defer writer.Shutdown()

	ctx := context.Background()
	mk := func(p string) *filer.Entry {
		return &filer.Entry{FullPath: util.FullPath(p), Attr: filer.Attr{Mode: 0644, Mtime: time.Now(), Crtime: time.Now()}}
	}
	for _, p := range []string{"/ckpt/a", "/ckpt/b", "/ckpt/tmp/x"} {
		if err := reader.InsertEntry(ctx, mk(p)); err != nil {
			t.Fatal(err)
		}
	}

	defer func(n int) { abstract_sql.SetScanPageSizeForTest(n) }(abstract_sql.SetScanPageSizeForTest(2))

	seen := map[string]int{}
	renamed := false
	err := reader.ScanEntries(ctx, "/", func(_ context.Context, e *filer.Entry) error {
		seen[string(e.FullPath)]++
		if renamed {
			return nil
		}
		renamed = true
		// /ckpt/tmp/x -> /ckpt/x, committed by another connection mid-scan.
		if err := writer.InsertEntry(ctx, mk("/ckpt/x")); err != nil {
			return err
		}
		return writer.DeleteEntry(ctx, util.FullPath("/ckpt/tmp/x"))
	})
	if err != nil {
		t.Fatal(err)
	}
	if !renamed {
		t.Fatal("scan yielded nothing")
	}
	if seen["/ckpt/a"] != 1 || seen["/ckpt/b"] != 1 {
		t.Errorf("untouched entries seen %d and %d times, want once each", seen["/ckpt/a"], seen["/ckpt/b"])
	}
	if old, new := seen["/ckpt/tmp/x"], seen["/ckpt/x"]; old+new != 1 {
		t.Errorf("renamed entry seen %d time(s) under the old name and %d under the new, want exactly one in total", old, new)
	} else if old != 1 {
		t.Errorf("the rename committed after the snapshot was taken, so the scan must show the old name")
	}
	if _, err := reader.FindEntry(ctx, util.FullPath("/ckpt/x")); err != nil {
		t.Errorf("once the scan is over the new name must be visible: %v", err)
	}
}

// Rows that are not entries, key-value records and a regular file under
// /buckets with a bucket-like name, are stepped over; a scan rooted at a bucket
// yields the bucket's own row (default table) and its contents (bucket table).
func TestScanEntriesRowsAndBucketTables(t *testing.T) {
	store := &SqliteStore{}
	if err := store.initialize(filepath.Join(t.TempDir(), "filer.db"), createTableSql, upsertQuerySql); err != nil {
		t.Fatal(err)
	}
	defer store.Shutdown()
	if _, err := store.DB.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	now := time.Now()
	// The filer creates bucket tables on bucket creation; the store layer alone does not.
	if err := store.CreateTable(ctx, "realbucket"); err != nil {
		t.Fatal(err)
	}
	for _, e := range []*filer.Entry{
		{FullPath: "/buckets", Attr: filer.Attr{Mode: 0755 | os.ModeDir, Mtime: now, Crtime: now}},
		{FullPath: "/buckets/realbucket", Attr: filer.Attr{Mode: 0755 | os.ModeDir, Mtime: now, Crtime: now}},
		{FullPath: "/buckets/realbucket/f1", Attr: filer.Attr{Mode: 0644, Mtime: now, Crtime: now}},
		{FullPath: "/buckets/readme", Attr: filer.Attr{Mode: 0644, Mtime: now, Crtime: now}},
		{FullPath: "/topics/t1", Attr: filer.Attr{Mode: 0644, Mtime: now, Crtime: now}},
	} {
		if err := store.InsertEntry(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []string{"filer.store.id", "/looks/like/a/path", "x"} {
		if err := store.KvPut(ctx, []byte(key), []byte("opaque value, not an Entry")); err != nil {
			t.Fatal(err)
		}
	}

	collect := func(root string) []string {
		var got []string
		if err := store.ScanEntries(ctx, util.FullPath(root), func(_ context.Context, e *filer.Entry) error {
			got = append(got, string(e.FullPath))
			return nil
		}); err != nil {
			t.Fatalf("scan %s: %v", root, err)
		}
		sort.Strings(got)
		return got
	}
	if got, want := collect("/"), []string{"/buckets", "/buckets/readme", "/buckets/realbucket", "/buckets/realbucket/f1", "/topics/t1"}; !reflect.DeepEqual(got, want) {
		t.Errorf("scan /: got %v, want %v", got, want)
	}
	if got, want := collect("/buckets/realbucket"), []string{"/buckets/realbucket", "/buckets/realbucket/f1"}; !reflect.DeepEqual(got, want) {
		t.Errorf("scan /buckets/realbucket: got %v, want %v", got, want)
	}

	// A bucket directory whose table does not exist fails the scan: from the
	// snapshot it cannot be told from a table dropped a moment ago.
	if err := store.InsertEntry(ctx, &filer.Entry{FullPath: "/buckets/ghostbucket", Attr: filer.Attr{Mode: 0755 | os.ModeDir, Mtime: now, Crtime: now}}); err != nil {
		t.Fatal(err)
	}
	err := store.ScanEntries(ctx, "/", func(context.Context, *filer.Entry) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "ghostbucket") {
		t.Fatalf("scan over a bucket without a table must fail naming it, got %v", err)
	}
}

// Without WAL the store declines the scan rather than block writers for its
// whole duration.
func TestScanEntriesDeclinedWithoutWAL(t *testing.T) {
	store := &SqliteStore{}
	if err := store.initialize(filepath.Join(t.TempDir(), "filer.db"), createTableSql, upsertQuerySql); err != nil {
		t.Fatal(err)
	}
	defer store.Shutdown()
	err := store.ScanEntries(context.Background(), "/", func(context.Context, *filer.Entry) error { return nil })
	if !errors.Is(err, filer.ErrUnsupportedEntryScan) {
		t.Fatalf("err = %v, want ErrUnsupportedEntryScan", err)
	}
}

// A store whose pool is one connection, and no scan connection of its own,
// declines rather than hold that connection for the whole scan.
func TestScanEntriesDeclinedOnSingleConnectionPool(t *testing.T) {
	store := &SqliteStore{}
	if err := store.initialize(filepath.Join(t.TempDir(), "filer.db"), createTableSql, upsertQuerySql); err != nil {
		t.Fatal(err)
	}
	defer store.Shutdown()
	if _, err := store.DB.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}
	store.ScanDB.Close()
	store.ScanDB = nil
	err := store.ScanEntries(context.Background(), "/", func(context.Context, *filer.Entry) error { return nil })
	if !errors.Is(err, filer.ErrUnsupportedEntryScan) {
		t.Fatalf("err = %v, want ErrUnsupportedEntryScan", err)
	}
}

// A hard-linked entry's chunks live in the shared key-value record, not in
// its path row; the wrapper resolves them from inside the snapshot, on the
// single connection the scan holds.
func TestScanEntriesResolvesHardLinks(t *testing.T) {
	store := &SqliteStore{}
	if err := store.initialize(filepath.Join(t.TempDir(), "filer.db"), createTableSql, upsertQuerySql); err != nil {
		t.Fatal(err)
	}
	defer store.Shutdown()
	if _, err := store.DB.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}
	wrapper := filer.NewFilerStoreWrapper(store)
	ctx := context.Background()
	now := time.Now()
	linked := &filer.Entry{
		FullPath:        "/buckets/hlbucket/f0001",
		Attr:            filer.Attr{Mode: 0644, Mtime: now, Crtime: now},
		HardLinkId:      filer.HardLinkId("hardlink-00000001"),
		HardLinkCounter: 2,
		Chunks:          []*filer_pb.FileChunk{{FileId: "3,01637037d6", Size: 4096}},
	}
	if err := wrapper.InsertEntry(ctx, linked); err != nil {
		t.Fatal(err)
	}

	var got *filer.Entry
	if err := wrapper.ScanEntries(ctx, "/", func(_ context.Context, e *filer.Entry) error {
		if e.FullPath == linked.FullPath {
			got = e
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got.GetChunks()) != 1 || got.GetChunks()[0].FileId != "3,01637037d6" || got.HardLinkCounter != 2 {
		t.Fatalf("hard-linked entry not resolved from its key-value record: %+v", got)
	}

	// Once the record is gone the path row is all there is; the scan hands it
	// over instead of failing on that entry every time it runs.
	if err := store.KvDelete(ctx, []byte(linked.HardLinkId)); err != nil {
		t.Fatal(err)
	}
	got = nil
	if err := wrapper.ScanEntries(ctx, "/", func(_ context.Context, e *filer.Entry) error {
		if e.FullPath == linked.FullPath {
			got = e
		}
		return nil
	}); err != nil {
		t.Fatalf("a missing hard-link record must not fail the scan: %v", err)
	}
	if got == nil {
		t.Fatal("entry with a missing hard-link record not handed over")
	}
}

// The keyset page must be a primary-key seek; an OR-shaped cursor predicate
// makes SQLite scan the table from the start for every page.
func TestScanPageQuerySeeksPrimaryKey(t *testing.T) {
	store := &SqliteStore{}
	if err := store.initialize(filepath.Join(t.TempDir(), "filer.db"), createTableSql, upsertQuerySql); err != nil {
		t.Fatal(err)
	}
	defer store.Shutdown()
	sqlText, args := store.GetSqlScanPage("filemeta", int64(-1), "", 10)
	rows, err := store.DB.Query("EXPLAIN QUERY PLAN "+sqlText, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	joined := strings.Join(plan, " | ")
	if !strings.Contains(joined, "SEARCH") || strings.Contains(joined, "SCAN") {
		t.Fatalf("page query does not seek the primary key: %s", joined)
	}
}

// A bucket name only the process cache remembers may have its table dropped
// meanwhile; the scan skips it instead of failing.
func TestScanEntriesSkipsCachedOnlyBucketTable(t *testing.T) {
	store := &SqliteStore{}
	if err := store.initialize(filepath.Join(t.TempDir(), "filer.db"), createTableSql, upsertQuerySql); err != nil {
		t.Fatal(err)
	}
	defer store.Shutdown()
	if _, err := store.DB.Exec("PRAGMA journal_mode=WAL"); err != nil {
		t.Fatal(err)
	}
	store.OnBucketCreation("gonebucket") // the table is never created
	if err := store.ScanEntries(context.Background(), "/", func(context.Context, *filer.Entry) error { return nil }); err != nil {
		t.Fatalf("cached-only bucket with a dropped table must not fail the scan: %v", err)
	}
}
