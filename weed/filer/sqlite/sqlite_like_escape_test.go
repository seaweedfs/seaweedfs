//go:build sqlite

package sqlite

import (
	"context"
	"database/sql"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/mysql"
	"github.com/seaweedfs/seaweedfs/weed/util"
	_ "modernc.org/sqlite"
)

func TestListDirectoryPrefixedEntriesEscapesLikeWildcards(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE filemeta (dirhash BIGINT, name VARCHAR(1000), directory TEXT, meta BLOB, PRIMARY KEY (dirhash, name))`)
	if err != nil {
		t.Fatal(err)
	}

	store := &SqliteStore{}
	store.SqlGenerator = &mysql.SqlGenMysql{}
	store.DB = db

	ctx := context.Background()
	names := []string{
		"call_03_KNlHPt6grkZMe4WZVwQi9710", // target: literal _ prefix
		"callX03XKNlZZZ",                   // would match if _ were a wildcard
		"call_99_other",
		"100%_done", // % and _ in the name
		"100aXdone", // would match prefix "100%_" if unescaped
		"bang!file",
	}
	for _, n := range names {
		entry := &filer.Entry{FullPath: util.FullPath("/dir/" + n)}
		if err := store.InsertEntry(ctx, entry); err != nil {
			t.Fatalf("insert %s: %v", n, err)
		}
	}

	list := func(prefix string) []string {
		var got []string
		_, err := store.ListDirectoryPrefixedEntries(ctx, "/dir", "", false, 100, prefix, func(e *filer.Entry) (bool, error) {
			got = append(got, e.Name())
			return true, nil
		})
		if err != nil {
			t.Fatalf("list %q: %v", prefix, err)
		}
		return got
	}

	if got := list("call_03_"); len(got) != 1 || got[0] != "call_03_KNlHPt6grkZMe4WZVwQi9710" {
		t.Fatalf("prefix with _: got %v", got)
	}
	if got := list("call_"); len(got) != 2 {
		t.Fatalf("prefix call_: got %v", got)
	}
	if got := list("100%_"); len(got) != 1 || got[0] != "100%_done" {
		t.Fatalf("prefix with %%: got %v", got)
	}
	if got := list("bang!"); len(got) != 1 || got[0] != "bang!file" {
		t.Fatalf("prefix with !: got %v", got)
	}
	if got := list(""); len(got) != len(names) {
		t.Fatalf("empty prefix: got %v", got)
	}
}
