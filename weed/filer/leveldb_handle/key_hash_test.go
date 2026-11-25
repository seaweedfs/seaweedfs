package leveldb_handle

import (
	"strings"
	"testing"

	"github.com/syndtr/goleveldb/leveldb/storage"
)

func openMemDBs(t *testing.T, n int) []*DB {
	t.Helper()
	var dbs []*DB
	for i := 0; i < n; i++ {
		db, err := Open(storage.NewMemStorage(), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		dbs = append(dbs, db)
	}
	return dbs
}

func TestCheckKeyHashMarksEmptyDatabases(t *testing.T) {
	dbs := openMemDBs(t, 2)
	if err := CheckKeyHash(dbs, true, false); err != nil {
		t.Fatalf("empty databases: %v", err)
	}
	if err := CheckKeyHash(dbs, true, false); err != nil {
		t.Fatalf("marked databases: %v", err)
	}
	err := CheckKeyHash(dbs, false, false)
	if err == nil || !strings.Contains(err.Error(), "digest sha256") {
		t.Fatalf("marked databases opened with digest md5: %v", err)
	}
}

func TestCheckKeyHashRefusesMd5Keys(t *testing.T) {
	dbs := openMemDBs(t, 2)
	if err := CheckKeyHash(dbs, false, false); err != nil {
		t.Fatalf("empty databases with digest md5: %v", err)
	}
	if err := dbs[1].Put([]byte("0123456789abcdefname"), []byte("entry"), nil); err != nil {
		t.Fatal(err)
	}
	if err := CheckKeyHash(dbs, false, false); err != nil {
		t.Fatalf("md5 keys with digest md5: %v", err)
	}
	if err := CheckKeyHash(dbs, true, false); err == nil || !strings.Contains(err.Error(), "MD5") {
		t.Fatalf("md5 keys with digest sha256: %v", err)
	}
	if _, err := dbs[0].Get(sha256KeyMarker, nil); err == nil {
		t.Fatalf("a refused check must not mark the empty database")
	}
}

func TestCheckKeyHashReadOnlyLeavesEmptyDatabasesUnmarked(t *testing.T) {
	dbs := openMemDBs(t, 1)
	if err := CheckKeyHash(dbs, true, true); err != nil {
		t.Fatal(err)
	}
	if err := CheckKeyHash(dbs, false, false); err != nil {
		t.Fatalf("read-only check marked the database: %v", err)
	}
}
