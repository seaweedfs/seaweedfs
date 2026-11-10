package abstract_sql

import (
	"context"
	"reflect"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// The bucket owner index lives at /buckets/.system/owners/<owner>/<bucket>.
// .system is an internal folder, not an S3 bucket, so getTxOrDB must route it
// to the default table rather than reject it as an invalid bucket name.
func TestGetTxOrDBInternalSystemFolder(t *testing.T) {
	store := &AbstractSqlStore{SupportBucketTable: true}
	ctx := context.Background()

	cases := []struct {
		path          string
		isForChildren bool
	}{
		{"/buckets/.system/owners", false},
		{"/buckets/.system/owners/foo/bucket1", false},
		{"/buckets/.system/owners/foo", true},
	}
	for _, c := range cases {
		_, bucket, shortPath, err := store.getTxOrDB(ctx, util.FullPath(c.path), c.isForChildren)
		if err != nil {
			t.Errorf("getTxOrDB(%s): unexpected error: %v", c.path, err)
		}
		if bucket != DEFAULT_TABLE {
			t.Errorf("getTxOrDB(%s): bucket = %q, want %q", c.path, bucket, DEFAULT_TABLE)
		}
		if string(shortPath) != c.path {
			t.Errorf("getTxOrDB(%s): shortPath = %q, want %q", c.path, shortPath, c.path)
		}
	}
}

func TestGetTxOrDBRealBucket(t *testing.T) {
	store := &AbstractSqlStore{SupportBucketTable: true, dbs: map[string]bool{"mybucket": true}}
	ctx := context.Background()

	_, bucket, shortPath, err := store.getTxOrDB(ctx, util.FullPath("/buckets/mybucket/dir/file.txt"), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bucket != "mybucket" {
		t.Errorf("bucket = %q, want mybucket", bucket)
	}
	if string(shortPath) != "/dir/file.txt" {
		t.Errorf("shortPath = %q, want /dir/file.txt", shortPath)
	}
}

func TestLikePattern(t *testing.T) {
	cases := []struct {
		prefix, want string
	}{
		{"", "%"},
		{"plain", "plain%"},
		{"call_03_KNl", "call!_03!_KNl%"},
		{"100%", "100!%%"},
		{"a!b", "a!!b%"},
		{`a\b`, `a\b%`},
	}
	for _, c := range cases {
		if got := LikePattern(c.prefix); got != c.want {
			t.Errorf("LikePattern(%q) = %q, want %q", c.prefix, got, c.want)
		}
	}
}

func TestSplitPoolForKv(t *testing.T) {
	cases := []struct {
		maxOpen, mainOpen, kvOpen int
	}{
		{0, 0, 0},   // unbounded: nothing waits, one pool is enough
		{-1, -1, 0}, // same, however the operator spelled it
		{50, 42, 8},
		{16, 12, 4},
		{4, 3, 1},
		{2, 1, 1},
		{1, 1, 1}, // a listing needs two, so a cap of 1 has to become 2
	}
	for _, c := range cases {
		mainOpen, kvOpen := splitPoolForKv(c.maxOpen)
		if mainOpen != c.mainOpen || kvOpen != c.kvOpen {
			t.Errorf("splitPoolForKv(%d) = %d, %d, want %d, %d", c.maxOpen, mainOpen, kvOpen, c.mainOpen, c.kvOpen)
		}
	}
}

type recordingDDLGen struct {
	SqlGenerator
	calls []string
}

func (g *recordingDDLGen) GetSqlCreateTable(tableName string) string {
	g.calls = append(g.calls, "create "+tableName)
	return "CREATE TABLE " + tableName
}

func (g *recordingDDLGen) GetSqlDropTable(tableName string) string {
	g.calls = append(g.calls, "drop "+tableName)
	return "DROP"
}

func TestBucketHooksSkipInvalidBucketNames(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec("CREATE TABLE mybucket").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("DROP").WillReturnResult(sqlmock.NewResult(0, 0))

	gen := &recordingDDLGen{}
	store := &AbstractSqlStore{SqlGenerator: gen, DB: db, SupportBucketTable: true}

	for _, name := range []string{
		`.x01" (a int);CREATE TABLE pwned(flag text);--`,
		`.z01","marker`,
		`b01" (a int); DROP TABLE filemeta; --`,
		DEFAULT_TABLE,
		".system",
		"",
	} {
		store.OnBucketCreation(name)
		store.OnBucketDeletion(name)
	}
	store.OnBucketCreation("mybucket")
	store.OnBucketDeletion("mybucket")

	if want := []string{"create mybucket", "drop mybucket"}; !reflect.DeepEqual(gen.calls, want) {
		t.Errorf("DDL calls = %q, want %q", gen.calls, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
