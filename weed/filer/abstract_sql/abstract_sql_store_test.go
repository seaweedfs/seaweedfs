package abstract_sql

import (
	"context"
	"testing"

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
