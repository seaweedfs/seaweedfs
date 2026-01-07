//go:build ydb
// +build ydb

package ydb

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer/store_test"
)

func TestStore(t *testing.T) {
	// run "make test_ydb" under docker folder.
	// to set up local env
	if false {
		store := &YdbStore{}
		store.initialize("/buckets", "grpc://localhost:2136/?database=local", "seaweedfs", true, 10, 50,
			true, 200, true, 5, 1000, 2000)
		store_test.TestFilerStore(t, store)
	}
}
