//go:build ydb
// +build ydb

package ydb

import (
	"github.com/seaweedfs/seaweedfs/weed/filer/store_test"
	"testing"
)

func TestStore(t *testing.T) {
	// run "make test_ydb" under docker folder.
	// to set up local env
	if false {
		store := &YdbStore{}
		store.initialize("/buckets", "grpc://localhost:2136/?database=local", "seaweedfs", true, 10, 50)
		store_test.TestFilerStore(t, store)
	}
}
