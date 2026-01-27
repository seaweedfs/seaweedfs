package etcd

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer/store_test"
)

func TestStore(t *testing.T) {
	// run "make test_etcd" under docker folder.
	// to set up local env
	if false {
		store := &EtcdStore{}
		store.initialize("localhost:2379", "", "", 3, nil)
		store_test.TestFilerStore(t, store)
	}
}
