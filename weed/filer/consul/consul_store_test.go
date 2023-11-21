package consul

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer/store_test"
)

func TestStore(t *testing.T) {
	// run "make test_consul" under docker folder.
	// to set up local env
	if false {
		store := &ConsulStore{consulKeyPrefix: "seaweedfs"}
		store.initialize("localhost:8500", "", "", "3s")
		store_test.TestFilerStore(t, store)
	}
}
