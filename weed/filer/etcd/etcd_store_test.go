package etcd

import (
	"github.com/chrislusf/seaweedfs/weed/filer/store_test"
	"testing"
)

func TestStore(t *testing.T) {
	if false { // turn on the testing when local env is set up
		store := &EtcdStore{}
		store.initialize("localhost:2379", "3s")
		store_test.TestFilerStore(t, store)
	}
}
