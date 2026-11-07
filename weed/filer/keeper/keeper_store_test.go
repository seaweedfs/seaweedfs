package keeper

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer/store_test"
)

func TestStore(t *testing.T) {
	// Point servers at a local ClickHouse Keeper (or ZooKeeper) and set to true.
	if false {
		store := &KeeperStore{}
		if err := store.initialize("localhost:9181", "/seaweedfs/filer", 10*time.Second); err != nil {
			t.Fatal(err)
		}
		defer store.Shutdown()
		store_test.TestFilerStore(t, store)
	}
}
