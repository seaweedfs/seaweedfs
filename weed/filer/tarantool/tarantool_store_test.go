//go:build tarantool
// +build tarantool

package tarantool

import (
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer/store_test"
)

func TestStore(t *testing.T) {
	// run "make test_tarantool" under docker folder.
	// to set up local env
	if os.Getenv("RUN_TARANTOOL_TESTS") != "1" {
		t.Skip("Tarantool tests are disabled. Set RUN_TARANTOOL_TESTS=1 to enable.")
	}
	store := &TarantoolStore{}
	addresses := []string{"127.0.1:3303"}
	store.initialize(addresses, "client", "client", 5*time.Second, 1000)
	store_test.TestFilerStore(t, store)
}
