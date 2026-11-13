package etcd

import (
	"context"
	"testing"
	"time"

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

func TestWithTimeout(t *testing.T) {
	store := &EtcdStore{timeout: time.Minute, requestTimeout: 3 * time.Second}

	ctx, cancel := store.withTimeout(context.Background())
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > store.requestTimeout {
		t.Fatalf("no deadline within the store's request timeout: %v %v", deadline, ok)
	}

	callerCtx, callerCancel := context.WithTimeout(context.Background(), time.Minute)
	defer callerCancel()
	ctx, cancel = store.withTimeout(callerCtx)
	defer cancel()
	if ctx != callerCtx {
		t.Fatal("the caller's deadline was replaced")
	}
}
