package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/store_test"
	clientv3 "go.etcd.io/etcd/client/v3"
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

	store.requestTimeout = 0
	ctx, cancel = store.withTimeout(context.Background())
	defer cancel()
	if _, ok := ctx.Deadline(); ok {
		t.Fatal(`request_timeout "0s" set a deadline`)
	}
}

type putDeadlineKV struct {
	clientv3.KV
	deadlines []bool
}

func (kv *putDeadlineKV) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	_, ok := ctx.Deadline()
	kv.deadlines = append(kv.deadlines, ok)
	return &clientv3.PutResponse{}, nil
}

func TestWritesHaveNoRequestDeadline(t *testing.T) {
	kv := &putDeadlineKV{}
	store := &EtcdStore{client: &clientv3.Client{KV: kv}, requestTimeout: 3 * time.Second}
	ctx := context.Background()
	entry := &filer.Entry{FullPath: "/dir/file"}

	if err := store.InsertEntry(ctx, entry); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateEntry(ctx, entry); err != nil {
		t.Fatal(err)
	}
	if err := store.KvPut(ctx, []byte("hard link id"), []byte("chunks")); err != nil {
		t.Fatal(err)
	}
	if len(kv.deadlines) != 3 {
		t.Fatalf("got %d puts, want 3", len(kv.deadlines))
	}
	for i, hasDeadline := range kv.deadlines {
		if hasDeadline {
			t.Errorf("put %d has a request deadline", i)
		}
	}
}
