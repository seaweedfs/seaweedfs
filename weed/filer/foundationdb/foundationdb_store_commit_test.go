//go:build foundationdb

package foundationdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestCommitErrorOnlyMarksKnownUncommittedConflicts(t *testing.T) {
	conflict := fdb.Error{Code: 1020}
	for _, err := range []error{conflict, fmt.Errorf("commit: %w", conflict)} {
		got := commitError(err)
		if !errors.Is(got, filer.ErrTransactionConflict) {
			t.Fatalf("commitError(%v) = %v, want transaction conflict", err, got)
		}
		var original fdb.Error
		if !errors.As(got, &original) || original.Code != conflict.Code {
			t.Fatalf("commitError lost FoundationDB's error: %v", got)
		}
	}
	// An unknown result must not let the retry loop delete manifests that
	// may have committed. Timeouts and other errors keep their own meaning.
	for _, err := range []error{nil, fdb.Error{Code: 1021}, fmt.Errorf("commit: %w", fdb.Error{Code: 1021}), fdb.Error{Code: 1007}, errors.New("unavailable")} {
		if got := commitError(err); got != err {
			t.Fatalf("commitError(%v) = %v, want unchanged", err, got)
		}
	}
}

func TestEntryAndKVTransactionsRefuseConcurrentDeletes(t *testing.T) {
	if _, err := os.Stat(getTestClusterFile()); err != nil {
		t.Skip("FoundationDB cluster file is not available")
	}
	store := &FoundationDBStore{directoryPrefix: fmt.Sprintf("seaweedfs-conflict-test-%d", time.Now().UnixNano())}
	if err := store.initialize(getTestClusterFile(), 740); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Shutdown)
	ctx := context.Background()
	path := util.FullPath("/conflict/file")
	entry := &filer.Entry{FullPath: path, Attr: filer.Attr{Mode: 0644}}
	key, value := []byte("hard-link"), []byte("shared metadata")

	for _, kv := range []bool{false, true} {
		t.Run(map[bool]string{false: "entry", true: "shared KV"}[kv], func(t *testing.T) {
			var err error
			if kv {
				err = store.KvPut(ctx, key, value)
			} else {
				err = store.InsertEntry(ctx, entry)
			}
			if err != nil {
				t.Fatal(err)
			}
			txCtx, err := store.BeginTransaction(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer store.RollbackTransaction(txCtx)
			if kv {
				_, err = store.KvGet(txCtx, key)
			} else {
				_, err = store.FindEntry(txCtx, path)
			}
			if err != nil {
				t.Fatal(err)
			}
			if kv {
				err = store.KvDelete(ctx, key)
			} else {
				err = store.DeleteEntry(ctx, path)
			}
			if err != nil {
				t.Fatal(err)
			}
			if kv {
				err = store.KvPut(txCtx, key, value)
			} else {
				err = store.UpdateEntry(txCtx, entry)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = store.CommitTransaction(txCtx); !errors.Is(err, filer.ErrTransactionConflict) {
				t.Fatalf("commit = %v, want transaction conflict", err)
			}
			if kv {
				_, err = store.KvGet(ctx, key)
				if !errors.Is(err, filer.ErrKvNotFound) {
					t.Fatalf("KV after refused commit = %v, want absent", err)
				}
			} else {
				_, err = store.FindEntry(ctx, path)
				if !errors.Is(err, filer_pb.ErrNotFound) {
					t.Fatalf("entry after refused commit = %v, want absent", err)
				}
			}
		})
	}
}
