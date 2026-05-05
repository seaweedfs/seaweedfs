package filer

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilerStoreWrapperMimeNormalization(t *testing.T) {
	tests := []struct {
		name     string
		mode     os.FileMode
		wantMime string
	}{
		{
			name:     "files strip octet-stream",
			mode:     0o660,
			wantMime: "",
		},
		{
			name:     "directories keep octet-stream",
			mode:     os.ModeDir | 0o770,
			wantMime: "application/octet-stream",
		},
	}

	operations := []struct {
		name string
		run  func(*FilerStoreWrapper, context.Context, *Entry) error
	}{
		{
			name: "insert",
			run: func(fsw *FilerStoreWrapper, ctx context.Context, entry *Entry) error {
				return fsw.InsertEntry(ctx, entry)
			},
		},
		{
			name: "update",
			run: func(fsw *FilerStoreWrapper, ctx context.Context, entry *Entry) error {
				return fsw.UpdateEntry(ctx, entry)
			},
		},
	}

	for _, tt := range tests {
		for _, op := range operations {
			t.Run(tt.name+"/"+op.name, func(t *testing.T) {
				store := newStubFilerStore()
				wrapper := NewFilerStoreWrapper(store)
				entry := &Entry{
					FullPath: util.FullPath("/buckets/test/object"),
					Attr: Attr{
						Mode: tt.mode,
						Mime: "application/octet-stream",
					},
				}

				err := op.run(wrapper, context.Background(), entry)
				require.NoError(t, err)

				storedEntry, findErr := store.FindEntry(context.Background(), entry.FullPath)
				require.NoError(t, findErr)
				assert.Equal(t, tt.wantMime, storedEntry.Mime)
			})
		}
	}
}

// cancelledCtx returns a context that is already cancelled.
func cancelledCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// expiredCtx returns a context whose deadline has already passed.
func expiredCtx() context.Context {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	cancel() // release resources immediately as it's already expired
	return ctx
}

func TestFilerStoreWrapperWriteOpsRejectCancelledContext(t *testing.T) {
	newEntry := func(path string) *Entry {
		return &Entry{
			FullPath: util.FullPath(path),
			Attr:     Attr{Mode: 0o660, Mime: "application/octet-stream"},
		}
	}

	// Each write operation that should be guarded.
	writeOps := []struct {
		name string
		run  func(*FilerStoreWrapper, context.Context) error
	}{
		{"InsertEntry", func(fsw *FilerStoreWrapper, ctx context.Context) error {
			return fsw.InsertEntry(ctx, newEntry("/test/a"))
		}},
		{"InsertEntryKnownAbsent", func(fsw *FilerStoreWrapper, ctx context.Context) error {
			return fsw.InsertEntryKnownAbsent(ctx, newEntry("/test/b"))
		}},
		{"UpdateEntry", func(fsw *FilerStoreWrapper, ctx context.Context) error {
			_ = fsw.InsertEntry(context.Background(), newEntry("/test/c"))
			return fsw.UpdateEntry(ctx, newEntry("/test/c"))
		}},
		{"DeleteEntry", func(fsw *FilerStoreWrapper, ctx context.Context) error {
			_ = fsw.InsertEntry(context.Background(), newEntry("/test/d"))
			return fsw.DeleteEntry(ctx, "/test/d")
		}},
		{"DeleteOneEntry", func(fsw *FilerStoreWrapper, ctx context.Context) error {
			e := newEntry("/test/e")
			_ = fsw.InsertEntry(context.Background(), e)
			return fsw.DeleteOneEntry(ctx, e)
		}},
		{"DeleteFolderChildren", func(fsw *FilerStoreWrapper, ctx context.Context) error {
			_ = fsw.InsertEntry(context.Background(), newEntry("/test/folder/child"))
			return fsw.DeleteFolderChildren(ctx, "/test/folder")
		}},
		{"BeginTransaction", func(fsw *FilerStoreWrapper, ctx context.Context) error {
			_, err := fsw.BeginTransaction(ctx)
			return err
		}},
		{"CommitTransaction", func(fsw *FilerStoreWrapper, ctx context.Context) error {
			return fsw.CommitTransaction(ctx)
		}},
		{"KvPut", func(fsw *FilerStoreWrapper, ctx context.Context) error {
			return fsw.KvPut(ctx, []byte("k"), []byte("v"))
		}},
		{"KvDelete", func(fsw *FilerStoreWrapper, ctx context.Context) error {
			_ = fsw.KvPut(context.Background(), []byte("k"), []byte("v"))
			return fsw.KvDelete(ctx, []byte("k"))
		}},
	}

	badContexts := []struct {
		name      string
		ctx       context.Context
		wantError error
	}{
		{"cancelled", cancelledCtx(), context.Canceled},
		{"deadline exceeded", expiredCtx(), context.DeadlineExceeded},
	}

	for _, op := range writeOps {
		for _, bc := range badContexts {
			t.Run(op.name+"/"+bc.name, func(t *testing.T) {
				wrapper := NewFilerStoreWrapper(newStubFilerStore())
				err := op.run(wrapper, bc.ctx)
				require.Error(t, err)
				assert.True(t, errors.Is(err, bc.wantError), "got %v, want %v", err, bc.wantError)
			})
		}
	}
}

func TestFilerStoreWrapperWriteOpsSucceedWithActiveContext(t *testing.T) {
	wrapper := NewFilerStoreWrapper(newStubFilerStore())
	ctx := context.Background()
	entry := &Entry{
		FullPath: util.FullPath("/test/obj"),
		Attr:     Attr{Mode: 0o660},
	}

	require.NoError(t, wrapper.InsertEntry(ctx, entry))
	require.NoError(t, wrapper.UpdateEntry(ctx, entry))
	require.NoError(t, wrapper.DeleteOneEntry(ctx, entry))
	require.NoError(t, wrapper.InsertEntryKnownAbsent(ctx, entry))
	require.NoError(t, wrapper.DeleteEntry(ctx, entry.FullPath))
	require.NoError(t, wrapper.KvPut(ctx, []byte("k"), []byte("v")))
	require.NoError(t, wrapper.KvDelete(ctx, []byte("k")))

	txCtx, err := wrapper.BeginTransaction(ctx)
	require.NoError(t, err)
	require.NoError(t, wrapper.CommitTransaction(txCtx))
}

func TestFilerStoreWrapperReadOpsSucceedWithCancelledContext(t *testing.T) {
	wrapper := NewFilerStoreWrapper(newStubFilerStore())
	entry := &Entry{
		FullPath: util.FullPath("/test/readable"),
		Attr:     Attr{Mode: 0o660},
	}
	require.NoError(t, wrapper.InsertEntry(context.Background(), entry))
	require.NoError(t, wrapper.KvPut(context.Background(), []byte("rk"), []byte("rv")))

	ctx := cancelledCtx()

	_, err := wrapper.FindEntry(ctx, entry.FullPath)
	assert.NoError(t, err)

	_, err = wrapper.KvGet(ctx, []byte("rk"))
	assert.NoError(t, err)
}

// RollbackTransaction must succeed even when the context is cancelled or
// expired, because it is a cleanup operation called after failures.
func TestFilerStoreWrapperRollbackSucceedsWithCancelledContext(t *testing.T) {
	wrapper := NewFilerStoreWrapper(newStubFilerStore())
	assert.NoError(t, wrapper.RollbackTransaction(cancelledCtx()))
	assert.NoError(t, wrapper.RollbackTransaction(expiredCtx()))
}
