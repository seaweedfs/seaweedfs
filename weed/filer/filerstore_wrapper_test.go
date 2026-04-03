package filer

import (
	"context"
	"errors"
	"os"
	"testing"

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

// TestFilerStoreWrapperContextCancellation verifies that write operations
// respect context cancellation to prevent orphaned metadata
func TestFilerStoreWrapperContextCancellation(t *testing.T) {
	tests := []struct {
		name          string
		cancelContext bool
		expectError   bool
		isWriteOp     bool
		run           func(*FilerStoreWrapper, context.Context, *Entry) error
	}{
		{
			name:          "InsertEntry with cancelled context fails",
			cancelContext: true,
			expectError:   true,
			isWriteOp:     true,
			run: func(fsw *FilerStoreWrapper, ctx context.Context, entry *Entry) error {
				return fsw.InsertEntry(ctx, entry)
			},
		},
		{
			name:          "InsertEntry with active context succeeds",
			cancelContext: false,
			expectError:   false,
			isWriteOp:     true,
			run: func(fsw *FilerStoreWrapper, ctx context.Context, entry *Entry) error {
				return fsw.InsertEntry(ctx, entry)
			},
		},
		{
			name:          "InsertEntryKnownAbsent with cancelled context fails",
			cancelContext: true,
			expectError:   true,
			isWriteOp:     true,
			run: func(fsw *FilerStoreWrapper, ctx context.Context, entry *Entry) error {
				return fsw.InsertEntryKnownAbsent(ctx, entry)
			},
		},
		{
			name:          "UpdateEntry with cancelled context fails",
			cancelContext: true,
			expectError:   true,
			isWriteOp:     true,
			run: func(fsw *FilerStoreWrapper, ctx context.Context, entry *Entry) error {
				_ = fsw.InsertEntry(context.Background(), entry)
				entry.Attr.Mime = "updated"
				return fsw.UpdateEntry(ctx, entry)
			},
		},
		{
			name:          "DeleteEntry with cancelled context fails",
			cancelContext: true,
			expectError:   true,
			isWriteOp:     true,
			run: func(fsw *FilerStoreWrapper, ctx context.Context, entry *Entry) error {
				_ = fsw.InsertEntry(context.Background(), entry)
				return fsw.DeleteEntry(ctx, entry.FullPath)
			},
		},
		{
			name:          "DeleteOneEntry with cancelled context fails",
			cancelContext: true,
			expectError:   true,
			isWriteOp:     true,
			run: func(fsw *FilerStoreWrapper, ctx context.Context, entry *Entry) error {
				_ = fsw.InsertEntry(context.Background(), entry)
				return fsw.DeleteOneEntry(ctx, entry)
			},
		},
		{
			name:          "DeleteFolderChildren with cancelled context fails",
			cancelContext: true,
			expectError:   true,
			isWriteOp:     true,
			run: func(fsw *FilerStoreWrapper, ctx context.Context, entry *Entry) error {
				childEntry := &Entry{
					FullPath: util.FullPath("/test/folder/child"),
					Attr:     Attr{Mode: 0o660},
				}
				_ = fsw.InsertEntry(context.Background(), childEntry)
				return fsw.DeleteFolderChildren(ctx, util.FullPath("/test/folder"))
			},
		},
		{
			name:          "KvPut with cancelled context fails",
			cancelContext: true,
			expectError:   true,
			isWriteOp:     true,
			run: func(fsw *FilerStoreWrapper, ctx context.Context, entry *Entry) error {
				return fsw.KvPut(ctx, []byte("test-key"), []byte("test-value"))
			},
		},
		{
			name:          "KvDelete with cancelled context fails",
			cancelContext: true,
			expectError:   true,
			isWriteOp:     true,
			run: func(fsw *FilerStoreWrapper, ctx context.Context, entry *Entry) error {
				_ = fsw.KvPut(context.Background(), []byte("test-key"), []byte("test-value"))
				return fsw.KvDelete(ctx, []byte("test-key"))
			},
		},
		{
			name:          "FindEntry with cancelled context succeeds (read operation)",
			cancelContext: true,
			expectError:   false,
			isWriteOp:     false,
			run: func(fsw *FilerStoreWrapper, ctx context.Context, entry *Entry) error {
				_ = fsw.InsertEntry(context.Background(), entry)
				_, err := fsw.FindEntry(ctx, entry.FullPath)
				return err
			},
		},
		{
			name:          "KvGet with cancelled context succeeds (read operation)",
			cancelContext: true,
			expectError:   false,
			isWriteOp:     false,
			run: func(fsw *FilerStoreWrapper, ctx context.Context, entry *Entry) error {
				_ = fsw.KvPut(context.Background(), []byte("test-key"), []byte("test-value"))
				_, err := fsw.KvGet(ctx, []byte("test-key"))
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newStubFilerStore()
			wrapper := NewFilerStoreWrapper(store)
			entry := &Entry{
				FullPath: util.FullPath("/test/object"),
				Attr: Attr{
					Mode: 0o660,
					Mime: "application/octet-stream",
				},
			}

			ctx, cancel := context.WithCancel(context.Background())
			if tt.cancelContext {
				cancel()
			} else {
				defer cancel()
			}

			err := tt.run(wrapper, ctx, entry)

			if tt.expectError {
				require.Error(t, err)
				if tt.isWriteOp {
					assert.True(t, errors.Is(err, context.Canceled))
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestFilerStoreWrapperTransactionContextCancellation verifies that transaction
// operations respect context cancellation.
func TestFilerStoreWrapperTransactionContextCancellation(t *testing.T) {
	tests := []struct {
		name          string
		cancelContext bool
		run           func(*FilerStoreWrapper, context.Context) error
	}{
		{
			name:          "BeginTransaction with cancelled context fails",
			cancelContext: true,
			run: func(fsw *FilerStoreWrapper, ctx context.Context) error {
				_, err := fsw.BeginTransaction(ctx)
				return err
			},
		},
		{
			name:          "BeginTransaction with active context succeeds",
			cancelContext: false,
			run: func(fsw *FilerStoreWrapper, ctx context.Context) error {
				_, err := fsw.BeginTransaction(ctx)
				return err
			},
		},
		{
			name:          "CommitTransaction with cancelled context fails",
			cancelContext: true,
			run: func(fsw *FilerStoreWrapper, ctx context.Context) error {
				return fsw.CommitTransaction(ctx)
			},
		},
		{
			name:          "CommitTransaction with active context succeeds",
			cancelContext: false,
			run: func(fsw *FilerStoreWrapper, ctx context.Context) error {
				return fsw.CommitTransaction(ctx)
			},
		},
		{
			name:          "RollbackTransaction with cancelled context fails",
			cancelContext: true,
			run: func(fsw *FilerStoreWrapper, ctx context.Context) error {
				return fsw.RollbackTransaction(ctx)
			},
		},
		{
			name:          "RollbackTransaction with active context succeeds",
			cancelContext: false,
			run: func(fsw *FilerStoreWrapper, ctx context.Context) error {
				return fsw.RollbackTransaction(ctx)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newStubFilerStore()
			wrapper := NewFilerStoreWrapper(store)

			ctx, cancel := context.WithCancel(context.Background())
			if tt.cancelContext {
				cancel()
			} else {
				defer cancel()
			}

			err := tt.run(wrapper, ctx)

			if tt.cancelContext {
				require.Error(t, err)
				assert.True(t, errors.Is(err, context.Canceled))
			} else {
				require.NoError(t, err)
			}
		})
	}
}
