package filer

import (
	"context"
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
