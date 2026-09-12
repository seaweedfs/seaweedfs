package weed_server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func TestSaveMetaDataAppendsToInlineContent(t *testing.T) {
	store := newRenameTestStore()
	store.entries["/small.txt"] = &filer.Entry{
		FullPath: util.FullPath("/small.txt"),
		Attr: filer.Attr{
			Mtime:    time.Unix(100, 0),
			Crtime:   time.Unix(100, 0),
			Mode:     0644,
			FileSize: 5,
		},
		Content: []byte("hello"),
	}

	server := &FilerServer{
		filer:  newRenameTestFiler(store),
		option: &FilerOption{MaxMB: 1},
	}
	appendChunk := &filer_pb.FileChunk{FileId: "2,append", Offset: 0, Size: 6}
	var savedData []byte
	var savedName string
	var savedOffset int64
	var savedSize uint64
	var savedTsNs int64

	r := httptest.NewRequest(http.MethodPut, "/small.txt?op=append&skipCheckParentDir=true", nil)
	result, err := server.saveMetaDataWithSaveFunc(
		context.Background(),
		r,
		"small.txt",
		"",
		&operation.StorageOption{},
		nil,
		[]*filer_pb.FileChunk{appendChunk},
		6,
		nil,
		func(reader io.Reader, name string, offset int64, tsNs int64, expectedDataSize uint64) (*filer_pb.FileChunk, error) {
			var readErr error
			savedData, readErr = io.ReadAll(reader)
			if readErr != nil {
				return nil, readErr
			}
			savedName = name
			savedOffset = offset
			savedSize = expectedDataSize
			savedTsNs = tsNs
			return &filer_pb.FileChunk{FileId: "1,inline", Offset: offset, Size: expectedDataSize, ModifiedTsNs: tsNs}, nil
		},
	)
	if err != nil {
		t.Fatalf("saveMetaDataWithSaveFunc: %v", err)
	}
	if result.Size != 11 {
		t.Fatalf("result size = %d, want 11", result.Size)
	}
	if string(savedData) != "hello" || savedName != "small.txt" || savedOffset != 0 || savedSize != 5 || savedTsNs == 0 {
		t.Fatalf("inline save got data=%q name=%q offset=%d size=%d ts=%d", string(savedData), savedName, savedOffset, savedSize, savedTsNs)
	}

	updated, err := store.FindEntry(context.Background(), "/small.txt")
	if err != nil {
		t.Fatalf("find updated entry: %v", err)
	}
	if len(updated.Content) != 0 {
		t.Fatalf("updated content length = %d, want 0", len(updated.Content))
	}
	if updated.FileSize != 11 {
		t.Fatalf("updated file size = %d, want 11", updated.FileSize)
	}
	if len(updated.Chunks) != 2 {
		t.Fatalf("updated chunks = %d, want 2: %+v", len(updated.Chunks), updated.Chunks)
	}
	if chunk := updated.Chunks[0]; chunk.FileId != "1,inline" || chunk.Offset != 0 || chunk.Size != 5 {
		t.Fatalf("inline chunk = %+v, want fileId=1,inline offset=0 size=5", chunk)
	}
	if chunk := updated.Chunks[1]; chunk.FileId != "2,append" || chunk.Offset != 5 || chunk.Size != 6 {
		t.Fatalf("append chunk = %+v, want fileId=2,append offset=5 size=6", chunk)
	}
}
