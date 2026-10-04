package weed_server

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestFilerInlineAppendStaysInlineBelowLimit(t *testing.T) {
	ctx := context.Background()
	const (
		filePath = "/append.txt"
		limit    = int64(12)
	)
	oldContent := []byte("old-")
	appendContent := []byte("new-")
	store := newRenameTestStore()
	f := newRenameTestFiler(t, store)
	originalExtended := map[string][]byte{"X-Existing": []byte("preserved")}
	entry := &filer.Entry{
		FullPath: util.FullPath(filePath),
		Attr: filer.Attr{
			Mtime:    time.Unix(1, 0),
			Crtime:   time.Unix(1, 0),
			Mode:     0640,
			Uid:      123,
			Gid:      456,
			FileSize: uint64(len(oldContent)),
		},
		Extended: originalExtended,
		Content:  append([]byte(nil), oldContent...),
	}
	if err := store.InsertEntry(ctx, entry); err != nil {
		t.Fatalf("insert inline entry: %v", err)
	}

	fs := &FilerServer{
		filer: f,
		option: &FilerOption{
			SaveToFilerLimit: limit,
			MaxMB:            1,
		},
	}
	so := &operation.StorageOption{}
	r := httptest.NewRequest(http.MethodPut, filePath+"?op=append", bytes.NewReader(appendContent))
	r.ContentLength = -1 // The inline decision must be based on the stream, not Content-Length.
	r.Header.Set("Cache-Control", "max-age=60")

	fileChunks, md5Hash, chunkOffset, err, smallContent := fs.uploadRequestToChunks(
		ctx, httptest.NewRecorder(), r, r.Body, 4, "append.txt", "", -1, so,
	)
	if err != nil {
		t.Fatalf("prepare append: %v", err)
	}
	if len(fileChunks) != 0 {
		t.Fatalf("inline append uploaded %d chunks, want none", len(fileChunks))
	}
	if md5Hash == nil {
		t.Fatal("append digest is nil")
	}
	if chunkOffset != int64(len(appendContent)) {
		t.Fatalf("append size = %d, want %d", chunkOffset, len(appendContent))
	}
	if !bytes.Equal(smallContent, appendContent) {
		t.Fatalf("buffered append = %q, want %q", smallContent, appendContent)
	}

	result, err, cleanupChunks := fs.saveMetaData(ctx, r, "append.txt", "", so, 4, md5Hash.Sum(nil), fileChunks, chunkOffset, smallContent)
	if err != nil {
		t.Fatalf("save appended entry: %v", err)
	}
	if len(cleanupChunks) != 0 {
		t.Fatalf("successful append returned uncommitted chunks: %#v", cleanupChunks)
	}
	if result == nil || result.Size != int64(len(oldContent)+len(appendContent)) {
		t.Fatalf("result = %#v, want size %d", result, len(oldContent)+len(appendContent))
	}

	updated, err := f.FindEntry(ctx, util.FullPath(filePath))
	if err != nil {
		t.Fatalf("find updated entry: %v", err)
	}
	wantContent := append(append([]byte(nil), oldContent...), appendContent...)
	if !bytes.Equal(updated.Content, wantContent) {
		t.Fatalf("stored content = %q, want %q", updated.Content, wantContent)
	}
	if len(updated.Chunks) != 0 {
		t.Fatalf("stored %d chunks for below-limit append, want inline content", len(updated.Chunks))
	}
	if updated.FileSize != uint64(len(wantContent)) {
		t.Fatalf("file size = %d, want %d", updated.FileSize, len(wantContent))
	}
	if string(updated.Extended["X-Existing"]) != "preserved" {
		t.Fatalf("existing extended attribute was lost: %#v", updated.Extended)
	}
	if string(updated.Extended["Cache-Control"]) != "max-age=60" {
		t.Fatalf("request extended attribute was not saved: %#v", updated.Extended)
	}
	if _, mutated := originalExtended["Cache-Control"]; mutated {
		t.Fatalf("the original entry's extended attributes were mutated: %#v", originalExtended)
	}
	if updated.Mtime.Equal(time.Unix(1, 0)) {
		t.Fatal("mtime was not updated")
	}
}

func TestFilerInlineAppendEmptyKeepsInlineContent(t *testing.T) {
	ctx := context.Background()
	const filePath = "/append.txt"
	oldContent := []byte("old-")
	store := newRenameTestStore()
	f := newRenameTestFiler(t, store)
	if err := store.InsertEntry(ctx, &filer.Entry{
		FullPath: util.FullPath(filePath),
		Attr: filer.Attr{
			Mtime:    time.Unix(1, 0),
			Crtime:   time.Unix(1, 0),
			FileSize: uint64(len(oldContent)),
		},
		Content: append([]byte(nil), oldContent...),
	}); err != nil {
		t.Fatalf("insert inline entry: %v", err)
	}

	// The current threshold is below the existing file size. An empty append
	// must not migrate the file merely because it is now over that threshold.
	fs := &FilerServer{filer: f, option: &FilerOption{SaveToFilerLimit: 3, MaxMB: 1}}
	r := httptest.NewRequest(http.MethodPut, filePath+"?op=append", bytes.NewReader(nil))
	fileChunks, md5Hash, chunkOffset, err, smallContent := fs.uploadRequestToChunks(
		ctx, httptest.NewRecorder(), r, r.Body, 4, "append.txt", "", 0, &operation.StorageOption{},
	)
	if err != nil {
		t.Fatalf("prepare empty append: %v", err)
	}
	if len(fileChunks) != 0 || smallContent == nil || len(smallContent) != 0 {
		t.Fatalf("empty append upload state: chunks=%d content=%#v", len(fileChunks), smallContent)
	}
	if md5Hash == nil || chunkOffset != 0 {
		t.Fatalf("empty append digest/size = %v/%d, want non-nil digest and size 0", md5Hash, chunkOffset)
	}

	if _, err, cleanupChunks := fs.saveMetaData(ctx, r, "append.txt", "", &operation.StorageOption{}, 4, md5Hash.Sum(nil), fileChunks, chunkOffset, smallContent); err != nil {
		t.Fatalf("save empty append: %v", err)
	} else if len(cleanupChunks) != 0 {
		t.Fatalf("empty append returned cleanup chunks: %#v", cleanupChunks)
	}
	updated, err := f.FindEntry(ctx, util.FullPath(filePath))
	if err != nil {
		t.Fatalf("find updated entry: %v", err)
	}
	if !bytes.Equal(updated.Content, oldContent) || len(updated.Chunks) != 0 || updated.FileSize != uint64(len(oldContent)) {
		t.Fatalf("empty append changed storage representation: content=%q chunks=%d size=%d", updated.Content, len(updated.Chunks), updated.FileSize)
	}
}

func TestFilerInlineAppendRejectsPromotionWithoutChunkSize(t *testing.T) {
	ctx := context.Background()
	const filePath = "/append.txt"
	oldContent := []byte("old-")
	store := newRenameTestStore()
	f := newRenameTestFiler(t, store)
	if err := store.InsertEntry(ctx, &filer.Entry{
		FullPath: util.FullPath(filePath),
		Attr: filer.Attr{
			Mtime:    time.Unix(1, 0),
			Crtime:   time.Unix(1, 0),
			FileSize: uint64(len(oldContent)),
		},
		Content: append([]byte(nil), oldContent...),
	}); err != nil {
		t.Fatalf("insert inline entry: %v", err)
	}
	fs := &FilerServer{filer: f, option: &FilerOption{SaveToFilerLimit: 8, MaxMB: 1}}
	r := httptest.NewRequest(http.MethodPut, filePath+"?op=append", strings.NewReader("new-"))
	chunks, _, _, err, smallContent := fs.uploadRequestToChunks(
		ctx, httptest.NewRecorder(), r, r.Body, 0, "append.txt", "", -1, &operation.StorageOption{},
	)
	if err == nil || !strings.Contains(err.Error(), "invalid chunk size 0") {
		t.Fatalf("promotion error = %v, want invalid chunk size", err)
	}
	if len(chunks) != 0 || smallContent != nil {
		t.Fatalf("invalid promotion returned data: chunks=%d inline=%q", len(chunks), smallContent)
	}
	unchanged, err := f.FindEntry(ctx, util.FullPath(filePath))
	if err != nil {
		t.Fatalf("find original entry: %v", err)
	}
	if !bytes.Equal(unchanged.Content, oldContent) {
		t.Fatalf("invalid promotion changed original content: %q", unchanged.Content)
	}
}

func TestFilerInlineAppendMultipartPostStaysInline(t *testing.T) {
	ctx := context.Background()
	const filePath = "/append.txt"
	oldContent := []byte("old-")
	appendContent := []byte("new-")
	store := newRenameTestStore()
	f := newRenameTestFiler(t, store)
	if err := store.InsertEntry(ctx, &filer.Entry{
		FullPath: util.FullPath(filePath),
		Attr: filer.Attr{
			Mtime:    time.Unix(1, 0),
			Crtime:   time.Unix(1, 0),
			FileSize: uint64(len(oldContent)),
		},
		Content: append([]byte(nil), oldContent...),
	}); err != nil {
		t.Fatalf("insert inline entry: %v", err)
	}
	fs := &FilerServer{filer: f, option: &FilerOption{SaveToFilerLimit: 12, MaxMB: 1}}

	var body bytes.Buffer
	multipartWriter := multipart.NewWriter(&body)
	part, err := multipartWriter.CreateFormFile("file", "append.txt")
	if err != nil {
		t.Fatalf("create multipart file part: %v", err)
	}
	if _, err = part.Write(appendContent); err != nil {
		t.Fatalf("write multipart file part: %v", err)
	}
	if err = multipartWriter.Close(); err != nil {
		t.Fatalf("close multipart body: %v", err)
	}
	r := httptest.NewRequest(http.MethodPost, "/?op=append", &body)
	r.Header.Set("Content-Type", multipartWriter.FormDataContentType())
	recorder := httptest.NewRecorder()
	result, requestMD5, err := fs.doPostAutoChunk(ctx, recorder, r, 4, int64(body.Len()), &operation.StorageOption{})
	if err != nil {
		t.Fatalf("POST inline append: %v", err)
	}
	if result == nil || result.Size != int64(len(oldContent)+len(appendContent)) {
		t.Fatalf("result = %#v, want size %d", result, len(oldContent)+len(appendContent))
	}
	wantMD5 := md5.Sum(appendContent)
	if !bytes.Equal(requestMD5, wantMD5[:]) {
		t.Fatalf("request MD5 = %x, want %x", requestMD5, wantMD5)
	}
	updated, err := f.FindEntry(ctx, util.FullPath(filePath))
	if err != nil {
		t.Fatalf("find updated entry: %v", err)
	}
	wantContent := append(append([]byte(nil), oldContent...), appendContent...)
	if !bytes.Equal(updated.Content, wantContent) || len(updated.Chunks) != 0 {
		t.Fatalf("multipart append result: content=%q chunks=%d, want inline %q", updated.Content, len(updated.Chunks), wantContent)
	}
}

func TestFilerInlineOffsetWriteRemainsUnsupported(t *testing.T) {
	ctx := context.Background()
	const filePath = "/offset.txt"
	oldContent := []byte("keep")
	store := newRenameTestStore()
	f := newRenameTestFiler(t, store)
	if err := store.InsertEntry(ctx, &filer.Entry{
		FullPath: util.FullPath(filePath),
		Attr: filer.Attr{
			Mtime:    time.Unix(1, 0),
			Crtime:   time.Unix(1, 0),
			FileSize: uint64(len(oldContent)),
		},
		Content: append([]byte(nil), oldContent...),
	}); err != nil {
		t.Fatalf("insert inline entry: %v", err)
	}

	fs := &FilerServer{filer: f, option: &FilerOption{SaveToFilerLimit: 12, MaxMB: 1}}
	r := httptest.NewRequest(http.MethodPut, filePath+"?offset=1", bytes.NewReader([]byte("X")))
	_, err, _ := fs.saveMetaData(ctx, r, "offset.txt", "", &operation.StorageOption{}, 4, nil, []*filer_pb.FileChunk{{Offset: 1, Size: 1}}, 2, nil)
	if err == nil || !strings.Contains(err.Error(), "offset write to inline small file is not supported") {
		t.Fatalf("offset write error = %v, want explicit unsupported error", err)
	}
	unchanged, err := f.FindEntry(ctx, util.FullPath(filePath))
	if err != nil {
		t.Fatalf("find original entry: %v", err)
	}
	if !bytes.Equal(unchanged.Content, oldContent) || unchanged.FileSize != uint64(len(oldContent)) {
		t.Fatalf("unsupported offset write changed entry: content=%q size=%d", unchanged.Content, unchanged.FileSize)
	}
}

func TestFilerInlineAppendReadErrorKeepsOriginalEntry(t *testing.T) {
	ctx := context.Background()
	const filePath = "/append.txt"
	oldContent := []byte("old-")
	store := newRenameTestStore()
	f := newRenameTestFiler(t, store)
	if err := store.InsertEntry(ctx, &filer.Entry{
		FullPath: util.FullPath(filePath),
		Attr: filer.Attr{
			Mtime:    time.Unix(1, 0),
			Crtime:   time.Unix(1, 0),
			FileSize: uint64(len(oldContent)),
		},
		Content: append([]byte(nil), oldContent...),
	}); err != nil {
		t.Fatalf("insert inline entry: %v", err)
	}
	fs := &FilerServer{filer: f, option: &FilerOption{SaveToFilerLimit: 12, MaxMB: 1}}
	r := httptest.NewRequest(http.MethodPut, filePath+"?op=append", nil)
	fileChunks, _, _, err, smallContent := fs.uploadRequestToChunks(
		ctx, httptest.NewRecorder(), r, &inlineAppendReadError{}, 4, "append.txt", "", -1, &operation.StorageOption{},
	)
	if err == nil || !strings.Contains(err.Error(), "read input: injected read failure") {
		t.Fatalf("read error = %v, want propagated input read failure", err)
	}
	if len(fileChunks) != 0 || smallContent != nil {
		t.Fatalf("failed read produced chunks/content: chunks=%d content=%q", len(fileChunks), smallContent)
	}
	unchanged, err := f.FindEntry(ctx, util.FullPath(filePath))
	if err != nil {
		t.Fatalf("find original entry: %v", err)
	}
	if !bytes.Equal(unchanged.Content, oldContent) {
		t.Fatalf("failed read changed original content: %q", unchanged.Content)
	}
}

type inlineAppendReadError struct {
	readData bool
}

func (r *inlineAppendReadError) Read(p []byte) (int, error) {
	if r.readData {
		return 0, errors.New("injected read failure")
	}
	r.readData = true
	return copy(p, []byte("new-")), nil
}

func TestFilerInlineAppendBadDigestKeepsOriginalEntry(t *testing.T) {
	ctx := context.Background()
	const filePath = "/append.txt"
	oldContent := []byte("old-")
	store := newRenameTestStore()
	f := newRenameTestFiler(t, store)
	if err := store.InsertEntry(ctx, &filer.Entry{
		FullPath: util.FullPath(filePath),
		Attr: filer.Attr{
			Mtime:    time.Unix(1, 0),
			Crtime:   time.Unix(1, 0),
			FileSize: uint64(len(oldContent)),
		},
		Content: append([]byte(nil), oldContent...),
	}); err != nil {
		t.Fatalf("insert inline entry: %v", err)
	}
	fs := &FilerServer{filer: f, option: &FilerOption{SaveToFilerLimit: 12, MaxMB: 1}}
	r := httptest.NewRequest(http.MethodPut, filePath+"?op=append", strings.NewReader("new-"))
	r.Header.Set("Content-MD5", "not-the-correct-digest")
	_, _, err := fs.doPutAutoChunk(ctx, httptest.NewRecorder(), r, 4, 4, &operation.StorageOption{})
	if err == nil || !strings.Contains(err.Error(), "Content-Md5") {
		t.Fatalf("PUT error = %v, want bad Content-MD5 error", err)
	}
	unchanged, err := f.FindEntry(ctx, util.FullPath(filePath))
	if err != nil {
		t.Fatalf("find original entry: %v", err)
	}
	if !bytes.Equal(unchanged.Content, oldContent) || len(unchanged.Chunks) != 0 {
		t.Fatalf("bad digest changed original entry: content=%q chunks=%d", unchanged.Content, len(unchanged.Chunks))
	}
}

func TestFilerChunkAppendStillOffsetsNewChunks(t *testing.T) {
	ctx := context.Background()
	const filePath = "/chunked.txt"
	oldChunk := &filer_pb.FileChunk{FileId: "1,00000001", Offset: 0, Size: 4}
	newChunk := &filer_pb.FileChunk{FileId: "1,00000002", Offset: 0, Size: 3}
	store := newRenameTestStore()
	f := newRenameTestFiler(t, store)
	if err := store.InsertEntry(ctx, &filer.Entry{
		FullPath: util.FullPath(filePath),
		Attr: filer.Attr{
			Mtime:    time.Unix(1, 0),
			Crtime:   time.Unix(1, 0),
			FileSize: 4,
		},
		Chunks: []*filer_pb.FileChunk{oldChunk},
	}); err != nil {
		t.Fatalf("insert chunk-backed entry: %v", err)
	}

	fs := &FilerServer{filer: f, option: &FilerOption{SaveToFilerLimit: 12, MaxMB: 1}}
	r := httptest.NewRequest(http.MethodPut, filePath+"?op=append", strings.NewReader("new"))
	result, err, cleanupChunks := fs.saveMetaData(ctx, r, "chunked.txt", "", &operation.StorageOption{}, 4, nil, []*filer_pb.FileChunk{newChunk}, 3, nil)
	if err != nil {
		t.Fatalf("append to chunk-backed entry: %v", err)
	}
	if len(cleanupChunks) != 0 {
		t.Fatalf("successful chunk append returned cleanup chunks: %#v", cleanupChunks)
	}
	updated, err := f.FindEntry(ctx, util.FullPath(filePath))
	if err != nil {
		t.Fatalf("find updated entry: %v", err)
	}
	if result == nil || result.Size != 7 || updated.FileSize != 7 || len(updated.Chunks) != 2 {
		t.Fatalf("append result=%#v entry-size=%d chunks=%d, want size 7 and two chunks", result, updated.FileSize, len(updated.Chunks))
	}
	if updated.Chunks[0].Offset != 0 || updated.Chunks[0].GetFileIdString() != oldChunk.GetFileIdString() || updated.Chunks[1].Offset != 4 || updated.Chunks[1].GetFileIdString() != newChunk.GetFileIdString() {
		t.Fatalf("chunk append metadata = %#v, want offsets 0 and 4 with old and new file IDs", updated.Chunks)
	}
}

func TestFilerInlineAppendPromotesAtLimit(t *testing.T) {
	for _, tc := range []struct {
		name          string
		appendContent string
		failStore     bool
		failVerify    bool
	}{
		{name: "exactly reaches limit", appendContent: "new-"},
		{name: "crosses limit", appendContent: "new!!"},
		{name: "metadata write fails", appendContent: "new-", failStore: true},
		{name: "metadata result cannot be verified", appendContent: "new-", failStore: true, failVerify: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			const (
				filePath = "/append.txt"
				limit    = int64(8)
			)
			oldContent := []byte("old-")
			appendContent := []byte(tc.appendContent)
			store := newRenameTestStore()
			f := newRenameTestFiler(t, store)
			if err := store.InsertEntry(ctx, &filer.Entry{
				FullPath: util.FullPath(filePath),
				Attr: filer.Attr{
					Mtime:    time.Unix(1, 0),
					Crtime:   time.Unix(1, 0),
					Mode:     0640,
					FileSize: uint64(len(oldContent)),
				},
				Content: append([]byte(nil), oldContent...),
			}); err != nil {
				t.Fatalf("insert inline entry: %v", err)
			}

			fs, uploadedChunks := newInlineAppendUploadServer(t, f, limit, 4)
			r := httptest.NewRequest(http.MethodPut, filePath+"?op=append", bytes.NewReader(appendContent))
			r.ContentLength = -1
			fileChunks, md5Hash, chunkOffset, err, smallContent := fs.uploadRequestToChunks(
				ctx, httptest.NewRecorder(), r, r.Body, 4, "append.txt", "", -1, &operation.StorageOption{},
			)
			if err != nil {
				t.Fatalf("prepare append: %v", err)
			}
			if len(fileChunks) == 0 {
				t.Fatal("append reaching the inline limit was not uploaded as chunks")
			}
			if smallContent != nil {
				t.Fatalf("append reaching the inline limit was buffered as inline content: %q", smallContent)
			}
			if chunkOffset != int64(len(appendContent)) {
				t.Fatalf("append size = %d, want %d", chunkOffset, len(appendContent))
			}
			wantAppendMD5 := md5.Sum(appendContent)
			if got := md5Hash.Sum(nil); !bytes.Equal(got, wantAppendMD5[:]) {
				t.Fatalf("append MD5 = %x, want %x", got, wantAppendMD5)
			}

			if tc.failVerify {
				f.Store = filer.NewFilerStoreWrapper(&inlineAppendUncertainUpdateStore{
					renameTestStore: store,
					err:             errors.New("injected ambiguous metadata update failure"),
				})
			} else if tc.failStore {
				f.Store = filer.NewFilerStoreWrapper(&inlineAppendFailUpdateStore{
					renameTestStore: store,
					err:             errors.New("injected metadata update failure"),
				})
			}

			result, err, cleanupChunks := fs.saveMetaData(ctx, r, "append.txt", "", &operation.StorageOption{}, 4, md5Hash.Sum(nil), fileChunks, chunkOffset, smallContent)
			if tc.failStore {
				if err == nil {
					t.Fatal("save appended entry succeeded despite injected metadata failure")
				}
				if tc.failVerify {
					if len(cleanupChunks) != 0 {
						t.Fatalf("unverified metadata state returned %d chunks for deletion, want retain all", len(cleanupChunks))
					}
				} else if len(cleanupChunks) != len(fileChunks)+1 {
					t.Fatalf("cleanup chunk count = %d, want uploaded append chunks %d plus one converted prefix chunk", len(cleanupChunks), len(fileChunks))
				}
				for _, chunk := range cleanupChunks {
					if _, found := uploadedChunks.get(chunk.GetFileIdString()); !found {
						t.Fatalf("cleanup includes chunk %s without uploaded data", chunk.GetFileIdString())
					}
				}
				unchanged, findErr := store.FindEntry(ctx, util.FullPath(filePath))
				if findErr != nil {
					t.Fatalf("find original entry after failed update: %v", findErr)
				}
				if !bytes.Equal(unchanged.Content, oldContent) || len(unchanged.Chunks) != 0 {
					t.Fatalf("failed update changed original entry: content=%q chunks=%d", unchanged.Content, len(unchanged.Chunks))
				}
				return
			}
			if err != nil {
				t.Fatalf("save appended entry: %v", err)
			}
			if len(cleanupChunks) != 0 {
				t.Fatalf("successful append returned uncommitted chunks: %#v", cleanupChunks)
			}

			updated, err := f.FindEntry(ctx, util.FullPath(filePath))
			if err != nil {
				t.Fatalf("find updated entry: %v", err)
			}
			if len(updated.Content) != 0 {
				t.Fatalf("promoted entry still has inline content %q", updated.Content)
			}
			if len(updated.Chunks) == 0 {
				t.Fatal("promoted entry has no chunks")
			}
			wantContent := append(append([]byte(nil), oldContent...), appendContent...)
			if result == nil || result.Size != int64(len(wantContent)) {
				t.Fatalf("result = %#v, want size %d", result, len(wantContent))
			}
			if updated.FileSize != uint64(len(wantContent)) {
				t.Fatalf("file size = %d, want %d", updated.FileSize, len(wantContent))
			}

			sort.Slice(updated.Chunks, func(i, j int) bool {
				return updated.Chunks[i].Offset < updated.Chunks[j].Offset
			})
			var gotContent []byte
			for _, chunk := range updated.Chunks {
				if chunk.Offset != int64(len(gotContent)) {
					t.Fatalf("chunk %s starts at %d, want contiguous offset %d", chunk.FileId, chunk.Offset, len(gotContent))
				}
				data, found := uploadedChunks.get(chunk.GetFileIdString())
				if !found {
					t.Fatalf("no uploaded data for chunk %s", chunk.GetFileIdString())
				}
				if uint64(len(data)) != chunk.Size {
					t.Fatalf("chunk %s metadata size = %d, uploaded size = %d", chunk.FileId, chunk.Size, len(data))
				}
				gotContent = append(gotContent, data...)
			}
			if !bytes.Equal(gotContent, wantContent) {
				t.Fatalf("chunk data = %q, want %q", gotContent, wantContent)
			}
		})
	}
}

func TestFilerInlineAppendRealThresholdBoundaries(t *testing.T) {
	const (
		filePath = "/append-boundary.bin"
		limit    = 65536
		oldSize  = 4096
	)
	for _, finalSize := range []int{limit - 1, limit, limit + 1} {
		t.Run(fmt.Sprintf("final_size_%d", finalSize), func(t *testing.T) {
			ctx := context.Background()
			oldContent := inlineAppendTestBytes(oldSize, 1)
			appendContent := inlineAppendTestBytes(finalSize-oldSize, 2)
			store := newRenameTestStore()
			f := newRenameTestFiler(t, store)
			if err := store.InsertEntry(ctx, &filer.Entry{
				FullPath: util.FullPath(filePath),
				Attr: filer.Attr{
					Mtime:    time.Unix(1, 0),
					Crtime:   time.Unix(1, 0),
					FileSize: uint64(len(oldContent)),
				},
				Content: oldContent,
			}); err != nil {
				t.Fatalf("insert inline entry: %v", err)
			}

			fs, uploaded := newInlineAppendUploadServer(t, f, limit, 1)
			r := httptest.NewRequest(http.MethodPut, filePath+"?op=append", bytes.NewReader(appendContent))
			r.ContentLength = -1
			fileChunks, md5Hash, chunkOffset, err, smallContent := fs.uploadRequestToChunks(
				ctx, httptest.NewRecorder(), r, r.Body, limit, "append-boundary.bin", "", -1, &operation.StorageOption{},
			)
			if err != nil {
				t.Fatalf("prepare append: %v", err)
			}
			if md5Hash == nil || chunkOffset != int64(len(appendContent)) {
				t.Fatalf("append digest/size = %v/%d, want non-nil digest and %d", md5Hash, chunkOffset, len(appendContent))
			}

			result, saveErr, cleanupChunks := fs.saveMetaData(ctx, r, "append-boundary.bin", "", &operation.StorageOption{}, limit, md5Hash.Sum(nil), fileChunks, chunkOffset, smallContent)
			if saveErr != nil {
				t.Fatalf("save appended entry: %v", saveErr)
			}
			if len(cleanupChunks) != 0 {
				t.Fatalf("successful append returned cleanup chunks: %#v", cleanupChunks)
			}
			updated, err := f.FindEntry(ctx, util.FullPath(filePath))
			if err != nil {
				t.Fatalf("find updated entry: %v", err)
			}
			if result == nil || result.Size != int64(finalSize) || updated.FileSize != uint64(finalSize) {
				t.Fatalf("result=%#v entry size=%d, want %d", result, updated.FileSize, finalSize)
			}

			wantContent := append(append([]byte(nil), oldContent...), appendContent...)
			if finalSize < limit {
				if len(fileChunks) != 0 || smallContent == nil || uploaded.count() != 0 {
					t.Fatalf("below-limit path uploaded data: chunks=%d inline=%d volume uploads=%d", len(fileChunks), len(smallContent), uploaded.count())
				}
				if !bytes.Equal(updated.Content, wantContent) || len(updated.Chunks) != 0 {
					t.Fatalf("below-limit entry: content size=%d chunks=%d", len(updated.Content), len(updated.Chunks))
				}
				return
			}

			if smallContent != nil || len(updated.Content) != 0 || len(updated.Chunks) == 0 {
				t.Fatalf("at/above-limit entry: inline=%d chunks=%d", len(updated.Content), len(updated.Chunks))
			}
			sort.Slice(updated.Chunks, func(i, j int) bool {
				return updated.Chunks[i].Offset < updated.Chunks[j].Offset
			})
			var reconstructed []byte
			for _, chunk := range updated.Chunks {
				if chunk.Offset != int64(len(reconstructed)) {
					t.Fatalf("chunk offset = %d, want %d", chunk.Offset, len(reconstructed))
				}
				data, found := uploaded.get(chunk.GetFileIdString())
				if !found {
					t.Fatalf("missing Volume data for %s", chunk.GetFileIdString())
				}
				if chunk.IsCompressed {
					data, err = util.DecompressData(data)
					if err != nil {
						t.Fatalf("decompress uploaded chunk %s: %v", chunk.GetFileIdString(), err)
					}
				}
				if uint64(len(data)) != chunk.Size {
					t.Fatalf("chunk size = %d, want %d", len(data), chunk.Size)
				}
				reconstructed = append(reconstructed, data...)
			}
			if !bytes.Equal(reconstructed, wantContent) {
				t.Fatalf("reconstructed %d bytes, want %d", len(reconstructed), len(wantContent))
			}
		})
	}
}

type inlineAppendFailUpdateStore struct {
	*renameTestStore
	err error
}

func (s *inlineAppendFailUpdateStore) UpdateEntry(context.Context, *filer.Entry) error {
	return s.err
}

type inlineAppendUncertainUpdateStore struct {
	*renameTestStore
	err          error
	updateFailed atomic.Bool
}

func (s *inlineAppendUncertainUpdateStore) UpdateEntry(context.Context, *filer.Entry) error {
	s.updateFailed.Store(true)
	return s.err
}

func (s *inlineAppendUncertainUpdateStore) FindEntry(ctx context.Context, path util.FullPath) (*filer.Entry, error) {
	if s.updateFailed.Load() {
		return nil, errors.New("injected verification lookup failure")
	}
	return s.renameTestStore.FindEntry(ctx, path)
}

type inlineAppendVolumeData struct {
	mu   sync.Mutex
	data map[string][]byte
}

func (d *inlineAppendVolumeData) get(fileID string) ([]byte, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	data, ok := d.data[fileID]
	return append([]byte(nil), data...), ok
}

func (d *inlineAppendVolumeData) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.data)
}

func inlineAppendTestBytes(length int, seed int64) []byte {
	data := make([]byte, length)
	_, _ = rand.New(rand.NewSource(seed)).Read(data)
	return data
}

type inlineAppendFakeMaster struct {
	master_pb.UnimplementedSeaweedServer
	volumeHost string
	nextFileID atomic.Uint32
}

func (m *inlineAppendFakeMaster) KeepConnected(stream grpc.BidiStreamingServer[master_pb.KeepConnectedRequest, master_pb.KeepConnectedResponse]) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	if err := stream.Send(&master_pb.KeepConnectedResponse{}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (m *inlineAppendFakeMaster) Assign(_ context.Context, _ *master_pb.AssignRequest) (*master_pb.AssignResponse, error) {
	n := m.nextFileID.Add(1)
	return &master_pb.AssignResponse{
		Fid:   fmt.Sprintf("1,%08x", n),
		Count: 1,
		Location: &master_pb.Location{
			Url:       m.volumeHost,
			PublicUrl: m.volumeHost,
		},
	}, nil
}

func newInlineAppendUploadServer(t *testing.T, f *filer.Filer, saveToFilerLimit int64, maxMB int) (*FilerServer, *inlineAppendVolumeData) {
	t.Helper()

	uploaded := &inlineAppendVolumeData{data: make(map[string][]byte)}
	volumeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		multipartReader, err := r.MultipartReader()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		part, err := multipartReader.NextPart()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		data, err := io.ReadAll(part)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fileID := strings.TrimPrefix(r.URL.Path, "/")
		uploaded.mu.Lock()
		uploaded.data[fileID] = append([]byte(nil), data...)
		uploaded.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprintf(w, `{"name":"chunk","size":%d}`, len(data))
	}))
	t.Cleanup(volumeServer.Close)

	volumeHost := strings.TrimPrefix(volumeServer.URL, "http://")
	masterAddr := startFakeMasterServerForLeaderLookup(t, &inlineAppendFakeMaster{volumeHost: volumeHost})
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	masterClient := wdclient.NewMasterClient(
		dialOption,
		"test",
		cluster.FilerType,
		pb.ServerAddress("localhost:0"),
		"",
		"",
		*pb.NewServiceDiscoveryFromMap(map[string]pb.ServerAddress{"master": masterAddr}),
	)
	f.MasterClient = masterClient
	masterCtx, cancelMaster := context.WithCancel(context.Background())
	t.Cleanup(cancelMaster)
	go masterClient.KeepConnectedToMaster(masterCtx)

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelWait()
	if masterClient.GetMaster(waitCtx) == "" {
		t.Fatal("fake master did not become available")
	}

	return &FilerServer{
		filer:          f,
		option:         &FilerOption{SaveToFilerLimit: saveToFilerLimit, MaxMB: maxMB},
		grpcDialOption: dialOption,
	}, uploaded
}
