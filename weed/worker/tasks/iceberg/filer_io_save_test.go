package iceberg

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

// fakeVolumeServer accepts chunk uploads and records what it received, so a
// test can check the bytes actually left the filer entry.
type fakeVolumeServer struct {
	mu     sync.Mutex
	server *httptest.Server
	parts  map[string][]byte // fid → data
}

func startFakeVolumeServer(t *testing.T) *fakeVolumeServer {
	t.Helper()
	initGlobalHTTPClientOnce.Do(util_http.InitGlobalHttpClient)

	v := &fakeVolumeServer{parts: make(map[string][]byte)}
	v.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fid := strings.TrimPrefix(r.URL.Path, "/")
		// The uploader sends the payload as a single form-data part named
		// "file" with an empty filename, which FormFile will not match.
		reader, err := r.MultipartReader()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		part, err := reader.NextPart()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		data, err := io.ReadAll(part)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		v.mu.Lock()
		v.parts[fid] = data
		v.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"name": fid, "size": len(data)})
	}))
	t.Cleanup(v.server.Close)
	return v
}

func (v *fakeVolumeServer) hostPort(t *testing.T) string {
	t.Helper()
	u, err := url.Parse(v.server.URL)
	if err != nil {
		t.Fatalf("parse fake volume url: %v", err)
	}
	return u.Host
}

func (v *fakeVolumeServer) uploaded() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.parts)
}

func (v *fakeVolumeServer) dataFor(fid string) []byte {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.parts[fid]
}

// AssignVolume on the fake filer hands out sequential fids pointing at the
// fake volume server.
func (f *fakeFilerServer) enableAssign(volumeServer string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.assignVolumeServer = volumeServer
}

func TestSaveFilerFileKeepsSmallContentInline(t *testing.T) {
	fakeServer, client := startFakeFiler(t)

	content := []byte(strings.Repeat("m", inlineContentLimit))
	if err := saveFilerFile(context.Background(), client, "/buckets/lake/ns/tbl/metadata", "snap.avro", content); err != nil {
		t.Fatalf("saveFilerFile: %v", err)
	}

	entry := fakeServer.getEntry("/buckets/lake/ns/tbl/metadata", "snap.avro")
	if entry == nil {
		t.Fatal("entry not created")
	}
	if len(entry.Content) != len(content) {
		t.Errorf("inline content is %d bytes, want %d", len(entry.Content), len(content))
	}
	if len(entry.Chunks) != 0 {
		t.Errorf("small file got %d chunks, want inline only", len(entry.Chunks))
	}
}

// A compacted data file must land in volumes, not in Entry.Content: an inline
// entry that big is stored verbatim by the filer store and rides the metadata
// change log as one event too large for a volume server to accept.
func TestSaveFilerFileChunksLargeContent(t *testing.T) {
	fakeServer, client := startFakeFiler(t)
	volumeServer := startFakeVolumeServer(t)
	fakeServer.enableAssign(volumeServer.hostPort(t))

	content := make([]byte, 3*filerFileChunkSize/2)
	for i := range content {
		content[i] = byte(i)
	}

	if err := saveFilerFile(context.Background(), client, "/buckets/lake/ns/tbl/data", "compact-1.parquet", content); err != nil {
		t.Fatalf("saveFilerFile: %v", err)
	}

	entry := fakeServer.getEntry("/buckets/lake/ns/tbl/data", "compact-1.parquet")
	if entry == nil {
		t.Fatal("entry not created")
	}
	if len(entry.Content) != 0 {
		t.Errorf("large file kept %d bytes inline, want none", len(entry.Content))
	}
	if len(entry.Chunks) != 2 {
		t.Fatalf("got %d chunks, want 2", len(entry.Chunks))
	}
	if entry.Attributes.FileSize != uint64(len(content)) {
		t.Errorf("file size %d, want %d", entry.Attributes.FileSize, len(content))
	}
	if volumeServer.uploaded() != 2 {
		t.Errorf("volume server received %d uploads, want 2", volumeServer.uploaded())
	}

	// Reassembling the chunks in offset order gives back the original bytes.
	rejoined := make([]byte, len(content))
	for _, chunk := range entry.Chunks {
		data := volumeServer.dataFor(chunk.FileId)
		if uint64(len(data)) != chunk.Size {
			t.Fatalf("chunk %s holds %d bytes, entry says %d", chunk.FileId, len(data), chunk.Size)
		}
		copy(rejoined[chunk.Offset:], data)
	}
	if string(rejoined) != string(content) {
		t.Error("reassembled chunks differ from the original content")
	}
}

// A failed upload must not silently fall back to an inline entry.
func TestSaveFilerFileFailsWhenUploadFails(t *testing.T) {
	fakeServer, client := startFakeFiler(t)

	content := make([]byte, filerFileChunkSize+1)
	err := saveFilerFile(context.Background(), client, "/buckets/lake/ns/tbl/data", "compact-2.parquet", content)
	if err == nil {
		t.Fatal("expected an error when no volume can be assigned")
	}
	if entry := fakeServer.getEntry("/buckets/lake/ns/tbl/data", "compact-2.parquet"); entry != nil {
		t.Errorf("entry created despite the failed upload: %d inline bytes", len(entry.Content))
	}
}
