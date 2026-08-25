package weed_server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestFilerServer_completeTusUpload_OverlappingRecords verifies a session whose
// chunk records overlap - a PATCH retried while its predecessor was still
// storing a sub-chunk - still completes: coverage is validated the way HEAD
// computes the offset, and only records extending coverage join the entry.
func TestFilerServer_completeTusUpload_OverlappingRecords(t *testing.T) {
	fidA, fidB, fidDup := "3,01637037d6", "4,02637037d6", "5,03637037d6"

	tests := []struct {
		name       string
		chunks     [][3]int64 // offset, size, fid index into fids
		wantChunks [][2]int64 // offset, size expected on the entry
	}{
		{
			name:       "exact duplicate dropped",
			chunks:     [][3]int64{{0, 8, 0}, {0, 8, 2}, {8, 4, 1}},
			wantChunks: [][2]int64{{0, 8}, {8, 4}},
		},
		{
			name:       "partial overlap kept",
			chunks:     [][3]int64{{0, 8, 0}, {6, 6, 2}},
			wantChunks: [][2]int64{{0, 8}, {6, 6}},
		},
	}

	fids := []string{fidA, fidB, fidDup}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs, store := newTusTestServer(t, nil)
			targetPath := "/buckets/data/raced.bin"
			size := int64(12)
			seedTusSession(t, fs, store, TusSession{ID: tusTestUploadID, TargetPath: targetPath, Size: size})
			for _, c := range tt.chunks {
				seedTusChunk(t, fs, store, tusTestUploadID, c[0], c[1], fids[c[2]])
			}

			// a zero-length PATCH at the reported offset triggers completion
			req := tusRequest(http.MethodPatch, "/.tus/.uploads/"+tusTestUploadID, map[string]string{
				"Authorization": "Bearer " + signFilerToken(t, tusTestWriteKey, nil, nil),
				"Content-Type":  "application/offset+octet-stream",
				"Upload-Offset": "12",
			}, "")
			rec := httptest.NewRecorder()
			fs.tusHandler(rec, req)

			if rec.Code != http.StatusNoContent {
				t.Fatalf("PATCH = %d, want %d; body=%q", rec.Code, http.StatusNoContent, rec.Body.String())
			}
			entry, err := store.FindEntry(context.Background(), util.FullPath(targetPath))
			if err != nil {
				t.Fatalf("final entry not created: %v", err)
			}
			chunks := entry.GetChunks()
			if len(chunks) != len(tt.wantChunks) {
				t.Fatalf("entry chunks = %d, want %d", len(chunks), len(tt.wantChunks))
			}
			for i, want := range tt.wantChunks {
				if chunks[i].Offset != want[0] || int64(chunks[i].Size) != want[1] {
					t.Errorf("chunk[%d] = @%d+%d, want @%d+%d", i, chunks[i].Offset, chunks[i].Size, want[0], want[1])
				}
			}
			if _, err := store.FindEntry(context.Background(), util.FullPath(fs.tusSessionInfoPath(tusTestUploadID))); err == nil {
				t.Errorf("session still present after completion")
			}
		})
	}
}

// TestFilerServer_completeTusUpload_GapRejected verifies records that do not
// cover the full size still fail completion.
func TestFilerServer_completeTusUpload_GapRejected(t *testing.T) {
	fs, store := newTusTestServer(t, nil)
	targetPath := "/buckets/data/gap.bin"
	seedTusSession(t, fs, store, TusSession{ID: tusTestUploadID, TargetPath: targetPath, Size: 12})

	session := &TusSession{
		ID:         tusTestUploadID,
		TargetPath: targetPath,
		Size:       12,
		Offset:     12,
		Chunks: []*TusChunkInfo{
			{Offset: 0, Size: 8, FileId: "3,01637037d6"},
			{Offset: 9, Size: 3, FileId: "4,02637037d6"},
		},
	}
	err := fs.completeTusUpload(context.Background(), session)
	if err == nil || !strings.Contains(err.Error(), "chunk gap") {
		t.Fatalf("completeTusUpload err = %v, want a chunk gap error", err)
	}
	if _, findErr := store.FindEntry(context.Background(), util.FullPath(targetPath)); findErr == nil {
		t.Fatalf("entry created despite gap")
	}
}
