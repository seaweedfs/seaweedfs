package weed_server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestFilerServer_tusHandler_TransientChunkLoadFailure reproduces #11151: a
// transient failure listing a session's chunk directory (volume timeout,
// canceled context) must not be reported as 404/204, since readTusSessionInfo
// already proved the session exists. A false "not found" makes a compliant
// client discard a live session and orphan every chunk it committed.
func TestFilerServer_tusHandler_TransientChunkLoadFailure(t *testing.T) {
	tests := []struct {
		name   string
		method string
	}{
		{"HEAD", http.MethodHead},
		{"PATCH", http.MethodPatch},
		{"DELETE", http.MethodDelete},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs, store := newTusTestServer(t, map[string]string{tusTestUploadID: "/buckets/data/file.bin"})
			store.listErr = context.Canceled

			headers := map[string]string{"Authorization": "Bearer " + signFilerToken(t, tusTestWriteKey, nil, nil)}
			if tt.method == http.MethodHead {
				headers["Authorization"] = "Bearer " + signFilerToken(t, tusTestReadKey, nil, nil)
			}
			if tt.method == http.MethodPatch {
				headers["Content-Type"] = "application/offset+octet-stream"
				headers["Upload-Offset"] = "0"
			}
			req := tusRequest(tt.method, "/.tus/.uploads/"+tusTestUploadID, headers, "")
			rec := httptest.NewRecorder()
			fs.tusHandler(rec, req)

			if rec.Code == http.StatusNotFound || rec.Code == http.StatusNoContent {
				t.Fatalf("%s on a transient chunk-load failure = %d, want a server error, not a not-found status", tt.method, rec.Code)
			}
			if rec.Code < 500 {
				t.Errorf("%s on a transient chunk-load failure = %d, want a 5xx status", tt.method, rec.Code)
			}

			store.listErr = nil
			if _, err := store.FindEntry(context.Background(), util.FullPath(fs.tusSessionInfoPath(tusTestUploadID))); err != nil {
				t.Errorf("session removed after a transient %s failure: %v", tt.method, err)
			}
		})
	}
}

// TestFilerServer_tusHandler_ChunkLoadNotFoundStillNotFound verifies a chunk
// listing failure that genuinely means "not found" is still reported as such,
// so the fix for #11151 does not mask a real absence.
func TestFilerServer_tusHandler_ChunkLoadNotFoundStillNotFound(t *testing.T) {
	fs, store := newTusTestServer(t, map[string]string{tusTestUploadID: "/buckets/data/file.bin"})
	store.listErr = filer_pb.ErrNotFound

	req := tusRequest(http.MethodHead, "/.tus/.uploads/"+tusTestUploadID, map[string]string{
		"Authorization": "Bearer " + signFilerToken(t, tusTestReadKey, nil, nil),
	}, "")
	rec := httptest.NewRecorder()
	fs.tusHandler(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("HEAD on a genuinely missing session = %d, want %d", rec.Code, http.StatusNotFound)
	}

	deleteReq := tusRequest(http.MethodDelete, "/.tus/.uploads/"+tusTestUploadID, map[string]string{
		"Authorization": "Bearer " + signFilerToken(t, tusTestWriteKey, nil, nil),
	}, "")
	deleteRec := httptest.NewRecorder()
	fs.tusHandler(deleteRec, deleteReq)

	if deleteRec.Code != http.StatusNoContent {
		t.Fatalf("DELETE on a genuinely missing session = %d, want %d", deleteRec.Code, http.StatusNoContent)
	}
}
