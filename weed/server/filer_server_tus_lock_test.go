package weed_server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestFilerServer_tusHandler_ConcurrentMutationLocked verifies a session with a
// mutating request in flight refuses a second PATCH or DELETE with 423 while
// HEAD still answers, and accepts the retry once the first request finishes.
func TestFilerServer_tusHandler_ConcurrentMutationLocked(t *testing.T) {
	fs, _ := newTusTestServer(t, map[string]string{tusTestUploadID: "/buckets/data/file.bin"})

	if !fs.lockTusUpload(tusTestUploadID) {
		t.Fatal("lockTusUpload failed on an idle session")
	}

	do := func(method string, headers map[string]string) int {
		req := tusRequest(method, "/.tus/.uploads/"+tusTestUploadID, headers, "")
		rec := httptest.NewRecorder()
		fs.tusHandler(rec, req)
		return rec.Code
	}
	patchHeaders := map[string]string{
		"Authorization": "Bearer " + signFilerToken(t, tusTestWriteKey, nil, nil),
		"Content-Type":  "application/offset+octet-stream",
		"Upload-Offset": "0",
	}
	deleteHeaders := map[string]string{
		"Authorization": "Bearer " + signFilerToken(t, tusTestWriteKey, nil, nil),
	}
	headHeaders := map[string]string{
		"Authorization": "Bearer " + signFilerToken(t, tusTestReadKey, nil, nil),
	}

	if code := do(http.MethodPatch, patchHeaders); code != http.StatusLocked {
		t.Errorf("PATCH while locked = %d, want %d", code, http.StatusLocked)
	}
	if code := do(http.MethodDelete, deleteHeaders); code != http.StatusLocked {
		t.Errorf("DELETE while locked = %d, want %d", code, http.StatusLocked)
	}
	if code := do(http.MethodHead, headHeaders); code != http.StatusOK {
		t.Errorf("HEAD while locked = %d, want %d", code, http.StatusOK)
	}

	fs.unlockTusUpload(tusTestUploadID)
	if code := do(http.MethodPatch, patchHeaders); code != http.StatusNoContent {
		t.Errorf("PATCH after unlock = %d, want %d", code, http.StatusNoContent)
	}
}
