package weed_server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// TestFilerServer_tusHandler_CrossPrefixSessionHijack reproduces
// GHSA-99q7-x53r-6j4g: a prefix-restricted token acting on another tenant's TUS
// session (HEAD/PATCH/DELETE) must be scoped against the session's stored
// TargetPath, not authorized on signature and method alone. The victim's session
// must survive a denied mutation.
func TestFilerServer_tusHandler_CrossPrefixSessionHijack(t *testing.T) {
	tests := []struct {
		name         string
		method       string
		prefix       string
		expectStatus int
		expectExists bool
	}{
		{"cross-prefix HEAD denied", http.MethodHead, "/buckets/allowed", http.StatusUnauthorized, true},
		{"matching-prefix HEAD allowed", http.MethodHead, "/buckets/secret", http.StatusOK, true},
		{"cross-prefix PATCH denied", http.MethodPatch, "/buckets/allowed", http.StatusUnauthorized, true},
		{"matching-prefix PATCH allowed", http.MethodPatch, "/buckets/secret", http.StatusNoContent, true},
		{"cross-prefix DELETE denied", http.MethodDelete, "/buckets/allowed", http.StatusUnauthorized, true},
		{"matching-prefix DELETE allowed", http.MethodDelete, "/buckets/secret", http.StatusNoContent, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs, store := newTusTestServer(t, map[string]string{tusTestUploadID: "/buckets/secret/victim.bin"})

			signingKey := tusTestWriteKey
			if tt.method == http.MethodHead {
				signingKey = tusTestReadKey
			}
			token := signFilerToken(t, signingKey, []string{tt.prefix}, nil)
			req := httptest.NewRequest(tt.method, "/.tus/.uploads/"+tusTestUploadID, http.NoBody)
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Tus-Resumable", TusVersion)
			if tt.method == http.MethodPatch {
				req.Header.Set("Content-Type", "application/offset+octet-stream")
				req.Header.Set("Upload-Offset", "0")
			}
			rec := httptest.NewRecorder()

			fs.tusHandler(rec, req)

			if rec.Code != tt.expectStatus {
				t.Fatalf("%s status = %d, want %d; body=%q", tt.method, rec.Code, tt.expectStatus, rec.Body.String())
			}
			_, err := store.FindEntry(context.Background(), util.FullPath(fs.tusSessionInfoPath(tusTestUploadID)))
			if tt.expectExists && err != nil {
				t.Fatalf("session removed after %s: %v", tt.method, err)
			}
			if !tt.expectExists && err == nil {
				t.Fatalf("session still present after authorized %s", tt.method)
			}
		})
	}
}
