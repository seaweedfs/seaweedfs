package weed_server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// newTusIDORTestServer builds a FilerServer backed by an in-memory store seeded
// with TUS sessions (uploadID -> stored TargetPath), so the JWT check can resolve
// a session's target the way the real handler does. The store is returned so a
// test can seed additional (e.g. corrupt) session entries.
func newTusIDORTestServer(t *testing.T, writeKey, readKey string, sessions map[string]string) (*FilerServer, *renameTestStore) {
	t.Helper()
	store := newRenameTestStore()
	fs := &FilerServer{
		filer:      newRenameTestFiler(store),
		filerGuard: security.NewGuard(nil, writeKey, 0, readKey, 0),
		option:     &FilerOption{TusBasePath: "/.tus"},
	}
	for uploadID, targetPath := range sessions {
		data, err := json.Marshal(&TusSession{ID: uploadID, TargetPath: targetPath, Size: 46})
		if err != nil {
			t.Fatalf("marshal session %s: %v", uploadID, err)
		}
		if err := store.InsertEntry(context.Background(), &filer.Entry{FullPath: util.FullPath(fs.tusSessionInfoPath(uploadID)), Content: data}); err != nil {
			t.Fatalf("seed session %s: %v", uploadID, err)
		}
	}
	return fs, store
}

// TestFilerServer_checkTusJwtAuthorization_CrossPrefixSessionHijack reproduces
// GHSA-99q7-x53r-6j4g: a prefix-restricted token acting on another tenant's
// existing TUS session (HEAD/PATCH/DELETE) must be scoped against the session's
// stored TargetPath, not authorized on signature and method alone.
func TestFilerServer_checkTusJwtAuthorization_CrossPrefixSessionHijack(t *testing.T) {
	const writeKey = "write-secret"
	const readKey = "read-secret"

	fs, store := newTusIDORTestServer(t, writeKey, readKey, map[string]string{
		"victim-session": "/buckets/secret/victim.bin",
		"own-session":    "/buckets/allowed/own.bin",
	})

	// A session whose .info is unreadable (corrupt JSON) must fail closed rather
	// than authorize a prefix-restricted token against a target we cannot resolve.
	if err := store.InsertEntry(context.Background(), &filer.Entry{
		FullPath: util.FullPath(fs.tusSessionInfoPath("corrupt-session")),
		Content:  []byte("{not valid json"),
	}); err != nil {
		t.Fatalf("seed corrupt session: %v", err)
	}

	attackerWrite := signFilerToken(t, writeKey, []string{"/buckets/allowed"}, nil)
	attackerRead := signFilerToken(t, readKey, []string{"/buckets/allowed"}, nil)

	tests := []struct {
		name             string
		method           string
		path             string
		token            string
		expectAuthorized bool
	}{
		// The IDOR: a token scoped to /buckets/allowed must not act on a session
		// whose target is /buckets/secret, regardless of the verb.
		{"patch victim session denied", http.MethodPatch, "/.tus/.uploads/victim-session", attackerWrite, false},
		{"delete victim session denied", http.MethodDelete, "/.tus/.uploads/victim-session", attackerWrite, false},
		{"head victim session denied", http.MethodHead, "/.tus/.uploads/victim-session", attackerRead, false},

		// The same token acting on its own in-prefix session is still allowed.
		{"patch own session allowed", http.MethodPatch, "/.tus/.uploads/own-session", attackerWrite, true},
		{"delete own session allowed", http.MethodDelete, "/.tus/.uploads/own-session", attackerWrite, true},
		{"head own session allowed", http.MethodHead, "/.tus/.uploads/own-session", attackerRead, true},

		// An unrestricted token (no AllowedPrefixes) keeps working and triggers no
		// session lookup.
		{"patch unrestricted allowed", http.MethodPatch, "/.tus/.uploads/victim-session", signFilerToken(t, writeKey, nil, nil), true},

		// An unknown session leaves the request unscoped so the handler can answer
		// 404, rather than being denied on a path that cannot be resolved.
		{"patch unknown session allowed", http.MethodPatch, "/.tus/.uploads/does-not-exist", attackerWrite, true},

		// A corrupt/unreadable session fails closed: the target cannot be resolved
		// so a prefix-restricted token must be denied, not authorized.
		{"patch corrupt session denied", http.MethodPatch, "/.tus/.uploads/corrupt-session", attackerWrite, false},
		{"head corrupt session denied", http.MethodHead, "/.tus/.uploads/corrupt-session", attackerRead, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			req.Header.Set("Authorization", "Bearer "+tt.token)
			if got := fs.checkTusJwtAuthorization(req); got != tt.expectAuthorized {
				t.Errorf("checkTusJwtAuthorization(%s %s) = %v, want %v", tt.method, tt.path, got, tt.expectAuthorized)
			}
		})
	}
}

// TestFilerServer_tusHandler_CrossPrefixPatchRejected drives the full handler:
// an attacker PATCH against another tenant's session must be rejected with 401
// before any bytes are written to the session's target path.
func TestFilerServer_tusHandler_CrossPrefixPatchRejected(t *testing.T) {
	const writeKey = "write-secret"
	const readKey = "read-secret"

	fs, _ := newTusIDORTestServer(t, writeKey, readKey, map[string]string{
		"victim-session": "/buckets/secret/victim.bin",
	})
	attacker := signFilerToken(t, writeKey, []string{"/buckets/allowed"}, nil)

	req := httptest.NewRequest(http.MethodPatch, "/.tus/.uploads/victim-session", strings.NewReader("PWNED-BY-CROSS-PREFIX-TUS-SESSION-HIJACK"))
	req.Header.Set("Tus-Resumable", TusVersion)
	req.Header.Set("Upload-Offset", "0")
	req.Header.Set("Content-Type", "application/offset+octet-stream")
	req.Header.Set("Authorization", "Bearer "+attacker)

	rec := httptest.NewRecorder()
	fs.tusHandler(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("cross-prefix PATCH = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}
