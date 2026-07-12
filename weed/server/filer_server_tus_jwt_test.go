package weed_server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	tusTestWriteKey = "write-secret"
	tusTestReadKey  = "read-secret"
	tusTestUploadID = "9f6f0d4b-6556-48f6-b953-0d8fca1966f1"
)

// signFilerToken builds a signed filer JWT for tests.
func signFilerToken(t *testing.T, signingKey string, allowedPrefixes, allowedMethods []string) string {
	t.Helper()
	claims := security.SeaweedFilerClaims{
		AllowedPrefixes: allowedPrefixes,
		AllowedMethods:  allowedMethods,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour)),
		},
	}
	str, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(signingKey))
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}
	return str
}

// newTusTestServer builds a FilerServer backed by an in-memory store seeded with
// TUS sessions (uploadID -> stored TargetPath), so the handler can resolve a
// session's target the way production does. The store is returned so a test can
// mutate or inspect the seeded session.
func newTusTestServer(t *testing.T, sessions map[string]string) (*FilerServer, *renameTestStore) {
	t.Helper()
	store := newRenameTestStore()
	fs := &FilerServer{
		filer:      newRenameTestFiler(store),
		filerGuard: security.NewGuard(nil, tusTestWriteKey, 0, tusTestReadKey, 0),
		option:     &FilerOption{TusBasePath: "/.tus"},
	}
	for uploadID, targetPath := range sessions {
		seedTusSession(t, fs, store, TusSession{ID: uploadID, TargetPath: targetPath, Size: 1})
	}
	return fs, store
}

// seedTusSession writes a session directory and its .info metadata into the store.
func seedTusSession(t *testing.T, fs *FilerServer, store *renameTestStore, session TusSession) {
	t.Helper()
	data, err := json.Marshal(&session)
	if err != nil {
		t.Fatalf("marshal session %s: %v", session.ID, err)
	}
	dir := &filer.Entry{FullPath: util.FullPath(fs.tusSessionPath(session.ID)), Attr: filer.Attr{Mode: os.ModeDir | 0755}}
	info := &filer.Entry{FullPath: util.FullPath(fs.tusSessionInfoPath(session.ID)), Content: data}
	for _, entry := range []*filer.Entry{dir, info} {
		if err := store.InsertEntry(context.Background(), entry); err != nil {
			t.Fatalf("seed session %s: %v", session.ID, err)
		}
	}
}

func TestFilerServer_checkJwtAuthorization(t *testing.T) {
	fs := &FilerServer{
		filerGuard: security.NewGuard(nil, tusTestWriteKey, 0, tusTestReadKey, 0),
		option:     &FilerOption{TusBasePath: "/.tus"},
	}
	victim := []string{"/buckets/secret/victim.bin"}
	uploadPath := "/.tus/.uploads/" + tusTestUploadID

	tests := []struct {
		name             string
		method           string
		path             string
		token            string
		scopedPaths      []string
		expectAuthorized bool
	}{
		// With a filer signing key configured, an unauthenticated request is denied.
		{"post without token denied", http.MethodPost, "/.tus/buckets/secret/owned.txt", "", []string{"/buckets/secret/owned.txt"}, false},
		{"patch without token denied", http.MethodPatch, uploadPath, "", victim, false},
		{"head without token denied", http.MethodHead, uploadPath, "", victim, false},

		// A valid token for the right access level is accepted.
		{"post write token allowed", http.MethodPost, "/.tus/buckets/data/ok.txt", signFilerToken(t, tusTestWriteKey, nil, nil), []string{"/buckets/data/ok.txt"}, true},
		{"head read token allowed", http.MethodHead, uploadPath, signFilerToken(t, tusTestReadKey, nil, nil), victim, true},

		// HEAD is a read, so a write token must not authorize it, nor a read token a write.
		{"head write token denied", http.MethodHead, uploadPath, signFilerToken(t, tusTestWriteKey, nil, nil), victim, false},
		{"post read token denied", http.MethodPost, "/.tus/buckets/data/ok.txt", signFilerToken(t, tusTestReadKey, nil, nil), []string{"/buckets/data/ok.txt"}, false},

		// Prefix-restricted tokens are scoped against the resolved target path.
		{"within allowed prefix", http.MethodPost, "/.tus/buckets/allowed/ok.txt", signFilerToken(t, tusTestWriteKey, []string{"/buckets/allowed"}, nil), []string{"/buckets/allowed/ok.txt"}, true},
		{"outside allowed prefix denied", http.MethodPost, "/.tus/buckets/secret/owned.txt", signFilerToken(t, tusTestWriteKey, []string{"/buckets/allowed"}, nil), []string{"/buckets/secret/owned.txt"}, false},

		// Fail closed: a prefix-restricted token with no resolved resource is denied.
		{"restricted token without resource denied", http.MethodPatch, uploadPath, signFilerToken(t, tusTestWriteKey, []string{"/buckets/allowed"}, nil), nil, false},

		// Method-restricted tokens are checked against the actual HTTP method.
		{"matching method allowed", http.MethodPatch, uploadPath, signFilerToken(t, tusTestWriteKey, nil, []string{"POST", "PATCH", "DELETE"}), victim, true},
		{"method not allowed denied", http.MethodPatch, uploadPath, signFilerToken(t, tusTestWriteKey, nil, []string{"POST"}), victim, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			if tt.token != "" {
				req.Header.Set("Authorization", "Bearer "+tt.token)
			}
			if got := fs.checkJwtAuthorization(req, tt.method != http.MethodHead, tt.scopedPaths); got != tt.expectAuthorized {
				t.Errorf("checkJwtAuthorization(%s %s) = %v, want %v", tt.method, tt.path, got, tt.expectAuthorized)
			}
		})
	}
}

// TestFilerServer_tusHandler_UnauthenticatedRejected exercises the full handler
// entry point: OPTIONS discovery stays open, but an unauthenticated request is
// rejected with 401 before any session is created or looked up.
func TestFilerServer_tusHandler_UnauthenticatedRejected(t *testing.T) {
	fs, _ := newTusTestServer(t, map[string]string{tusTestUploadID: "/buckets/secret/victim.bin"})

	// OPTIONS is capability discovery and must not require a token.
	optionsReq := httptest.NewRequest(http.MethodOptions, "/.tus/buckets/secret/owned.txt", nil)
	optionsRec := httptest.NewRecorder()
	fs.tusHandler(optionsRec, optionsReq)
	if optionsRec.Code != http.StatusOK {
		t.Errorf("OPTIONS without token = %d, want %d", optionsRec.Code, http.StatusOK)
	}

	// POST without a token must be rejected before touching the filer store.
	postReq := httptest.NewRequest(http.MethodPost, "/.tus/buckets/secret/owned.txt", nil)
	postReq.Header.Set("Tus-Resumable", TusVersion)
	postReq.Header.Set("Upload-Length", "5")
	postRec := httptest.NewRecorder()
	fs.tusHandler(postRec, postReq)
	if postRec.Code != http.StatusUnauthorized {
		t.Errorf("unauthenticated POST = %d, want %d", postRec.Code, http.StatusUnauthorized)
	}

	// A missing credential is rejected before the upload id triggers a metadata
	// lookup, so an existing session is not an unauthenticated resource oracle.
	headReq := httptest.NewRequest(http.MethodHead, "/.tus/.uploads/"+tusTestUploadID, nil)
	headReq.Header.Set("Tus-Resumable", TusVersion)
	headRec := httptest.NewRecorder()
	fs.tusHandler(headRec, headReq)
	if headRec.Code != http.StatusUnauthorized {
		t.Errorf("unauthenticated HEAD = %d, want %d", headRec.Code, http.StatusUnauthorized)
	}
}

// TestFilerServer_refreshTusSessionChunks_RevalidatesPinnedSession verifies a
// pinned session cannot complete after it is deleted or its target is replaced
// between authorization and completion.
func TestFilerServer_refreshTusSessionChunks_RevalidatesPinnedSession(t *testing.T) {
	pin := func(t *testing.T) (*FilerServer, *renameTestStore, *TusSession) {
		t.Helper()
		fs, store := newTusTestServer(t, nil)
		seedTusSession(t, fs, store, TusSession{
			ID:         tusTestUploadID,
			TargetPath: "/buckets/secret/victim.bin",
			Size:       1,
			CreatedAt:  time.Unix(1700000000, 123),
		})
		session, err := fs.readTusSessionInfo(context.Background(), tusTestUploadID)
		if err != nil {
			t.Fatalf("pin session: %v", err)
		}
		return fs, store, session
	}

	t.Run("deleted before completion", func(t *testing.T) {
		fs, store, session := pin(t)
		if err := store.DeleteEntry(context.Background(), util.FullPath(fs.tusSessionInfoPath(tusTestUploadID))); err != nil {
			t.Fatalf("delete session info: %v", err)
		}
		if err := fs.refreshTusSessionChunks(context.Background(), session); err == nil {
			t.Fatal("refresh succeeded after the session was deleted")
		}
	})

	t.Run("replaced before completion", func(t *testing.T) {
		fs, store, session := pin(t)
		replaced := *session
		replaced.TargetPath = "/buckets/other/replacement.bin"
		data, err := json.Marshal(&replaced)
		if err != nil {
			t.Fatalf("marshal replacement: %v", err)
		}
		if err := store.InsertEntry(context.Background(), &filer.Entry{
			FullPath: util.FullPath(fs.tusSessionInfoPath(tusTestUploadID)),
			Content:  data,
		}); err != nil {
			t.Fatalf("replace session info: %v", err)
		}
		if err := fs.refreshTusSessionChunks(context.Background(), session); err == nil {
			t.Fatal("refresh succeeded after the session target was replaced")
		}
	})
}
