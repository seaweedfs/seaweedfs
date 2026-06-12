package s3api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
)

func TestValidateRequestPath_RejectsTraversal(t *testing.T) {
	tests := []struct {
		name string
		// rawPath is sent as the Request-URI; net/http.NewRequest does not
		// rewrite the path, so `..` segments survive into mux when the router
		// is built with SkipClean(true) — matching the production setup in
		// weed/command/s3.go.
		rawPath  string
		wantCode int
	}{
		{"clean path passes", "/bucket-a/folder/file.txt", http.StatusOK},
		{"bucket only passes", "/bucket-a", http.StatusOK},
		{"trailing slash passes", "/bucket-a/folder/", http.StatusOK},

		{"leading dotdot rejected", "/bucket-a/../evil-bucket/test.txt", http.StatusBadRequest},
		{"nested dotdot rejected", "/bucket-a/good/../evil/test.txt", http.StatusBadRequest},
		{"backslash dotdot rejected", "/bucket-a/..\\evil\\test.txt", http.StatusBadRequest},
		{"percent-encoded dotdot rejected", "/bucket-a/%2e%2e/evil/test.txt", http.StatusBadRequest},
		{"bare dot object rejected", "/bucket-a/./evil/test.txt", http.StatusBadRequest},
		{"dotdot bucket rejected", "/../buckets/evil", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := mux.NewRouter().SkipClean(true)
			sub := router.PathPrefix("/{bucket}").Subrouter()
			sub.Use(validateRequestPath)
			handlerCalled := false
			pass := func(w http.ResponseWriter, r *http.Request) {
				handlerCalled = true
				w.WriteHeader(http.StatusOK)
			}
			// Mirror the production routes: /{bucket}/{object:(?s).+} for
			// object-scoped requests, bare /{bucket} for bucket-scoped ones.
			sub.Path("/{object:(?s).+}").HandlerFunc(pass)
			sub.Path("").HandlerFunc(pass)

			req := httptest.NewRequest(http.MethodGet, tt.rawPath, nil)
			rr := httptest.NewRecorder()
			router.ServeHTTP(rr, req)

			if rr.Code != tt.wantCode {
				t.Fatalf("path %q: got status %d, want %d (body=%q)", tt.rawPath, rr.Code, tt.wantCode, rr.Body.String())
			}
			if tt.wantCode == http.StatusBadRequest && handlerCalled {
				t.Fatalf("path %q: inner handler reached despite rejection", tt.rawPath)
			}
		})
	}
}

// Defense-in-depth: a future router or middleware that captures the {bucket}
// or {object} mux var as an empty string must still be rejected, even though
// mux's default `[^/]+` regex won't match an empty segment from a real URL.
func TestValidateRequestPath_RejectsEmptyCapturedVars(t *testing.T) {
	tests := []struct {
		name string
		vars map[string]string
	}{
		{"empty bucket", map[string]string{"bucket": "", "object": "key"}},
		{"empty object", map[string]string{"bucket": "bucket-a", "object": ""}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handlerCalled := false
			h := validateRequestPath(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handlerCalled = true
			}))
			req := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/", nil), tt.vars)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if handlerCalled {
				t.Fatalf("vars %v: inner handler reached despite empty capture", tt.vars)
			}
		})
	}
}

func TestValidateRequestPath_RejectsUnsafePathQueryValues(t *testing.T) {
	tests := []struct {
		name     string
		rawQuery string
		wantCode int
	}{
		{"clean version ID", "versionId=opaque-version", http.StatusOK},
		{"clean upload ID", "uploadId=opaque_upload", http.StatusOK},
		{"empty values", "versionId=&uploadId=", http.StatusOK},
		{"version ID encoded slash", "versionId=v1%2F..%2Fsecret", http.StatusBadRequest},
		{"version ID backslash", "versionId=v1%5C..%5Csecret", http.StatusBadRequest},
		{"upload ID traversal", "uploadId=hash%2F..%2F..%2Fvictim", http.StatusBadRequest},
		{"unsafe repeated value", "versionId=clean&versionId=bad%2Fvalue", http.StatusBadRequest},
		{"encoded version ID name", "version%49d=v1%2F..%2Fsecret", http.StatusBadRequest},
		{"encoded upload ID name", "upload%49d=hash%2F..%2Fvictim", http.StatusBadRequest},
		{"unrelated query", "prefix=folder&delimiter=%2F", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			h := validateRequestPath(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				w.WriteHeader(http.StatusOK)
			}))
			req := mux.SetURLVars(
				httptest.NewRequest(http.MethodGet, "/bucket-a/key?"+tt.rawQuery, nil),
				map[string]string{"bucket": "bucket-a", "object": "key"},
			)
			rr := httptest.NewRecorder()

			h.ServeHTTP(rr, req)

			if rr.Code != tt.wantCode {
				t.Fatalf("query %q: got status %d, want %d", tt.rawQuery, rr.Code, tt.wantCode)
			}
			if tt.wantCode != http.StatusOK && called {
				t.Fatalf("query %q: inner handler reached despite rejection", tt.rawQuery)
			}
		})
	}
}

func TestHasPathSegmentQuery_CommonPathDoesNotAllocate(t *testing.T) {
	allocations := testing.AllocsPerRun(1000, func() {
		if hasPathSegmentQuery("prefix=folder&delimiter=%2F") {
			t.Fatal("unrelated query recognized as a path segment query")
		}
	})
	if allocations != 0 {
		t.Fatalf("common query path allocated %v times per run, want 0", allocations)
	}
}
