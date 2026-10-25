package iceberg

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// The catch-all answers anything unrouted, so a path that reaches it is a
// route that does not exist.
func TestRegisteredRoutesReachAHandler(t *testing.T) {
	router := mux.NewRouter().SkipClean(true)
	(&Server{}).RegisterRoutes(router)

	cases := []struct {
		method string
		target string
	}{
		{http.MethodPost, "/v1/views/rename"},
		{http.MethodPost, "/v1/warehouse/views/rename"},
		{http.MethodPost, "/v1/namespaces/ns/tables/t/metrics"},
		{http.MethodPost, "/v1/warehouse/namespaces/ns/tables/t/metrics"},
	}

	for _, tc := range cases {
		var match mux.RouteMatch
		if !router.Match(httptest.NewRequest(tc.method, tc.target, nil), &match) {
			t.Errorf("%s %s matched no route", tc.method, tc.target)
			continue
		}
		if match.MatchErr != nil {
			t.Errorf("%s %s: %v", tc.method, tc.target, match.MatchErr)
			continue
		}
		if tmpl, err := match.Route.GetPathTemplate(); err == nil && tmpl == "/" {
			t.Errorf("%s %s fell through to the catch-all", tc.method, tc.target)
		}
	}
}

// A catalog URI configured with a trailing /v1 (a common iceberg-java client
// mistake) produces /v1/v1/... requests; the router collapses the repeated
// prefix instead of answering "Path not found" or resolving "v1" as a table
// bucket name.
func TestDoubledV1PrefixIsCollapsed(t *testing.T) {
	router := mux.NewRouter().SkipClean(true)
	(&Server{}).RegisterRoutes(router)

	for _, target := range []string{
		"/v1/v1/config?warehouse=bucket",
		// Degenerate repetition collapses in a single pass.
		"/v1/v1/v1/config",
	} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: got %d, want 200: %s", target, rec.Code, rec.Body.String())
		}
	}

	// /v1/v1/namespaces also matches the {prefix} routes ("v1" can never be a
	// table bucket name); it must still collapse to the unprefixed endpoint,
	// which requires auth here rather than answering 404.
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/v1/namespaces", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("/v1/v1/namespaces: got %d, want 401: %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/nonexistent/path", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("/v1/nonexistent/path: got %d, want 404", rec.Code)
	}
}

// The rewrite must preserve the client-sent path for SigV4 verification: a
// signed request is checked against the URI it was signed with, not the
// collapsed path it was routed under.
func TestDoubledV1CollapsePreservesOriginalPath(t *testing.T) {
	router := mux.NewRouter().SkipClean(true)
	var gotPath, gotSignedPath, gotQuery string
	router.HandleFunc("/v1/probe", func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		gotSignedPath = s3_constants.GetOriginalRequestPathFromContext(r)
	}).Methods(http.MethodGet)
	router.Use(collapseDoubledV1Middleware(router))
	router.PathPrefix("/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/v1/probe?warehouse=bucket", nil))
	if gotPath != "/v1/probe" {
		t.Fatalf("routed path: got %q, want /v1/probe", gotPath)
	}
	if gotQuery != "warehouse=bucket" {
		t.Fatalf("query: got %q, want warehouse=bucket", gotQuery)
	}
	if gotSignedPath != "/v1/v1/probe" {
		t.Fatalf("original signed path: got %q, want /v1/v1/probe", gotSignedPath)
	}
}
