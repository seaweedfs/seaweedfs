package iceberg

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
)

func TestValidateRequestPath_RejectsTraversal(t *testing.T) {
	tests := []struct {
		name     string
		rawPath  string
		wantCode int
	}{
		{"clean namespace+table passes", "/v1/namespaces/sales/tables/orders", http.StatusOK},
		{"clean prefixed passes", "/v1/wh/namespaces/sales/tables/orders", http.StatusOK},
		{"clean namespace only passes", "/v1/namespaces/sales", http.StatusOK},

		// SkipClean(true) means raw `..` survives routing — these are the
		// realistic traversal shapes the middleware must catch.
		{"dotdot as prefix var rejected", "/v1/../namespaces/sales", http.StatusBadRequest},
		{"dotdot as namespace var rejected", "/v1/namespaces/..", http.StatusBadRequest},
		{"dotdot as namespace var prefixed rejected", "/v1/wh/namespaces/..", http.StatusBadRequest},
		{"dotdot as table var rejected", "/v1/namespaces/sales/tables/..", http.StatusBadRequest},
		{"dot as table var rejected", "/v1/namespaces/sales/tables/.", http.StatusBadRequest},
		// Iceberg clients send the 0x1F unit separator percent-encoded; mux
		// decodes it before the middleware sees the namespace var.
		{"unit-sep namespace with dotdot part rejected", "/v1/namespaces/sales%1F..%1Fevil", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := mux.NewRouter().SkipClean(true)
			router.Use(validateRequestPath)
			handlerCalled := false
			pass := func(w http.ResponseWriter, r *http.Request) {
				handlerCalled = true
				w.WriteHeader(http.StatusOK)
			}
			router.HandleFunc("/v1/namespaces/{namespace}", pass)
			router.HandleFunc("/v1/namespaces/{namespace}/tables/{table}", pass)
			router.HandleFunc("/v1/{prefix}/namespaces/{namespace}", pass)
			router.HandleFunc("/v1/{prefix}/namespaces/{namespace}/tables/{table}", pass)

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

func TestIsValidNameSegment(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"empty ok", "", true},
		{"plain", "orders", true},
		{"with dot inside", "my.table", true},
		{"hidden", ".hidden", true},

		{"bare dot", ".", false},
		{"bare dotdot", "..", false},
		{"contains slash", "foo/bar", false},
		{"contains backslash", "foo\\bar", false},
		{"contains nul", "foo\x00bar", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isValidNameSegment(tt.input); got != tt.want {
				t.Errorf("isValidNameSegment(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
