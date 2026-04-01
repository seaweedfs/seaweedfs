package s3api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/gorilla/mux"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

const testTableBucketARN = "arn:aws:s3tables:us-east-1:123456789012:bucket/test-bucket"

func TestBuildListTablesRequestRejectsInvalidNamespaceQuery(t *testing.T) {
	testCases := []struct {
		name      string
		namespace string
	}{
		{name: "uppercase", namespace: "InvalidNamespace"},
		{name: "hyphen", namespace: "invalid-ns"},
		{name: "slash", namespace: "a/b"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/tables?namespace="+url.QueryEscape(tc.namespace), nil)
			req = mux.SetURLVars(req, map[string]string{"tableBucketARN": testTableBucketARN})

			_, err := buildListTablesRequest(req)
			if err == nil {
				t.Fatalf("expected invalid namespace query to return an error")
			}
			if !strings.Contains(err.Error(), "invalid namespace") {
				t.Fatalf("expected invalid namespace error, got %q", err.Error())
			}
		})
	}
}

func TestBuildGetTableRequestRejectsInvalidNamespaceQuery(t *testing.T) {
	testCases := []struct {
		name      string
		namespace string
	}{
		{name: "uppercase", namespace: "InvalidNamespace"},
		{name: "hyphen", namespace: "invalid-ns"},
		{name: "slash", namespace: "a/b"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/get-table?tableBucketARN="+url.QueryEscape(testTableBucketARN)+"&namespace="+url.QueryEscape(tc.namespace)+"&name=table1", nil)

			_, err := buildGetTableRequest(req)
			if err == nil {
				t.Fatalf("expected invalid namespace query to return an error")
			}
			if !strings.Contains(err.Error(), "invalid namespace") {
				t.Fatalf("expected invalid namespace error, got %q", err.Error())
			}
		})
	}
}

func TestHandleRestOperationReturnsBadRequestForInvalidNamespaceQuery(t *testing.T) {
	testCases := []struct {
		name      string
		namespace string
	}{
		{name: "uppercase", namespace: "InvalidNamespace"},
		{name: "hyphen", namespace: "invalid-ns"},
		{name: "slash", namespace: "a/b"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/tables?namespace="+url.QueryEscape(tc.namespace), nil)
			req = mux.SetURLVars(req, map[string]string{"tableBucketARN": testTableBucketARN})
			rr := httptest.NewRecorder()

			st := &S3TablesApiServer{}
			st.handleRestOperation("ListTables", buildListTablesRequest).ServeHTTP(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rr.Code)
			}

			var body map[string]string
			if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
				t.Fatalf("failed to decode error response: %v", err)
			}
			if got, want := body["__type"], s3tables.ErrCodeInvalidRequest; got != want {
				t.Fatalf("expected __type=%q, got %q", want, got)
			}
			if !strings.Contains(body["message"], "invalid namespace") {
				t.Fatalf("expected invalid namespace error message, got %q", body["message"])
			}
		})
	}
}
