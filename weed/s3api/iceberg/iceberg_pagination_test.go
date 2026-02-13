package iceberg

import (
	"net/http/httptest"
	"testing"
)

func TestParsePaginationDefaultValues(t *testing.T) {
	req := httptest.NewRequest("GET", "/v1/namespaces", nil)

	pageToken, pageSize, err := parsePagination(req)
	if err != nil {
		t.Fatalf("parsePagination() error = %v", err)
	}
	if pageToken != "" {
		t.Fatalf("pageToken = %q, want empty", pageToken)
	}
	if pageSize != defaultListPageSize {
		t.Fatalf("pageSize = %d, want %d", pageSize, defaultListPageSize)
	}
}

func TestParsePaginationUsesCamelCaseParameters(t *testing.T) {
	req := httptest.NewRequest("GET", "/v1/namespaces?pageToken=abc&pageSize=25", nil)

	pageToken, pageSize, err := parsePagination(req)
	if err != nil {
		t.Fatalf("parsePagination() error = %v", err)
	}
	if pageToken != "abc" {
		t.Fatalf("pageToken = %q, want %q", pageToken, "abc")
	}
	if pageSize != 25 {
		t.Fatalf("pageSize = %d, want %d", pageSize, 25)
	}
}

func TestParsePaginationSupportsHyphenatedFallback(t *testing.T) {
	req := httptest.NewRequest("GET", "/v1/namespaces?page-token=abc&page-size=17", nil)

	pageToken, pageSize, err := parsePagination(req)
	if err != nil {
		t.Fatalf("parsePagination() error = %v", err)
	}
	if pageToken != "abc" {
		t.Fatalf("pageToken = %q, want %q", pageToken, "abc")
	}
	if pageSize != 17 {
		t.Fatalf("pageSize = %d, want %d", pageSize, 17)
	}
}

func TestParsePaginationRejectsInvalidPageSize(t *testing.T) {
	testCases := []string{
		"/v1/namespaces?pageSize=0",
		"/v1/namespaces?pageSize=-1",
		"/v1/namespaces?pageSize=foo",
		"/v1/namespaces?pageSize=1001",
	}

	for _, rawURL := range testCases {
		t.Run(rawURL, func(t *testing.T) {
			req := httptest.NewRequest("GET", rawURL, nil)
			if _, _, err := parsePagination(req); err == nil {
				t.Fatalf("parsePagination() expected error for url %q", rawURL)
			}
		})
	}
}
