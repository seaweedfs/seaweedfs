package iceberg

import (
	"net/http/httptest"
	"testing"
)

func TestGetBucketFromPrefix_WarehouseQueryFallback(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{
			name: "warehouse query routes to its bucket when no prefix in path",
			url:  "/v1/namespaces?warehouse=s3%3A%2F%2Fmyblkt%2F",
			want: "myblkt",
		},
		{
			name: "warehouse query with sub-path still picks the bucket",
			url:  "/v1/namespaces?warehouse=s3%3A%2F%2Fanother%2Fextra",
			want: "another",
		},
		{
			name: "malformed warehouse value falls through to default",
			url:  "/v1/namespaces?warehouse=not-a-url",
			want: "warehouse",
		},
		{
			name: "no warehouse and no prefix returns default",
			url:  "/v1/namespaces",
			want: "warehouse",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", tc.url, nil)
			got := getBucketFromPrefix(r)
			if got != tc.want {
				t.Fatalf("getBucketFromPrefix(%q) = %q, want %q", tc.url, got, tc.want)
			}
		})
	}
}

func TestBuildFileIOConfig(t *testing.T) {
	t.Run("no endpoint configured yields empty config", func(t *testing.T) {
		s := &Server{}
		got := s.buildFileIOConfig()
		if len(got) != 0 {
			t.Fatalf("buildFileIOConfig() = %v, want empty", got)
		}
	})

	t.Run("endpoint is advertised with path-style-access", func(t *testing.T) {
		s := &Server{s3Endpoint: "http://seaweed.example:8333"}
		got := s.buildFileIOConfig()
		if got["s3.endpoint"] != "http://seaweed.example:8333" {
			t.Fatalf("s3.endpoint = %q, want %q", got["s3.endpoint"], "http://seaweed.example:8333")
		}
		if got["s3.path-style-access"] != "true" {
			t.Fatalf("s3.path-style-access = %q, want %q", got["s3.path-style-access"], "true")
		}
	})
}
