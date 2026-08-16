package iceberg

import (
	"net/http"
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
			name: "bare bucket name is taken as the table bucket",
			url:  "/v1/namespaces?warehouse=not-a-url",
			want: "not-a-url",
		},
		{
			name: "table bucket ARN routes to its bucket",
			url:  "/v1/namespaces?warehouse=arn%3Aaws%3As3tables%3Aus-east-1%3Aadmin%3Abucket%2Fseaweed-iceberg",
			want: "seaweed-iceberg",
		},
		{
			name: "unusable warehouse value falls through to default",
			url:  "/v1/namespaces?warehouse=file%3A%2F%2F%2Ftmp%2Fwh",
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
	loadTable := func() *http.Request {
		return httptest.NewRequest(http.MethodGet, "/v1/namespaces/ns/tables/t", nil)
	}

	t.Run("no endpoint configured yields empty config", func(t *testing.T) {
		s := &Server{}
		got := s.buildFileIOConfig(loadTable(), "s3://warehouse/ns/t")
		if len(got) != 0 {
			t.Fatalf("buildFileIOConfig() = %v, want empty", got)
		}
	})

	t.Run("endpoint is advertised with path-style-access and region", func(t *testing.T) {
		s := &Server{s3Endpoint: "http://seaweed.example:8333"}
		got := s.buildFileIOConfig(loadTable(), "s3://warehouse/ns/t")
		if got["s3.endpoint"] != "http://seaweed.example:8333" {
			t.Fatalf("s3.endpoint = %q, want %q", got["s3.endpoint"], "http://seaweed.example:8333")
		}
		if got["s3.path-style-access"] != "true" {
			t.Fatalf("s3.path-style-access = %q, want %q", got["s3.path-style-access"], "true")
		}
		if got["s3.region"] == "" {
			t.Fatalf("s3.region was empty, want a non-empty default so clients like DuckDB do not require AWS_REGION")
		}
	})
}

func TestGetBucketFromPrefix_TableBucketEnvFallback(t *testing.T) {
	r := httptest.NewRequest("GET", "/v1/namespaces", nil)

	t.Setenv("S3_TABLE_BUCKET", " ,analytics, other")
	if got := getBucketFromPrefix(r); got != "analytics" {
		t.Fatalf("first S3_TABLE_BUCKET entry: got %q, want analytics", got)
	}

	// The explicit default still wins over the table bucket list.
	t.Setenv("S3TABLES_DEFAULT_BUCKET", "explicit")
	if got := getBucketFromPrefix(r); got != "explicit" {
		t.Fatalf("S3TABLES_DEFAULT_BUCKET override: got %q, want explicit", got)
	}

	// A prefixless value with neither env set falls through to the default.
	t.Setenv("S3TABLES_DEFAULT_BUCKET", "")
	t.Setenv("S3_TABLE_BUCKET", " , ")
	if got := getBucketFromPrefix(r); got != "warehouse" {
		t.Fatalf("empty specs: got %q, want warehouse", got)
	}
}
