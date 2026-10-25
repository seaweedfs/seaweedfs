package s3api

import (
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

func TestNormalizeQuotaUnit(t *testing.T) {
	tests := []struct {
		input string
		want  string
		err   bool
	}{
		{"", "B", false},
		{"B", "B", false},
		{"b", "B", false},
		{"KB", "KB", false},
		{"kb", "KB", false},
		{"MB", "MB", false},
		{"mb", "MB", false},
		{"GB", "GB", false},
		{"gb", "GB", false},
		{"TB", "TB", false},
		{"tb", "TB", false},
		{"PB", "", true},
		{"invalid", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := normalizeQuotaUnit(tc.input)
			if tc.err && err == nil {
				t.Errorf("expected error for %q, got nil", tc.input)
			}
			if !tc.err && err != nil {
				t.Errorf("unexpected error for %q: %v", tc.input, err)
			}
			if got != tc.want {
				t.Errorf("normalizeQuotaUnit(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestConvertQuotaToBytes(t *testing.T) {
	tests := []struct {
		size    int64
		unit    string
		want    int64
		wantErr bool
	}{
		{0, "B", 0, false},
		{0, "GB", 0, false},
		{1024, "B", 1024, false},
		{1, "KB", 1024, false},
		{1, "MB", 1024 * 1024, false},
		{1, "GB", 1024 * 1024 * 1024, false},
		{1, "TB", 1024 * 1024 * 1024 * 1024, false},
		{2, "GB", 2 * 1024 * 1024 * 1024, false},
		{-1, "GB", 0, false},
		// Overflow: 8388608 TB = 2^23 * 2^40 = 2^63 which overflows int64
		{8388608, "TB", 0, true},
		// MaxInt64 KB overflows
		{math.MaxInt64, "KB", 0, true},
		// MaxInt64 B does not overflow
		{math.MaxInt64, "B", math.MaxInt64, false},
	}
	for _, tc := range tests {
		t.Run(tc.unit, func(t *testing.T) {
			got, err := convertQuotaToBytes(tc.size, tc.unit)
			if tc.wantErr && err == nil {
				t.Errorf("expected error for %d %s, got %d", tc.size, tc.unit, got)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error for %d %s: %v", tc.size, tc.unit, err)
			}
			if got != tc.want {
				t.Errorf("convertQuotaToBytes(%d, %q) = %d, want %d", tc.size, tc.unit, got, tc.want)
			}
		})
	}
}

func TestPutBucketQuotaHandler_InvalidBody(t *testing.T) {
	s3a := &S3ApiServer{}
	req := httptest.NewRequest(http.MethodPut, "/test-bucket?seaweedfs-quota", strings.NewReader("not json"))
	req = mux.SetURLVars(req, map[string]string{"bucket": "test-bucket"})
	rr := httptest.NewRecorder()
	s3a.PutBucketQuotaHandler(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for malformed body, got %d", rr.Code)
	}
}

func TestPutBucketQuotaHandler_TrailingData(t *testing.T) {
	s3a := &S3ApiServer{}
	body := `{"quota_size":100,"quota_unit":"GB","quota_enabled":true} garbage`
	req := httptest.NewRequest(http.MethodPut, "/test-bucket?seaweedfs-quota", strings.NewReader(body))
	req = mux.SetURLVars(req, map[string]string{"bucket": "test-bucket"})
	rr := httptest.NewRecorder()
	s3a.PutBucketQuotaHandler(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for trailing data, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestPutBucketQuotaHandler_EnabledWithZeroSize(t *testing.T) {
	s3a := &S3ApiServer{}
	body := `{"quota_size":0,"quota_unit":"GB","quota_enabled":true}`
	req := httptest.NewRequest(http.MethodPut, "/test-bucket?seaweedfs-quota", strings.NewReader(body))
	req = mux.SetURLVars(req, map[string]string{"bucket": "test-bucket"})
	rr := httptest.NewRecorder()
	s3a.PutBucketQuotaHandler(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for enabled quota with size=0, got %d", rr.Code)
	}
}

func TestPutBucketQuotaHandler_InvalidUnit(t *testing.T) {
	s3a := &S3ApiServer{}
	body := `{"quota_size":100,"quota_unit":"PB","quota_enabled":true}`
	req := httptest.NewRequest(http.MethodPut, "/test-bucket?seaweedfs-quota", strings.NewReader(body))
	req = mux.SetURLVars(req, map[string]string{"bucket": "test-bucket"})
	rr := httptest.NewRecorder()
	s3a.PutBucketQuotaHandler(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid unit, got %d", rr.Code)
	}
}

func TestPutBucketQuotaHandler_NoBucket(t *testing.T) {
	s3a := &S3ApiServer{}
	body := `{"quota_size":100,"quota_unit":"GB","quota_enabled":true}`
	req := httptest.NewRequest(http.MethodPut, "/?seaweedfs-quota", strings.NewReader(body))
	req = mux.SetURLVars(req, map[string]string{"bucket": ""})
	rr := httptest.NewRecorder()
	s3a.PutBucketQuotaHandler(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing bucket, got %d", rr.Code)
	}
}

func TestGetBucketQuotaHandler_NoBucket(t *testing.T) {
	s3a := &S3ApiServer{}
	req := httptest.NewRequest(http.MethodGet, "/?seaweedfs-quota", nil)
	req = mux.SetURLVars(req, map[string]string{"bucket": ""})
	rr := httptest.NewRecorder()
	s3a.GetBucketQuotaHandler(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing bucket, got %d", rr.Code)
	}
}
