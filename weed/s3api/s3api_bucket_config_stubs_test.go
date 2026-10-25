package s3api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

func TestBucketConfigStubs(t *testing.T) {
	const bucket = "stub-bucket"
	s3a := &S3ApiServer{
		iam:               &IdentityAccessManagement{isAuthEnabled: true},
		bucketConfigCache: NewBucketConfigCache(time.Minute),
	}
	s3a.bucketConfigCache.Set(bucket, &BucketConfig{Name: bucket})

	listCases := []struct {
		name        string
		query       string
		rootElement string
		handler     http.HandlerFunc
	}{
		{"AnalyticsList", "analytics", "ListBucketAnalyticsConfigurationsResult", s3a.ListBucketAnalyticsConfigurations},
		{"InventoryList", "inventory", "ListInventoryConfigurationsResult", s3a.ListBucketInventoryConfigurations},
		{"IntelligentTieringList", "intelligent-tiering", "ListBucketIntelligentTieringConfigurationsResult", s3a.ListBucketIntelligentTieringConfigurations},
		{"MetricsList", "metrics", "ListBucketMetricsConfigurationsResult", s3a.ListBucketMetricsConfigurations},
	}
	for _, tc := range listCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/"+bucket+"?"+tc.query, nil)
			req = mux.SetURLVars(req, map[string]string{"bucket": bucket})
			rr := httptest.NewRecorder()
			tc.handler(rr, req)

			if rr.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
			}
			body := rr.Body.String()
			if !strings.Contains(body, "<"+tc.rootElement) {
				t.Fatalf("expected root element %q in body: %s", tc.rootElement, body)
			}
			if !strings.Contains(body, "<IsTruncated>false</IsTruncated>") {
				t.Fatalf("expected IsTruncated=false in body: %s", body)
			}
		})
	}

	getCases := []struct {
		name    string
		query   string
		handler http.HandlerFunc
	}{
		{"AnalyticsGet", "analytics&id=x", s3a.GetAnalyticsConfiguration},
		{"InventoryGet", "inventory&id=x", s3a.GetInventoryConfiguration},
		{"IntelligentTieringGet", "intelligent-tiering&id=x", s3a.GetIntelligentTieringConfiguration},
		{"MetricsGet", "metrics&id=x", s3a.GetMetricsConfiguration},
	}
	for _, tc := range getCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/"+bucket+"?"+tc.query, nil)
			req = mux.SetURLVars(req, map[string]string{"bucket": bucket})
			rr := httptest.NewRecorder()
			tc.handler(rr, req)

			if rr.Code != http.StatusNotFound {
				t.Fatalf("expected 404, got %d: %s", rr.Code, rr.Body.String())
			}
			if !strings.Contains(rr.Body.String(), "<Code>NoSuchConfiguration</Code>") {
				t.Fatalf("expected NoSuchConfiguration code in body: %s", rr.Body.String())
			}
		})
	}
}
