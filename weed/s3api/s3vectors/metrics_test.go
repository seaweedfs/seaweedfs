package s3vectors

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

func TestVectorMetricsAttributeAuthorizedBuckets(t *testing.T) {
	fs := s3tablestest.Start(t)
	s := NewServer(s3tables.NewManagerClient(fs.Client), headerAuth{}, newFakeVectors(0))
	var limitCalls int
	s.SetLimiter(func(next http.HandlerFunc, action string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) { limitCalls++; next(w, r) }
	})
	router := mux.NewRouter()
	s.RegisterRoutes(router)
	request := func(identity, operation string, payload interface{}) *httptest.ResponseRecorder {
		t.Helper()
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest(http.MethodPost, "/"+operation, bytes.NewReader(body))
		r.Header.Set("X-Test-Identity", identity)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		return w
	}
	const bucket = "metrics-vector-bucket"
	if w := request("", "CreateVectorBucket", CreateVectorBucketRequest{VectorBucketName: bucket}); w.Code != http.StatusOK {
		t.Fatalf("create: %d %s", w.Code, w.Body)
	}
	count := stats.S3RequestCounter.WithLabelValues("s3vectors.GetVectorBucket", "200", bucket)
	before := testutil.ToFloat64(count)
	if w := request("", "GetVectorBucket", GetVectorBucketRequest{VectorBucketArn: s.vectorBucketArn(bucket)}); w.Code != http.StatusOK {
		t.Fatalf("get by ARN: %d %s", w.Code, w.Body)
	}
	if got := testutil.ToFloat64(count) - before; got != 1 {
		t.Fatalf("bucket requests = %v, want 1", got)
	}
	denied := stats.S3RequestCounter.WithLabelValues("s3vectors.GetVectorBucket", "403", "")
	before = testutil.ToFloat64(denied)
	if w := request("stranger", "GetVectorBucket", GetVectorBucketRequest{VectorBucketName: bucket}); w.Code != http.StatusForbidden {
		t.Fatalf("unauthorized get: %d %s", w.Code, w.Body)
	}
	if got := testutil.ToFloat64(denied) - before; got != 1 {
		t.Fatalf("unattributed denied requests = %v, want 1", got)
	}
	if got := testutil.ToFloat64(stats.S3BucketTrafficReceivedBytesCounter.WithLabelValues(bucket)); got <= 0 {
		t.Fatalf("received bytes = %v", got)
	}
	if got := testutil.ToFloat64(stats.S3BucketTrafficSentBytesCounter.WithLabelValues(bucket)); got <= 0 {
		t.Fatalf("sent bytes = %v", got)
	}
	if got := testutil.ToFloat64(stats.S3InFlightRequestsGauge.WithLabelValues("s3vectors.GetVectorBucket")); got != 0 {
		t.Fatalf("in-flight requests = %v after responses", got)
	}
	r := httptest.NewRequest(http.MethodGet, tagsPathPrefix+s.vectorBucketArn(bucket), nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	if w.Code != http.StatusOK || testutil.ToFloat64(stats.S3RequestCounter.WithLabelValues("s3vectors.ListTagsForResource", "200", bucket)) == 0 {
		t.Fatalf("tag metrics: %d %s", w.Code, w.Body)
	}
	if limitCalls != 4 {
		t.Fatalf("limiter calls = %d, want 4", limitCalls)
	}
}
