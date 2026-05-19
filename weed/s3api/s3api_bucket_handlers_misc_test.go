package s3api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

func newMiscTestServer(t *testing.T, bucket string) *S3ApiServer {
	t.Helper()
	s3a := &S3ApiServer{
		iam:               &IdentityAccessManagement{isAuthEnabled: true},
		bucketConfigCache: NewBucketConfigCache(time.Minute),
	}
	s3a.bucketConfigCache.Set(bucket, &BucketConfig{
		Name:  bucket,
		Entry: &filer_pb.Entry{Name: bucket},
	})
	return s3a
}

func newBucketRequest(method, bucket, query, body string) *http.Request {
	req := httptest.NewRequest(method, "/"+bucket+"?"+query, strings.NewReader(body))
	req = mux.SetURLVars(req, map[string]string{"bucket": bucket})
	return req
}

func TestGetBucketPolicyStatusIsPublic(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want bool
	}{
		{
			name: "public allow star",
			raw:  `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}`,
			want: true,
		},
		{
			name: "deny is not public",
			raw:  `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}`,
			want: false,
		},
		{
			name: "condition makes it non-public",
			raw:  `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*","Condition":{"IpAddress":{"aws:SourceIp":"10.0.0.0/8"}}}]}`,
			want: false,
		},
		{
			name: "specific principal is not public",
			raw:  `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"arn:aws:iam::1:user/a","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}`,
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var doc policy_engine.PolicyDocument
			if err := json.Unmarshal([]byte(tc.raw), &doc); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if got := isPolicyPublic(&doc); got != tc.want {
				t.Fatalf("isPolicyPublic = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPutBucketRequestPaymentBucketOwner(t *testing.T) {
	s3a := newMiscTestServer(t, "b")
	body := `<RequestPaymentConfiguration><Payer>BucketOwner</Payer></RequestPaymentConfiguration>`
	req := newBucketRequest(http.MethodPut, "b", "requestPayment=", body)
	rec := httptest.NewRecorder()

	s3a.PutBucketRequestPaymentHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestPutBucketRequestPaymentRequesterRejected(t *testing.T) {
	s3a := newMiscTestServer(t, "b")
	body := `<RequestPaymentConfiguration><Payer>Requester</Payer></RequestPaymentConfiguration>`
	req := newBucketRequest(http.MethodPut, "b", "requestPayment=", body)
	rec := httptest.NewRecorder()

	s3a.PutBucketRequestPaymentHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d, body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "MalformedXML") {
		t.Fatalf("body missing MalformedXML: %s", rec.Body.String())
	}
}

func TestGetBucketAccelerateConfiguration(t *testing.T) {
	s3a := &S3ApiServer{}
	req := newBucketRequest(http.MethodGet, "b", "accelerate=", "")
	rec := httptest.NewRecorder()

	s3a.GetBucketAccelerateConfigurationHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	body, _ := io.ReadAll(rec.Body)
	got := string(body)
	if !strings.Contains(got, "<AccelerateConfiguration") {
		t.Fatalf("missing root element: %s", got)
	}
	if !strings.Contains(got, "<Status>Suspended</Status>") {
		t.Fatalf("missing Suspended status: %s", got)
	}
	if !strings.Contains(got, `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`) {
		t.Fatalf("missing xmlns: %s", got)
	}
}

func TestGetBucketLogging(t *testing.T) {
	s3a := &S3ApiServer{}
	req := newBucketRequest(http.MethodGet, "b", "logging=", "")
	rec := httptest.NewRecorder()

	s3a.GetBucketLoggingHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	got := rec.Body.String()
	if !strings.Contains(got, "<BucketLoggingStatus") {
		t.Fatalf("missing root element: %s", got)
	}
	if strings.Contains(got, "<LoggingEnabled") {
		t.Fatalf("unexpected LoggingEnabled element: %s", got)
	}
	if !strings.Contains(got, `xmlns="http://s3.amazonaws.com/doc/2006-03-01/"`) {
		t.Fatalf("missing xmlns: %s", got)
	}
}
