package s3vectors

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func TestVectorLimitsResolveResourcesAndPreserveBodies(t *testing.T) {
	s := NewServer(nil, headerAuth{}, nil)
	s.SetRegion("r")
	s.SetAccountID("a")
	for _, tc := range []struct {
		op, action string
		body       interface{}
	}{
		{"PutVectors", s3_constants.ACTION_WRITE, PutVectorsRequest{VectorBucketName: "embeddings", IndexName: "documents", Vectors: []*PutInputVector{{Key: "a", Data: &VectorData{Float32: []float32{1, 2}}}}}},
		{"QueryVectors", s3_constants.ACTION_READ, QueryVectorsRequest{IndexArn: s.indexArn("embeddings", "documents"), TopK: 1}},
		{"GetVectorBucket", s3_constants.ACTION_READ, GetVectorBucketRequest{VectorBucketArn: s.vectorBucketArn("embeddings")}},
	} {
		t.Run(tc.op, func(t *testing.T) {
			payload, err := json.Marshal(tc.body)
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest(http.MethodPost, "/"+tc.op, bytes.NewReader(payload))
			var called bool
			s.SetLimiter(func(next http.HandlerFunc, action string) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					if mux.Vars(r)["bucket"] != "embeddings" || action != tc.action {
						t.Fatalf("limit target %v, action %s", mux.Vars(r), action)
					}
					next(w, r)
				}
			})
			s.instrument(func(w http.ResponseWriter, r *http.Request) {
				s.limitRequest(func(w http.ResponseWriter, r *http.Request) {
					called = true
					var got json.RawMessage
					if err := readJSON(r, &got); err != nil || !bytes.Equal(got, payload) {
						t.Fatalf("handler body %s: %v", got, err)
					}
				}, w, r)
			})(httptest.NewRecorder(), r)
			if !called {
				t.Fatal("limited handler was not called")
			}
		})
	}
}

func TestVectorLimitsReturnJSONWithoutCallingHandler(t *testing.T) {
	s := NewServer(nil, headerAuth{}, nil)
	s.SetLimiter(func(next http.HandlerFunc, action string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			s3err.WriteErrorResponse(w, r, s3err.ErrTooManyRequest)
		}
	})
	r := httptest.NewRequest(http.MethodPost, "/PutVectors", bytes.NewBufferString(`{"vectorBucketName":"embeddings","indexName":"documents"}`))
	w := httptest.NewRecorder()
	s.instrument(func(w http.ResponseWriter, r *http.Request) {
		s.limitRequest(func(http.ResponseWriter, *http.Request) { t.Fatal("rejected handler ran") }, w, r)
	})(w, r)
	var failure Error
	if json.Unmarshal(w.Body.Bytes(), &failure) != nil || failure.Code == "" || w.Code < http.StatusBadRequest || w.Header().Get("Content-Type") != "application/x-amz-json-1.1" {
		t.Fatalf("limit response: %d %v %s", w.Code, w.Header(), w.Body)
	}
}

func TestVectorLimitsIgnoreUnrelatedResourceFields(t *testing.T) {
	s := NewServer(nil, headerAuth{}, nil)
	for _, tc := range []struct{ op, body, bucket string }{
		{"PutVectors", `{"vectorBucketName":"bucket-a","indexName":"idx","resourceArn":"arn:aws:s3vectors:r:a:bucket/bucket-b"}`, "bucket-a"},
		{"PutVectors", `{"vectorBucketName":"bucket-a","indexName":"idx","resourceArn":{"ignored":true}}`, "bucket-a"},
		{"GetVectorBucket", `{"vectorBucketName":"bucket-a","indexArn":"invalid","indexName":42}`, "bucket-a"},
		{"CreateIndex", `{"vectorBucketName":"bucket-a","indexName":"idx","resourceArn":"ignored"}`, "bucket-a"},
		{"ListVectorBuckets", `{"vectorBucketName":"ignored","resourceArn":"ignored"}`, ""},
	} {
		t.Run(tc.op+tc.body, func(t *testing.T) {
			called := false
			s.SetLimiter(func(next http.HandlerFunc, action string) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					called = true
					if got := mux.Vars(r)["bucket"]; got != tc.bucket {
						t.Fatalf("limited bucket %q, want %q", got, tc.bucket)
					}
					next(w, r)
				}
			})
			r := httptest.NewRequest(http.MethodPost, "/"+tc.op, bytes.NewBufferString(tc.body))
			s.instrument(func(w http.ResponseWriter, r *http.Request) {
				s.limitRequest(func(http.ResponseWriter, *http.Request) {}, w, r)
			})(httptest.NewRecorder(), r)
			if !called {
				t.Fatal("limiter was not invoked")
			}
		})
	}
}

func TestVectorLimitsCountChunkedBodyBytes(t *testing.T) {
	s := NewServer(nil, headerAuth{}, nil)
	body := `{"vectorBucketName":"embeddings","indexName":"documents"}`
	var got int64
	s.SetLimiter(func(next http.HandlerFunc, action string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) { got = r.ContentLength; next(w, r) }
	})
	r := httptest.NewRequest(http.MethodPost, "/PutVectors", bytes.NewBufferString(body))
	r.ContentLength = -1
	s.instrument(func(w http.ResponseWriter, r *http.Request) {
		s.limitRequest(func(http.ResponseWriter, *http.Request) {}, w, r)
	})(httptest.NewRecorder(), r)
	if got != int64(len(body)) {
		t.Fatalf("limited body length = %d, want %d", got, len(body))
	}
	if r.ContentLength != -1 {
		t.Fatal("mutated original request")
	}
}

type admissionAuth struct {
	started, release chan struct{}
	calls            atomic.Int32
}

func (a *admissionAuth) DefaultAllow() bool { return true }
func (a *admissionAuth) AuthenticateRequest(r *http.Request) (string, interface{}, s3err.ErrorCode) {
	a.calls.Add(1)
	_, _ = io.ReadAll(r.Body)
	if r.Header.Get("Block") != "" {
		close(a.started)
		select {
		case <-a.release:
		case <-r.Context().Done():
		}
	}
	if r.Header.Get("Deny") != "" {
		return "", nil, s3err.ErrSignatureDoesNotMatch
	}
	return "tester", nil, s3err.ErrNone
}

func TestVectorAdmissionBoundsBufferingBeforeAuthentication(t *testing.T) {
	for _, tc := range []struct {
		name            string
		requests, bytes int64
		chunked         bool
	}{
		{"request count", 1, 10, false},
		{"byte count", 2, 2, false},
		{"chunked body", 2, 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			auth := &admissionAuth{started: make(chan struct{}), release: make(chan struct{})}
			server := NewServer(nil, auth, nil)
			server.SetRequestLimits(tc.requests, tc.bytes)
			handler := server.Auth(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
			request := func() *http.Request {
				r := httptest.NewRequest(http.MethodPost, "/PutVectors", strings.NewReader("{}"))
				if tc.chunked {
					r.ContentLength = -1
				}
				return r
			}
			first := request()
			first.Header.Set("Block", "true")
			firstCtx, cancelFirst := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancelFirst()
			done := make(chan struct{})
			go func() { defer close(done); handler(httptest.NewRecorder(), first.WithContext(firstCtx)) }()
			select {
			case <-auth.started:
			case <-firstCtx.Done():
				t.Fatal("first request did not enter authentication")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			waiting := httptest.NewRecorder()
			handler(waiting, request().WithContext(ctx))
			if waiting.Code != http.StatusRequestTimeout || auth.calls.Load() != 1 {
				t.Fatalf("waiting request buffered/authenticated: status %d, calls %d", waiting.Code, auth.calls.Load())
			}
			close(auth.release)
			<-done
			denied := request()
			denied.Header.Set("Deny", "true")
			handler(httptest.NewRecorder(), denied.WithContext(firstCtx))
			after := httptest.NewRecorder()
			handler(after, request().WithContext(firstCtx))
			if after.Code != http.StatusNoContent || auth.calls.Load() != 3 {
				t.Fatalf("capacity leaked after success/failure: status %d, calls %d", after.Code, auth.calls.Load())
			}
		})
	}
}

func TestVectorErrorResponsesEscapeHTML(t *testing.T) {
	payload := "<script>alert(1)</script>"
	for _, limited := range []bool{false, true} {
		server := NewServer(nil, headerAuth{}, nil)
		operation := payload
		body := `{}`
		if limited {
			operation = "QueryVectors"
			body = `{"vectorBucketName":"embeddings","indexName":"documents"}`
			server.SetLimiter(func(http.HandlerFunc, string) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusTooManyRequests)
					_ = xml.NewEncoder(w).Encode(struct {
						XMLName       xml.Name `xml:"Error"`
						Code, Message string
					}{Code: "SlowDown", Message: payload})
				}
			})
		}
		router := mux.NewRouter()
		server.RegisterRoutes(router)
		request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		request.Header.Set("X-Amz-Target", operation)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		var failure Error
		if response.Code < 400 || json.Unmarshal(response.Body.Bytes(), &failure) != nil || !strings.Contains(failure.Message, payload) {
			t.Fatalf("error response lost message: %d %s", response.Code, response.Body)
		}
		if strings.Contains(response.Body.String(), "<") || response.Header().Get("X-Content-Type-Options") != "nosniff" || response.Header().Get("Content-Type") != "application/x-amz-json-1.1" {
			t.Fatalf("unsafe error response: %v %s", response.Header(), response.Body)
		}
	}
}
