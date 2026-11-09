package s3vectors

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

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
