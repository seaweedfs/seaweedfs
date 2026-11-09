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
