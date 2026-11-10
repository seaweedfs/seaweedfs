package s3vectors

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"golang.org/x/sync/semaphore"
)

func (s *Server) SetLimiter(limit func(http.HandlerFunc, string) http.HandlerFunc) {
	s.limit = limit
}

func (s *Server) SetRequestLimits(maxRequests, maxBytes int64) {
	if maxRequests <= 0 {
		maxRequests = 16
	}
	if maxBytes <= 0 {
		maxBytes = 8 * maxRequestBodyBytes
	}
	s.requestSlots = semaphore.NewWeighted(maxRequests)
	s.requestBytes = semaphore.NewWeighted(maxBytes)
	s.requestByteLimit = maxBytes
}

func (s *Server) limitRequest(handler http.HandlerFunc, w http.ResponseWriter, r *http.Request) {

	op := operationFromRequest(r)
	var target interface{} = &struct{}{}
	resolve := func() (vectorBucketARNs, error) { return vectorBucketARNs{}, nil }
	switch op {
	case "TagResource", "UntagResource", "ListTagsForResource":
		req := &struct {
			ResourceArn string `json:"resourceArn"`
		}{}
		target = req
		resolve = func() (vectorBucketARNs, error) { arns, _, err := s.tagTarget(r, req.ResourceArn); return arns, err }
	case "CreateVectorBucket":
		req := &struct {
			VectorBucketName string `json:"vectorBucketName"`
		}{}
		target = req
		resolve = func() (vectorBucketARNs, error) { return vectorBucketARNs{bucket: req.VectorBucketName}, nil }
	case "GetVectorBucket", "DeleteVectorBucket", "PutVectorBucketPolicy", "GetVectorBucketPolicy", "DeleteVectorBucketPolicy", "ListIndexes":
		req := &GetVectorBucketRequest{}
		target = req
		resolve = func() (vectorBucketARNs, error) { return s.bucketARNs(req.VectorBucketName, req.VectorBucketArn) }
	case "CreateIndex", "GetIndex", "DeleteIndex", "PutVectors", "GetVectors", "DeleteVectors", "ListVectors", "QueryVectors":
		req := &GetIndexRequest{}
		target = req
		resolve = func() (vectorBucketARNs, error) {
			return s.resolveIndexARNs(req.VectorBucketName, req.VectorBucketArn, req.IndexName, req.IndexArn)
		}
	}
	if r.Method == http.MethodPost {
		if err := readJSON(r, target); err != nil {
			writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
			return
		}
	}
	if info, ok := r.Context().Value(requestInfoKey{}).(*requestInfo); ok && r.Method == http.MethodPost {
		r = r.Clone(r.Context())
		r.ContentLength = int64(len(info.body))
		r.Body = io.NopCloser(bytes.NewReader(info.body))
	}
	arns, err := resolve()
	if err != nil {
		writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		return
	}

	setRequestTarget(r, arns.bucket, arns.index)
	vars := make(map[string]string, len(mux.Vars(r))+2)
	for k, v := range mux.Vars(r) {
		vars[k] = v
	}
	vars["bucket"], vars["object"] = arns.bucket, arns.index
	r = mux.SetURLVars(r, vars)
	action := s3_constants.ACTION_WRITE
	if strings.HasPrefix(op, "Get") || strings.HasPrefix(op, "List") || op == "QueryVectors" {
		action = s3_constants.ACTION_READ
	}
	response := &limitResponseWriter{ResponseWriter: w, denied: httptest.NewRecorder()}
	s.limit(func(w http.ResponseWriter, r *http.Request) {
		response.allowed = true
		handler(w, r)
	}, action)(response, r)
	if !response.allowed {
		w.Header().Del("Content-Length")
		var failure struct{ Code, Message string }
		if xml.Unmarshal(response.denied.Body.Bytes(), &failure) != nil || failure.Code == "" {
			failure.Code, failure.Message = "ThrottlingException", "request limit exceeded"
		}
		writeError(w, response.denied.Code, failure.Code, failure.Message)
	}
}

type limitResponseWriter struct {
	http.ResponseWriter
	denied  *httptest.ResponseRecorder
	allowed bool
}

func (w *limitResponseWriter) WriteHeader(status int) {
	if w.allowed {
		w.ResponseWriter.WriteHeader(status)
	} else {
		w.denied.WriteHeader(status)
	}
}

func (w *limitResponseWriter) Write(body []byte) (int, error) {
	if w.allowed {
		return w.ResponseWriter.Write(body)
	}
	return w.denied.Write(body)
}

func (w *limitResponseWriter) Flush() {
	if w.allowed {
		if f, ok := w.ResponseWriter.(http.Flusher); ok {
			f.Flush()
		}
	}
}
