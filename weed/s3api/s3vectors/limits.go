package s3vectors

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func (s *Server) SetLimiter(limit func(http.HandlerFunc, string) http.HandlerFunc) {
	s.limit = limit
}

func (s *Server) limitRequest(handler http.HandlerFunc, w http.ResponseWriter, r *http.Request) {
	var target struct {
		VectorBucketName string `json:"vectorBucketName"`
		VectorBucketArn  string `json:"vectorBucketArn"`
		IndexName        string `json:"indexName"`
		IndexArn         string `json:"indexArn"`
		ResourceArn      string `json:"resourceArn"`
	}
	if r.Method == http.MethodPost {
		if err := readJSON(r, &target); err != nil {
			writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
			return
		}
	}
	var arns vectorBucketARNs
	var err error
	if strings.HasPrefix(r.URL.Path, tagsPathPrefix) || target.ResourceArn != "" {
		arns, _, err = s.tagTarget(r, target.ResourceArn)
	} else if target.IndexArn != "" || target.IndexName != "" {
		arns, err = s.resolveIndexARNs(target.VectorBucketName, target.VectorBucketArn, target.IndexName, target.IndexArn)
	} else {
		arns, err = s.bucketARNs(target.VectorBucketName, target.VectorBucketArn)
	}
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
	op := operationFromRequest(r)
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
