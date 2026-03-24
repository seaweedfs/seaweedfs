package s3api

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	"github.com/seaweedfs/seaweedfs/weed/util/constants"
)

func TestFilerErrorToS3Error(t *testing.T) {
	tests := []struct {
		name        string
		err         error
		expectedErr s3err.ErrorCode
	}{
		{
			name:        "nil error",
			err:         nil,
			expectedErr: s3err.ErrNone,
		},
		{
			name:        "MD5 mismatch error",
			err:         errors.New(constants.ErrMsgBadDigest),
			expectedErr: s3err.ErrBadDigest,
		},
		{
			name:        "Read only error (direct)",
			err:         weed_server.ErrReadOnly,
			expectedErr: s3err.ErrAccessDenied,
		},
		{
			name:        "Read only error (wrapped)",
			err:         fmt.Errorf("create file /buckets/test/file.txt: %w", weed_server.ErrReadOnly),
			expectedErr: s3err.ErrAccessDenied,
		},
		{
			name:        "Context canceled error",
			err:         errors.New("rpc error: code = Canceled desc = context canceled"),
			expectedErr: s3err.ErrInvalidRequest,
		},
		{
			name:        "Context canceled error (simple)",
			err:         errors.New("context canceled"),
			expectedErr: s3err.ErrInvalidRequest,
		},
		{
			name:        "Directory exists error",
			err:         errors.New("existing /path/to/file is a directory"),
			expectedErr: s3err.ErrExistingObjectIsDirectory,
		},
		{
			name:        "Directory exists error (CreateEntry-wrapped)",
			err:         errors.New("CreateEntry: existing /path/to/file is a directory"),
			expectedErr: s3err.ErrExistingObjectIsDirectory,
		},
		{
			name:        "File exists error",
			err:         errors.New("/path/to/file is a file"),
			expectedErr: s3err.ErrExistingObjectIsFile,
		},
		{
			name:        "Entry name too long error",
			err:         errors.New("CreateEntry: entry name too long"),
			expectedErr: s3err.ErrKeyTooLongError,
		},
		{
			name:        "Entry name too long error (unwrapped)",
			err:         errors.New("entry name too long"),
			expectedErr: s3err.ErrKeyTooLongError,
		},
		{
			name:        "Unknown error",
			err:         errors.New("some random error"),
			expectedErr: s3err.ErrInternalError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filerErrorToS3Error(tt.err)
			if result != tt.expectedErr {
				t.Errorf("filerErrorToS3Error(%v) = %v, want %v", tt.err, result, tt.expectedErr)
			}
		})
	}
}

// setupKeyLengthTestRouter creates a minimal router that maps requests directly
// to the given handler with {bucket} and {object} mux vars, bypassing auth.
func setupKeyLengthTestRouter(handler http.HandlerFunc) *mux.Router {
	router := mux.NewRouter()
	bucket := router.PathPrefix("/{bucket}").Subrouter()
	bucket.Path("/{object:.+}").HandlerFunc(handler)
	return router
}

func TestPutObjectHandler_KeyTooLong(t *testing.T) {
	s3a := &S3ApiServer{}
	router := setupKeyLengthTestRouter(s3a.PutObjectHandler)

	longKey := strings.Repeat("a", s3_constants.MaxS3ObjectKeyLength+1)
	req := httptest.NewRequest(http.MethodPut, "/bucket/"+longKey, nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, rr.Code)
	}
	var errResp s3err.RESTErrorResponse
	if err := xml.Unmarshal(rr.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed to parse error XML: %v", err)
	}
	if errResp.Code != "KeyTooLongError" {
		t.Errorf("expected error code KeyTooLongError, got %s", errResp.Code)
	}
}

func TestPutObjectHandler_KeyAtLimit(t *testing.T) {
	s3a := &S3ApiServer{}

	// Wrap handler to convert panics from uninitialized server state into 500
	// responses. The key length check runs early and writes 400 KeyTooLongError
	// before reaching any code that needs a fully initialized server. A panic
	// means the handler accepted the key and continued past the check.
	panicSafe := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if p := recover(); p != nil {
				w.WriteHeader(http.StatusInternalServerError)
			}
		}()
		s3a.PutObjectHandler(w, r)
	})
	router := setupKeyLengthTestRouter(panicSafe)

	atLimitKey := strings.Repeat("a", s3_constants.MaxS3ObjectKeyLength)
	req := httptest.NewRequest(http.MethodPut, "/bucket/"+atLimitKey, nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	// Must NOT be KeyTooLongError — any other response (including 500 from
	// the minimal server hitting uninitialized state) proves the key passed.
	var errResp s3err.RESTErrorResponse
	if rr.Code == http.StatusBadRequest {
		if err := xml.Unmarshal(rr.Body.Bytes(), &errResp); err == nil && errResp.Code == "KeyTooLongError" {
			t.Errorf("key at exactly %d bytes should not be rejected as too long", s3_constants.MaxS3ObjectKeyLength)
		}
	}
}

func TestCopyObjectHandler_KeyTooLong(t *testing.T) {
	s3a := &S3ApiServer{}
	router := setupKeyLengthTestRouter(s3a.CopyObjectHandler)

	longKey := strings.Repeat("a", s3_constants.MaxS3ObjectKeyLength+1)
	req := httptest.NewRequest(http.MethodPut, "/bucket/"+longKey, nil)
	req.Header.Set("X-Amz-Copy-Source", "/src-bucket/src-object")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, rr.Code)
	}
	var errResp s3err.RESTErrorResponse
	if err := xml.Unmarshal(rr.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed to parse error XML: %v", err)
	}
	if errResp.Code != "KeyTooLongError" {
		t.Errorf("expected error code KeyTooLongError, got %s", errResp.Code)
	}
}

func TestNewMultipartUploadHandler_KeyTooLong(t *testing.T) {
	s3a := &S3ApiServer{}
	router := setupKeyLengthTestRouter(s3a.NewMultipartUploadHandler)

	longKey := strings.Repeat("a", s3_constants.MaxS3ObjectKeyLength+1)
	req := httptest.NewRequest(http.MethodPost, "/bucket/"+longKey+"?uploads", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, rr.Code)
	}
	var errResp s3err.RESTErrorResponse
	if err := xml.Unmarshal(rr.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed to parse error XML: %v", err)
	}
	if errResp.Code != "KeyTooLongError" {
		t.Errorf("expected error code KeyTooLongError, got %s", errResp.Code)
	}
}
