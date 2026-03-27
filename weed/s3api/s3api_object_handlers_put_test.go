package s3api

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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
			name:        "Directory exists error (sentinel)",
			err:         fmt.Errorf("CreateEntry /path: %w", filer_pb.ErrExistingIsDirectory),
			expectedErr: s3err.ErrExistingObjectIsDirectory,
		},
		{
			name:        "Parent is file error (sentinel)",
			err:         fmt.Errorf("CreateEntry /path: %w", filer_pb.ErrParentIsFile),
			expectedErr: s3err.ErrExistingObjectIsFile,
		},
		{
			name:        "Existing is file error (sentinel)",
			err:         fmt.Errorf("CreateEntry /path: %w", filer_pb.ErrExistingIsFile),
			expectedErr: s3err.ErrExistingObjectIsFile,
		},
		{
			name:        "Entry name too long (sentinel)",
			err:         fmt.Errorf("CreateEntry: %w", filer_pb.ErrEntryNameTooLong),
			expectedErr: s3err.ErrKeyTooLongError,
		},
		{
			name:        "Entry name too long (bare sentinel)",
			err:         filer_pb.ErrEntryNameTooLong,
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

type testObjectWriteLockFactory struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func (f *testObjectWriteLockFactory) newLock(bucket, object string) objectWriteLock {
	key := bucket + "|" + object

	f.mu.Lock()
	lock, ok := f.locks[key]
	if !ok {
		lock = &sync.Mutex{}
		f.locks[key] = lock
	}
	f.mu.Unlock()

	lock.Lock()
	return &testObjectWriteLock{unlock: lock.Unlock}
}

type testObjectWriteLock struct {
	once   sync.Once
	unlock func()
}

func (l *testObjectWriteLock) StopShortLivedLock() error {
	l.once.Do(l.unlock)
	return nil
}

func TestWithObjectWriteLockSerializesConcurrentPreconditions(t *testing.T) {
	s3a := NewS3ApiServerForTest()
	lockFactory := &testObjectWriteLockFactory{
		locks: make(map[string]*sync.Mutex),
	}
	s3a.newObjectWriteLock = lockFactory.newLock

	const workers = 3
	const bucket = "test-bucket"
	const object = "/file.txt"

	start := make(chan struct{})
	results := make(chan s3err.ErrorCode, workers)
	var wg sync.WaitGroup

	var stateMu sync.Mutex
	objectExists := false

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			errCode := s3a.withObjectWriteLock(bucket, object,
				func() s3err.ErrorCode {
					stateMu.Lock()
					defer stateMu.Unlock()
					if objectExists {
						return s3err.ErrPreconditionFailed
					}
					return s3err.ErrNone
				},
				func() s3err.ErrorCode {
					stateMu.Lock()
					defer stateMu.Unlock()
					objectExists = true
					return s3err.ErrNone
				},
			)

			results <- errCode
		}()
	}

	close(start)
	wg.Wait()
	close(results)

	var successCount int
	var preconditionFailedCount int

	for errCode := range results {
		switch errCode {
		case s3err.ErrNone:
			successCount++
		case s3err.ErrPreconditionFailed:
			preconditionFailedCount++
		default:
			t.Fatalf("unexpected error code: %v", errCode)
		}
	}

	if successCount != 1 {
		t.Fatalf("expected exactly one successful writer, got %d", successCount)
	}
	if preconditionFailedCount != workers-1 {
		t.Fatalf("expected %d precondition failures, got %d", workers-1, preconditionFailedCount)
	}
}
