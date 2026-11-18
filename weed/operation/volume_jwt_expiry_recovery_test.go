package operation

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/security"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/stretchr/testify/require"
)

func TestUploadExpiryResponseContract(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status       int
		marker, body string
		want         bool
		calls        int
	}{
		{"marked", 401, "expired-jwt-before-append", `{"error":"wrong jwt"}`, true, 1},
		{"marked_empty_error", 401, "expired-jwt-before-append", `{}`, true, 1},
		{"marked_bad_json", 401, "expired-jwt-before-append", `{`, true, 1},
		{"unmarked", 401, "", `{"error":"wrong jwt"}`, false, 3},
		{"wrong_value", 401, "expired-jwt-before-append-extra", `{"error":"wrong jwt"}`, false, 3},
		{"wrong_status", 403, "expired-jwt-before-append", `{"error":"denied"}`, false, 3},
		{"existing_409", 409, "refused-before-append", `{"error":"full"}`, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			calls := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				mu.Lock()
				calls++
				mu.Unlock()
				w.Header().Set(util_http.WriteOutcomeHeader, tc.marker)
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()
			u := NewUploaderWithHttpClient(srv.Client())
			_, err := u.UploadData(context.Background(), []byte("direct preassigned data"), &UploadOption{UploadUrl: srv.URL, Jwt: "disposable-preassigned"})
			require.Error(t, err)
			require.Equal(t, tc.want, ShouldReassignUpload(err))
			mu.Lock()
			actual := calls
			mu.Unlock()
			require.Equal(t, tc.calls, actual)
		})
	}
}

func TestUploadExpiryBudgetAndAssignmentFailure(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		forever, assignmentFailure bool
	}{{"bounded", false, false}, {"forever", true, false}, {"assignment_failure", false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			calls := 0
			var payloads [][]byte
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mr, err := r.MultipartReader()
				if err != nil {
					w.WriteHeader(400)
					return
				}
				part, err := mr.NextPart()
				if err != nil {
					w.WriteHeader(400)
					return
				}
				data, err := io.ReadAll(part)
				if err != nil {
					w.WriteHeader(400)
					return
				}
				mu.Lock()
				calls++
				payloads = append(payloads, data)
				mu.Unlock()
				w.Header().Set(util_http.WriteOutcomeHeader, util_http.WriteOutcomeExpiredJwtBeforeAppend)
				w.WriteHeader(401)
				_, _ = io.WriteString(w, `{"error":"wrong jwt"}`)
			}))
			defer srv.Close()
			u := NewUploaderWithHttpClient(srv.Client())
			assigns := 0
			sentinel := errors.New("synthetic assignment failure")
			_, _, err := u.uploadWithRetryData(func() (string, string, security.EncodedJwt, bool, error) {
				assigns++
				if tc.assignmentFailure && assigns == 2 {
					return "", "", "", false, sentinel
				}
				return fmt.Sprintf("1,%08x00001234", assigns), strings.TrimPrefix(srv.URL, "http://"), "disposable-per-assignment", false, nil
			}, &UploadOption{Cipher: true, MimeType: "application/octet-stream", RetryForever: tc.forever}, bytes.Repeat([]byte("compressible data"), 1024))
			require.Error(t, err)
			mu.Lock()
			actual := calls
			got := append([][]byte(nil), payloads...)
			mu.Unlock()
			if tc.assignmentFailure {
				require.ErrorIs(t, err, sentinel)
				require.Equal(t, 2, assigns)
				require.Equal(t, 1, actual)
			} else {
				require.Equal(t, 3, assigns)
				require.Equal(t, 3, actual)
			}
			require.NotEmpty(t, got)
			for _, data := range got {
				require.Equal(t, got[0], data)
			}
		})
	}
}

// Generic401 still uses the old RetryForever behavior. Only the explicitly
// marked expiry path gains the separate finite recovery budget.
func TestUploadExpiryPreservesRetryForeverGenericDenial(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n <= 3 {
			w.WriteHeader(401)
			_, _ = io.WriteString(w, `{"error":"wrong jwt"}`)
			return
		}
		_, _ = io.WriteString(w, `{"size":5}`)
	}))
	defer srv.Close()
	u := NewUploaderWithHttpClient(srv.Client())
	assigns := 0
	_, result, err := u.uploadWithRetryData(func() (string, string, security.EncodedJwt, bool, error) {
		assigns++
		return "1,0000000100001234", strings.TrimPrefix(srv.URL, "http://"), "disposable", false, nil
	}, &UploadOption{RetryForever: true}, []byte("plain"))
	require.NoError(t, err)
	require.Equal(t, uint32(5), result.Size)
	require.Equal(t, 2, assigns)
	mu.Lock()
	actual := calls
	mu.Unlock()
	require.Equal(t, 4, actual)
}

func TestUploadExpiryCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		calls++
		mu.Unlock()
		cancel()
		w.Header().Set(util_http.WriteOutcomeHeader, util_http.WriteOutcomeExpiredJwtBeforeAppend)
		w.WriteHeader(401)
		_, _ = io.WriteString(w, `{"error":"wrong jwt"}`)
	}))
	defer srv.Close()
	client := srv.Client()
	client.Timeout = 2 * time.Second
	_, err := NewUploaderWithHttpClient(client).UploadData(ctx, []byte("plain"), &UploadOption{UploadUrl: srv.URL})
	require.ErrorIs(t, err, context.Canceled)
	mu.Lock()
	actual := calls
	mu.Unlock()
	require.Equal(t, 1, actual)
}

func TestUploadExpiryTruncatedResponsePreservesMarker(t *testing.T) {
	for _, marked := range []bool{false, true} {
		name := "unmarked"
		if marked {
			name = "marked"
		}
		t.Run(name, func(t *testing.T) {
			var mu sync.Mutex
			calls := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				mu.Lock()
				calls++
				mu.Unlock()
				if marked {
					w.Header().Set(util_http.WriteOutcomeHeader, util_http.WriteOutcomeExpiredJwtBeforeAppend)
				}
				w.Header().Set("Content-Length", "100")
				w.WriteHeader(401)
				_, _ = io.WriteString(w, `{"error":"wrong jwt"}`)
			}))
			defer srv.Close()
			_, err := NewUploaderWithHttpClient(srv.Client()).UploadData(context.Background(), []byte("plain"), &UploadOption{UploadUrl: srv.URL})
			require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			var statusErr *uploadStatusError
			require.ErrorAs(t, err, &statusErr)
			require.Equal(t, 401, statusErr.StatusCode)
			require.Equal(t, marked, statusErr.expiredBeforeAppend)
			require.Equal(t, marked, ShouldReassignUpload(err))
			want := 3
			if marked {
				want = 1
			}
			mu.Lock()
			actual := calls
			mu.Unlock()
			require.Equal(t, want, actual)
		})
	}
}
