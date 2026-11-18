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
				n := calls
				payloads = append(payloads, data)
				mu.Unlock()
				// Terminal fallback: if the expiry budget regressed and did not
				// stop at 3, end the run with an unmarked 409 so this test fails
				// here instead of looping to the package timeout under RetryForever.
				if n > 3 {
					w.WriteHeader(409)
					_, _ = io.WriteString(w, `{"error":"full"}`)
					return
				}
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
				// The run must end on the 3rd marked 401 (the budget), not the
				// 409 terminal fallback above.
				var finalErr *uploadStatusError
				require.ErrorAs(t, err, &finalErr)
				require.Equal(t, http.StatusUnauthorized, finalErr.StatusCode)
				require.True(t, finalErr.expiredBeforeAppend)
			}
			require.NotEmpty(t, got)
			for _, data := range got {
				require.Equal(t, got[0], data)
			}
		})
	}
}

// A filer-proxy upload must retry a marked-expiry 401 on the SAME FID -- the
// proxy mints a fresh volume JWT per forwarded request, so a same-FID replay
// recovers without a reassignment it does not own.
func TestUploadExpiryProxyRetriesSameFid(t *testing.T) {
	const fid = "3,01637037d6"
	var mu sync.Mutex
	calls := 0
	var seenFids []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		calls++
		n := calls
		seenFids = append(seenFids, r.URL.Query().Get(util_http.ProxyChunkIdParam))
		mu.Unlock()
		if n == 1 {
			// The proxy-minted volume JWT expired in transit on the first try.
			w.Header().Set(util_http.WriteOutcomeHeader, util_http.WriteOutcomeExpiredJwtBeforeAppend)
			w.WriteHeader(401)
			_, _ = io.WriteString(w, `{"error":"wrong jwt"}`)
			return
		}
		_, _ = io.WriteString(w, `{"size":5}`)
	}))
	defer srv.Close()
	proxyURL := srv.URL + "/?" + util_http.ProxyChunkIdParam + "=" + fid
	result, err := NewUploaderWithHttpClient(srv.Client()).UploadData(context.Background(), []byte("plain"), &UploadOption{UploadUrl: proxyURL})
	require.NoError(t, err)
	require.Equal(t, uint32(5), result.Size)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 2, calls)
	// Both forwarded requests used the same FID: a proxy upload never reassigns.
	require.Equal(t, []string{fid, fid}, seenFids)
}

// With filer signing enabled, the caller's filer credential can expire together
// with the volume JWT. The proxy same-FID retry must carry a freshly minted
// filer credential, not replay the stale one (which the filer would reject with
// an unmarked 401 that stops recovery).
func TestUploadExpiryProxyRefreshesFilerJwtOnRetry(t *testing.T) {
	const fid = "3,01637037d6"
	var mu sync.Mutex
	calls := 0
	var auths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		calls++
		n := calls
		auths = append(auths, r.Header.Get("Authorization"))
		mu.Unlock()
		if n == 1 {
			w.Header().Set(util_http.WriteOutcomeHeader, util_http.WriteOutcomeExpiredJwtBeforeAppend)
			w.WriteHeader(401)
			_, _ = io.WriteString(w, `{"error":"wrong jwt"}`)
			return
		}
		_, _ = io.WriteString(w, `{"size":5}`)
	}))
	defer srv.Close()
	proxyURL := srv.URL + "/?" + util_http.ProxyChunkIdParam + "=" + fid
	minted := 0
	option := &UploadOption{UploadUrl: proxyURL, Jwt: "stale-filer-jwt", FilerJwt: func(isWrite bool) security.EncodedJwt {
		minted++
		return security.EncodedJwt(fmt.Sprintf("fresh-filer-jwt-%d", minted))
	}}
	result, err := NewUploaderWithHttpClient(srv.Client()).UploadData(context.Background(), []byte("plain"), option)
	require.NoError(t, err)
	require.Equal(t, uint32(5), result.Size)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 2, calls)
	require.Equal(t, 1, minted)
	// The first request carried the stale credential; the retry re-minted one.
	require.Equal(t, "Bearer stale-filer-jwt", auths[0])
	require.Equal(t, "Bearer fresh-filer-jwt-1", auths[1])
}

// A proxy upload driven through uploadWithRetryData must be terminal after its
// bounded same-FID attempts: the outer loop must not reassign a FID the proxy
// owns, nor count exhausted groups toward the expiry budget.
func TestUploadExpiryProxyDoesNotReassignThroughOuterRetry(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		calls++
		mu.Unlock()
		// The proxy keeps reporting the marked expiry (never recovers).
		w.Header().Set(util_http.WriteOutcomeHeader, util_http.WriteOutcomeExpiredJwtBeforeAppend)
		w.WriteHeader(401)
		_, _ = io.WriteString(w, `{"error":"wrong jwt"}`)
	}))
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")
	assigns := 0
	option := &UploadOption{
		FilerJwt: func(isWrite bool) security.EncodedJwt { return "fresh-filer-jwt" },
		GenUploadUrl: func(host, fileId string) string {
			return "http://" + host + "/?" + util_http.ProxyChunkIdParam + "=" + fileId
		},
	}
	_, _, err := NewUploaderWithHttpClient(srv.Client()).uploadWithRetryData(func() (string, string, security.EncodedJwt, bool, error) {
		assigns++
		return "3,01637037d6", host, "vol-jwt", false, nil
	}, option, []byte("plain"))
	require.Error(t, err)
	mu.Lock()
	defer mu.Unlock()
	// One assignment (no reassignment), and only the inner bounded same-FID
	// attempts reached the server (the outer loop did not re-enter).
	require.Equal(t, 1, assigns)
	require.Equal(t, 3, calls)
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
