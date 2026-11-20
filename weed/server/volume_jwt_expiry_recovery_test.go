package weed_server

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type expiryAssignRPC struct {
	filer_pb.SeaweedFilerClient
	assign func() (*filer_pb.AssignVolumeResponse, error)
}

func (c expiryAssignRPC) AssignVolume(_ context.Context, _ *filer_pb.AssignVolumeRequest, _ ...grpc.CallOption) (*filer_pb.AssignVolumeResponse, error) {
	return c.assign()
}

type expiryFilerClient struct{ rpc expiryAssignRPC }

func (c expiryFilerClient) WithFilerClient(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fn(c.rpc)
}
func (c expiryFilerClient) AdjustedUrl(l *filer_pb.Location) string { return l.Url }
func (c expiryFilerClient) GetDataCenter() string                   { return "" }

type expiryCountingBody struct {
	io.ReadCloser
	bytesRead int
}

func (b *expiryCountingBody) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	b.bytesRead += n
	return n, e
}

func expiryVolume(t *testing.T, key security.SigningKey) *VolumeServer {
	t.Helper()
	s := newTierTestStore(t, t.TempDir())
	require.NoError(t, s.AddVolume(1, "", storage.NeedleMapInMemory, "000", "", 0, needle.Version3, 0, types.HardDriveType, 0))
	return &VolumeServer{store: s, guard: security.NewGuard(nil, string(key), 10, "", 0, "", ""), fileSizeLimitBytes: 4 << 20}
}

func expirySignedToken(t *testing.T, key security.SigningKey, fid string, expires time.Time) string {
	t.Helper()
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, security.SeaweedFileIdClaims{Fid: fid, RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(expires)}}).SignedString([]byte(key))
	require.NoError(t, err)
	return token
}

// Assignment returns a genuinely valid ten-second JWT. The HTTP server delays
// only that assignment until its exact expiry boundary before running the real
// PostHandler. This bounded wait reproduces transit/preauthorization delay; it
// makes no CPU-throughput or shared-clock-injection assumption.
func TestExpiredVolumeJwtReassignmentHTTP(t *testing.T) {
	for _, tc := range []struct {
		name            string
		forever, cipher bool
	}{{"bounded", false, false}, {"bounded_cipher", false, true}, {"retry_forever", true, false}, {"retry_forever_cipher", true, true}} {
		name, forever := tc.name, tc.forever
		t.Run(name, func(t *testing.T) {
			key := security.SigningKey("disposable-volume-expiry-test")
			vs := expiryVolume(t, key)
			plain := bytes.Repeat([]byte("compressible lifecycle payload"), 4096)
			sum := md5.Sum(plain)
			var mu sync.Mutex
			var ids []string
			var statuses []int
			var outcomes []string
			var bodyReads []int
			var fsyncs []string
			var assigned int
			var firstExpires time.Time
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				expires := firstExpires
				mu.Unlock()
				if r.URL.Path == "/1,0000000100001234" {
					if delay := time.Until(expires.Add(100 * time.Millisecond)); delay > 0 {
						timer := time.NewTimer(delay)
						defer timer.Stop()
						select {
						case <-timer.C:
						case <-r.Context().Done():
							return
						}
					}
				}
				counted := &expiryCountingBody{ReadCloser: r.Body}
				r.Body = counted
				rec := httptest.NewRecorder()
				vs.PostHandler(rec, r)
				mu.Lock()
				ids = append(ids, r.URL.Path)
				statuses = append(statuses, rec.Code)
				outcomes = append(outcomes, rec.Header().Get("X-Seaweed-Write-Outcome"))
				bodyReads = append(bodyReads, counted.bytesRead)
				fsyncs = append(fsyncs, r.URL.Query().Get("fsync"))
				mu.Unlock()
				for k, v := range rec.Header() {
					w.Header()[k] = v
				}
				w.WriteHeader(rec.Code)
				_, _ = w.Write(rec.Body.Bytes())
			}))
			defer server.Close()
			c := expiryFilerClient{rpc: expiryAssignRPC{assign: func() (*filer_pb.AssignVolumeResponse, error) {
				assigned++
				fid := "1,0000000100001234"
				expiry := time.Now().Add(10 * time.Second)
				if assigned == 1 {
					mu.Lock()
					firstExpires = expiry
					mu.Unlock()
				}
				if assigned > 1 {
					fid = "1,0000000200001234"
					expiry = time.Now().Add(time.Hour)
				}
				token := expirySignedToken(t, key, fid, expiry)
				parsed, decodeErr := security.DecodeJwt(key, security.EncodedJwt(token), &security.SeaweedFileIdClaims{})
				require.NoError(t, decodeErr)
				require.True(t, parsed.Valid)
				return &filer_pb.AssignVolumeResponse{FileId: fid, Auth: token, Fsync: true, Location: &filer_pb.Location{Url: strings.TrimPrefix(server.URL, "http://")}}, nil
			}}}
			httpClient := server.Client()
			httpClient.Timeout = 25 * time.Second
			uploader := operation.NewUploaderWithHttpClient(httpClient)
			fid, result, err, returned := uploader.UploadWithRetry(c, &filer_pb.AssignVolumeRequest{Path: "/own/long.bin"}, &operation.UploadOption{Cipher: tc.cipher, Filename: "long.bin", MimeType: "application/octet-stream", WantMd5: true, RetryForever: forever}, bytes.NewReader(plain))
			require.NoError(t, err)
			require.Equal(t, "1,0000000200001234", fid)
			require.Equal(t, 2, assigned)
			require.Equal(t, plain, returned)
			require.Equal(t, uint32(len(plain)), result.Size)
			require.Equal(t, base64.StdEncoding.EncodeToString(sum[:]), result.ContentMd5)
			mu.Lock()
			gotIds := append([]string(nil), ids...)
			gotStatuses := append([]int(nil), statuses...)
			gotFsyncs := append([]string(nil), fsyncs...)
			gotOutcomes := append([]string(nil), outcomes...)
			gotBodyReads := append([]int(nil), bodyReads...)
			mu.Unlock()
			require.Equal(t, []string{"/1,0000000100001234", "/1,0000000200001234"}, gotIds)
			require.Equal(t, []int{http.StatusUnauthorized, http.StatusCreated}, gotStatuses)
			require.Equal(t, []string{"true", "true"}, gotFsyncs)
			require.Equal(t, []string{"expired-jwt-before-append", ""}, gotOutcomes)
			require.Equal(t, 0, gotBodyReads[0])
			require.Positive(t, gotBodyReads[1])
			require.Equal(t, uint32(1), result.Gzip)
			if tc.cipher {
				require.NotEmpty(t, result.CipherKey)
			} else {
				require.Empty(t, result.CipherKey)
			}
			_, err = vs.store.ReadVolumeNeedle(1, &needle.Needle{Id: 1, Cookie: 0x1234}, nil, nil)
			require.Error(t, err)
			stored := &needle.Needle{Id: 2, Cookie: 0x1234}
			_, err = vs.store.ReadVolumeNeedle(1, stored, nil, nil)
			require.NoError(t, err)
			require.NotEmpty(t, stored.Data)
			recovered := stored.Data
			if tc.cipher {
				recovered, err = util.Decrypt(recovered, util.CipherKey(result.CipherKey))
				require.NoError(t, err)
			}
			if result.Gzip > 0 {
				recovered, err = util.DecompressData(recovered)
				require.NoError(t, err)
			}
			require.Equal(t, plain, recovered)
		})
	}
}

func TestVolumeJwtExpiryRefusalBoundary(t *testing.T) {
	key := security.SigningKey("disposable-volume-boundary")
	for _, tc := range []struct {
		name                                                                  string
		expired, nbf, iat, badKey, badFid, missing, urlencoded, suffix, noKey bool
		want                                                                  int
		marker                                                                bool
	}{
		{name: "expired", expired: true, want: 401, marker: true},
		{name: "expired_suffix", expired: true, suffix: true, want: 401, marker: true},
		{name: "expired_future_nbf", expired: true, nbf: true, want: 401},
		{name: "expired_future_iat", expired: true, iat: true, want: 401},
		{name: "expired_wrong_signature", expired: true, badKey: true, want: 401},
		{name: "expired_wrong_fid", expired: true, badFid: true, want: 401},
		{name: "missing", missing: true, want: 401},
		{name: "nonmultipart", expired: true, urlencoded: true, want: 401},
		{name: "valid", want: 201},
		{name: "no_key", missing: true, noKey: true, want: 201},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vs := expiryVolume(t, key)
			if tc.noKey {
				vs.guard = security.NewGuard(nil, "", 0, "", 0, "", "")
			}
			plain := []byte("boundary content")
			var body bytes.Buffer
			mw := multipart.NewWriter(&body)
			part, e := mw.CreateFormFile("file", "sample.bin")
			require.NoError(t, e)
			_, e = part.Write(plain)
			require.NoError(t, e)
			require.NoError(t, mw.Close())
			contentType := mw.FormDataContentType()
			if tc.urlencoded {
				body.Reset()
				body.WriteString("sample=value")
				contentType = "application/x-www-form-urlencoded"
			}
			counted := &expiryCountingBody{ReadCloser: io.NopCloser(bytes.NewReader(body.Bytes()))}
			path := "/1,0000000100001234"
			if tc.suffix {
				path += "_2"
			}
			req := httptest.NewRequest(http.MethodPost, path, counted)
			req.Header.Set("Content-Type", contentType)
			// A request-supplied marker must never be echoed as response proof.
			req.Header.Set("X-Seaweed-Write-Outcome", "expired-jwt-before-append")
			if !tc.missing {
				fid := "1,0000000100001234"
				if tc.badFid {
					fid = "1,0000000200001234"
				}
				exp := time.Now().Add(time.Hour)
				if tc.expired {
					exp = time.Now().Add(-time.Hour)
				}
				claims := security.SeaweedFileIdClaims{Fid: fid, RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(exp)}}
				if tc.nbf {
					claims.NotBefore = jwt.NewNumericDate(time.Now().Add(time.Hour))
				}
				if tc.iat {
					claims.IssuedAt = jwt.NewNumericDate(time.Now().Add(time.Hour))
				}
				signingKey := key
				if tc.badKey {
					signingKey = security.SigningKey("different-disposable-key")
				}
				token, e := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(signingKey))
				require.NoError(t, e)
				req.Header.Set("Authorization", "Bearer "+token)
			}
			rec := httptest.NewRecorder()
			vs.PostHandler(rec, req)
			require.Equal(t, tc.want, rec.Code)
			want := ""
			if tc.marker {
				want = "expired-jwt-before-append"
			}
			require.Equal(t, want, rec.Header().Get("X-Seaweed-Write-Outcome"))
			n := &needle.Needle{Id: 1, Cookie: 0x1234}
			_, readErr := vs.store.ReadVolumeNeedle(1, n, nil, nil)
			if tc.want == 401 {
				require.Error(t, readErr)
				if !tc.urlencoded {
					require.Zero(t, counted.bytesRead)
				}
			} else {
				require.NoError(t, readErr)
				require.Equal(t, plain, n.Data)
			}
		})
	}
}

func TestVolumeJwtExpiryProxyForwarding(t *testing.T) {
	volumeKey := security.SigningKey("disposable-proxy-volume-key")
	filerKey := security.SigningKey("disposable-proxy-filer-key")
	vs := expiryVolume(t, volumeKey)
	var hits atomic.Int32
	volume := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			claims := &security.SeaweedFileIdClaims{}
			parsed, err := security.DecodeJwt(volumeKey, security.GetJwt(r), claims)
			if err != nil || !parsed.Valid || claims.ExpiresAt == nil {
				w.WriteHeader(500)
				return
			}
			timer := time.NewTimer(time.Until(claims.ExpiresAt.Time.Add(100 * time.Millisecond)))
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-r.Context().Done():
				return
			}
		}
		vs.PostHandler(w, r)
	}))
	defer volume.Close()
	fs := &FilerServer{filerGuard: security.NewGuard(nil, string(filerKey), 3600, "", 0, "", ""), volumeGuard: security.NewGuard(nil, string(volumeKey), 10, "", 0, "", "")}
	fid := "1,0000000100001234"
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Exercise the same real caller verifier as filerHandler, then its actual
		// forwarding function with an already-resolved synthetic target.
		if _, ok := fs.authenticateFilerJwt(r, true); !ok {
			w.WriteHeader(401)
			return
		}
		fs.proxyToVolumeServerURL(w, r, fid, volume.URL+"/"+fid)
	}))
	defer proxy.Close()
	caller := string(security.GenJwtForFilerServer(filerKey, 3600))
	for i, want := range []int{401, 401, 201} {
		var body bytes.Buffer
		mw := multipart.NewWriter(&body)
		part, err := mw.CreateFormFile("file", "proxy.bin")
		require.NoError(t, err)
		_, err = part.Write([]byte("proxy clear data"))
		require.NoError(t, err)
		require.NoError(t, mw.Close())
		req, err := http.NewRequest(http.MethodPost, proxy.URL+"/?proxyChunkId="+fid, bytes.NewReader(body.Bytes()))
		require.NoError(t, err)
		req.Header.Set("Content-Type", mw.FormDataContentType())
		req.Header.Set("X-Seaweed-Write-Outcome", "expired-jwt-before-append")
		token := caller
		if i == 1 {
			token = "invalid-disposable-caller"
		}
		req.Header.Set("Authorization", "Bearer "+token)
		client := proxy.Client()
		client.Timeout = 25 * time.Second
		resp, err := client.Do(req)
		require.NoError(t, err)
		_, err = io.Copy(io.Discard, resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, want, resp.StatusCode)
		marker := ""
		if i == 0 {
			marker = "expired-jwt-before-append"
		}
		require.Equal(t, marker, resp.Header.Get("X-Seaweed-Write-Outcome"))
		if i == 1 {
			require.Equal(t, int32(1), hits.Load())
		}
	}
	require.Equal(t, int32(2), hits.Load())
}

func TestVolumeJwtExpiryFanoutRollback(t *testing.T) {
	key := security.SigningKey("disposable-fanout-expiry-key")
	primary, replica := expiryVolume(t, key), expiryVolume(t, key)
	firstCommitted := make(chan struct{})
	var committed sync.Once
	var mu sync.Mutex
	var firstReplicaStatuses []int
	var firstReplicaMarkers []string
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			primary.DeleteHandler(w, r)
			return
		}
		rec := httptest.NewRecorder()
		primary.PostHandler(rec, r)
		if r.URL.Path == "/1,0000000100001234" && rec.Code == 201 {
			committed.Do(func() { close(firstCommitted) })
		}
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	}))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			replica.DeleteHandler(w, r)
			return
		}
		if r.URL.Path == "/1,0000000100001234" {
			select {
			case <-firstCommitted:
			case <-r.Context().Done():
				return
			}
			claims := &security.SeaweedFileIdClaims{}
			parsed, err := security.DecodeJwt(key, security.GetJwt(r), claims)
			if err != nil || !parsed.Valid || claims.ExpiresAt == nil {
				w.WriteHeader(500)
				return
			}
			timer := time.NewTimer(time.Until(claims.ExpiresAt.Time.Add(100 * time.Millisecond)))
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-r.Context().Done():
				return
			}
		}
		rec := httptest.NewRecorder()
		replica.PostHandler(rec, r)
		if r.URL.Path == "/1,0000000100001234" {
			mu.Lock()
			firstReplicaStatuses = append(firstReplicaStatuses, rec.Code)
			firstReplicaMarkers = append(firstReplicaMarkers, rec.Header().Get("X-Seaweed-Write-Outcome"))
			mu.Unlock()
		}
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	}))
	defer b.Close()
	var assignments int
	plain := bytes.Repeat([]byte("fanout content"), 4096)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	result, err := operation.UploadReaderInChunks(ctx, bytes.NewReader(plain), &operation.ChunkedUploadOption{
		ChunkSize: 1 << 20, MimeType: "application/octet-stream", AssignFunc: func(_ context.Context, _ int, _ uint64) (*operation.VolumeAssignRequest, *operation.AssignResult, error) {
			assignments++
			fid := "1,0000000100001234"
			if assignments > 1 {
				fid = "1,0000000200001234"
			}
			return nil, &operation.AssignResult{Fid: fid, Url: strings.TrimPrefix(a.URL, "http://"), Auth: security.EncodedJwt(expirySignedToken(t, key, fid, time.Now().Add(10*time.Second))), Replicas: []operation.Location{{Url: strings.TrimPrefix(b.URL, "http://")}}, Fsync: true}, nil
		}, LookupJwtFunc: func(_ context.Context, fid string) security.EncodedJwt {
			return security.EncodedJwt(expirySignedToken(t, key, fid, time.Now().Add(time.Hour)))
		},
	})
	require.NoError(t, err)
	require.Equal(t, 2, assignments)
	mu.Lock()
	gotStatuses := append([]int(nil), firstReplicaStatuses...)
	gotMarkers := append([]string(nil), firstReplicaMarkers...)
	mu.Unlock()
	require.Equal(t, []int{401}, gotStatuses)
	require.Equal(t, []string{"expired-jwt-before-append"}, gotMarkers)
	require.Len(t, result.FileChunks, 1)
	require.Equal(t, "1,0000000200001234", result.FileChunks[0].FileId)
	for _, vs := range []*VolumeServer{primary, replica} {
		_, err := vs.store.ReadVolumeNeedle(1, &needle.Needle{Id: 1, Cookie: 0x1234}, nil, nil)
		require.Error(t, err)
		n := &needle.Needle{Id: 2, Cookie: 0x1234}
		_, err = vs.store.ReadVolumeNeedle(1, n, nil, nil)
		require.NoError(t, err)
		data := n.Data
		if result.FileChunks[0].IsCompressed {
			data, err = util.DecompressData(data)
			require.NoError(t, err)
		}
		require.Equal(t, plain, data)
	}
}
