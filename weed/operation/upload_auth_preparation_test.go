package operation

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

type preparationAssignClient struct {
	filer_pb.SeaweedFilerClient
	assign func(*filer_pb.AssignVolumeRequest) *filer_pb.AssignVolumeResponse
}

func (c *preparationAssignClient) AssignVolume(_ context.Context, req *filer_pb.AssignVolumeRequest, _ ...grpc.CallOption) (*filer_pb.AssignVolumeResponse, error) {
	return c.assign(req), nil
}
func (c *preparationAssignClient) WithFilerClient(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fn(c)
}
func (c *preparationAssignClient) AdjustedUrl(loc *filer_pb.Location) string { return loc.Url }
func (c *preparationAssignClient) GetDataCenter() string                     { return "" }

// The preparation collaborator performs real gzip/encryption and advances only
// the JWT verifier's clock. No wall-clock sleep or slow-machine threshold is used.
func TestUploadPreparesBeforeAssignment(t *testing.T) {
	for _, cipher := range []bool{false, true} {
		name := "gzip"
		if cipher {
			name = "gzip_cipher"
		}
		t.Run(name, func(t *testing.T) {
			payload := bytes.Repeat([]byte{0}, 128*1024)
			sum := md5.Sum(payload)
			digest := base64.StdEncoding.EncodeToString(sum[:])
			const fid = "3,01637037d6"
			key := []byte("disposable uploader boundary test")
			var now atomic.Int64
			now.Store(1700000000)
			var requests atomic.Int32
			var stored []*needle.Needle
			var mu sync.Mutex
			var parseFailure atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				claims := &security.SeaweedFileIdClaims{}
				token, err := jwt.ParseWithClaims(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), claims, func(*jwt.Token) (interface{}, error) { return key, nil }, jwt.WithTimeFunc(func() time.Time { return time.Unix(now.Load(), 0) }), jwt.WithValidMethods([]string{"HS256"}))
				if err != nil || !token.Valid || claims.Fid != fid {
					w.WriteHeader(http.StatusUnauthorized)
					_, _ = io.WriteString(w, `{"error":"wrong jwt"}`)
					return
				}
				if r.Method != http.MethodPost || r.URL.Path != "/"+fid || r.URL.Query().Get("fsync") != "true" {
					parseFailure.Store(true)
				}
				n, _, gotMd5, err := needle.CreateNeedleFromRequest(r, false, 1024*1024, &bytes.Buffer{})
				if err != nil {
					parseFailure.Store(true)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if cipher {
					if gotMd5 != "" || len(n.Name) != 0 || len(n.Mime) != 0 || len(n.Pairs) != 0 {
						parseFailure.Store(true)
					}
				} else if gotMd5 != digest {
					parseFailure.Store(true)
				}
				mu.Lock()
				stored = append(stored, n)
				first := len(stored) == 1
				mu.Unlock()
				if first {
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = io.WriteString(w, `{"error":"busy"}`)
					return
				}
				w.Header().Set("Content-MD5", digest)
				w.WriteHeader(http.StatusCreated)
				_, _ = io.WriteString(w, `{"name":"long.bin","size":131072}`)
			}))
			defer server.Close()
			uploader := newUploader(server.Client())
			preparations := 0
			uploader.prepareData = func(data []byte, opt *UploadOption) (*preparedUpload, error) {
				preparations++
				prepared, err := prepareUploadData(data, opt)
				now.Add(11)
				return prepared, err
			}
			assignments := 0
			client := &preparationAssignClient{assign: func(req *filer_pb.AssignVolumeRequest) *filer_pb.AssignVolumeResponse {
				assignments++
				if req.ExpectedDataSize != uint64(len(payload)) {
					t.Error("assignment did not receive clear size")
				}
				encoded, err := jwt.NewWithClaims(jwt.SigningMethodHS256, security.SeaweedFileIdClaims{Fid: fid, RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Unix(now.Load()+10, 0))}}).SignedString(key)
				if err != nil {
					t.Fatal("could not sign disposable fixture")
				}
				return &filer_pb.AssignVolumeResponse{FileId: fid, Auth: encoded, Location: &filer_pb.Location{Url: strings.TrimPrefix(server.URL, "http://")}, Fsync: true}
			}}
			option := &UploadOption{Filename: "long.bin", Cipher: cipher, MimeType: "application/octet-stream", WantMd5: true, PairMap: map[string]string{"test": "private-metadata"}}
			gotFid, result, err, clear := uploader.UploadWithRetry(client, &filer_pb.AssignVolumeRequest{Count: 1}, option, bytes.NewReader(payload))
			mu.Lock()
			saved := append([]*needle.Needle(nil), stored...)
			mu.Unlock()
			if err != nil {
				t.Fatalf("prepared upload was rejected: assignments=%d requests=%d preparations=%d", assignments, requests.Load(), preparations)
			}
			if gotFid != fid || !bytes.Equal(clear, payload) || result.Size != uint32(len(payload)) || result.Gzip != 1 || result.ContentMd5 != digest {
				t.Fatal("clear result metadata changed")
			}
			if assignments != 1 || preparations != 1 || requests.Load() != 2 || len(saved) != 2 || parseFailure.Load() {
				t.Fatalf("outbound/retry contract mismatch: assignments=%d preparations=%d requests=%d", assignments, preparations, requests.Load())
			}
			if !bytes.Equal(saved[0].Data, saved[1].Data) {
				t.Fatal("retry changed prepared bytes")
			}
			wire := saved[1].Data
			if cipher {
				if len(result.CipherKey) == 0 {
					t.Fatal("missing cipher key")
				}
				wire, err = util.Decrypt(wire, util.CipherKey(result.CipherKey))
				if err != nil {
					t.Fatal("cipher payload cannot be decrypted")
				}
			}
			decoded, err := util.DecompressData(wire)
			if err != nil || !bytes.Equal(decoded, payload) {
				t.Fatal("prepared wire payload changed clear content")
			}
			if option.MimeType != "application/octet-stream" {
				t.Fatal("preparation mutated caller MIME option")
			}
		})
	}
}

func TestUploadChunksPrepareBeforeAssignment(t *testing.T) {
	for _, mode := range []string{"relay", "fanout", "cipher"} {
		t.Run(mode, func(t *testing.T) {
			cipher := mode == "cipher"
			payload := bytes.Repeat([]byte{0}, 128*1024)
			sum := md5.Sum(payload)
			digest := base64.StdEncoding.EncodeToString(sum[:])
			const fid = "3,01637037d6"
			key := []byte("disposable chunk boundary test")
			var now atomic.Int64
			now.Store(1700000000)
			var posts, deletes, preparations, assignments atomic.Int32
			var invalid atomic.Bool
			var mu sync.Mutex
			var stored []*needle.Needle
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodDelete {
					deletes.Add(1)
					w.WriteHeader(http.StatusAccepted)
					return
				}
				posts.Add(1)
				claims := &security.SeaweedFileIdClaims{}
				token, err := jwt.ParseWithClaims(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), claims, func(*jwt.Token) (interface{}, error) { return key, nil }, jwt.WithTimeFunc(func() time.Time { return time.Unix(now.Load(), 0) }), jwt.WithValidMethods([]string{"HS256"}))
				if err != nil || !token.Valid || claims.Fid != fid {
					w.WriteHeader(http.StatusUnauthorized)
					_, _ = io.WriteString(w, `{"error":"wrong jwt"}`)
					return
				}
				if r.Method != http.MethodPost || r.URL.Path != "/"+fid || r.URL.Query().Get("fsync") != "true" {
					invalid.Store(true)
				}
				if mode == "fanout" && r.URL.Query().Get("type") != "replicate" {
					invalid.Store(true)
				}
				n, _, gotMd5, err := needle.CreateNeedleFromRequest(r, false, 1024*1024, &bytes.Buffer{})
				if err != nil {
					invalid.Store(true)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if cipher {
					if gotMd5 != "" || len(n.Name) != 0 || len(n.Mime) != 0 || len(n.Pairs) != 0 {
						invalid.Store(true)
					}
				} else if gotMd5 != digest {
					invalid.Store(true)
				}
				mu.Lock()
				stored = append(stored, n)
				mu.Unlock()
				w.Header().Set("Content-MD5", digest)
				w.WriteHeader(http.StatusCreated)
				_, _ = io.WriteString(w, `{"size":131072}`)
			})
			primary := httptest.NewServer(handler)
			defer primary.Close()
			replica := httptest.NewServer(handler)
			defer replica.Close()
			option := &ChunkedUploadOption{ChunkSize: 256 * 1024, Cipher: cipher, MimeType: "application/octet-stream", AssignFunc: func(ctx context.Context, count int, size uint64) (*VolumeAssignRequest, *AssignResult, error) {
				assignments.Add(1)
				if count != 1 || size != uint64(len(payload)) {
					invalid.Store(true)
				}
				encoded, err := jwt.NewWithClaims(jwt.SigningMethodHS256, security.SeaweedFileIdClaims{Fid: fid, RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Unix(now.Load()+10, 0))}}).SignedString(key)
				if err != nil {
					return nil, nil, err
				}
				result := &AssignResult{Fid: fid, Url: strings.TrimPrefix(primary.URL, "http://"), Auth: security.EncodedJwt(encoded), Fsync: true}
				if mode == "fanout" {
					result.Replicas = []Location{{Url: strings.TrimPrefix(replica.URL, "http://")}}
				}
				return nil, result, nil
			}}
			result, err := uploadReaderInChunks(context.Background(), bytes.NewReader(payload), option, func(data []byte, opt *UploadOption) (*preparedUpload, error) {
				preparations.Add(1)
				prepared, err := prepareUploadData(data, opt)
				now.Add(11)
				return prepared, err
			})
			if err != nil {
				t.Fatalf("chunk JWT expired during preparation: assignments=%d posts=%d preparations=%d", assignments.Load(), posts.Load(), preparations.Load())
			}
			wantPosts := int32(1)
			if mode == "fanout" {
				wantPosts = 2
			}
			if invalid.Load() || assignments.Load() != 1 || preparations.Load() != 1 || posts.Load() != wantPosts || deletes.Load() != 0 || len(stored) != int(wantPosts) {
				t.Fatal("chunk assignment/holder contract changed")
			}
			if len(result.FileChunks) != 1 || result.TotalSize != int64(len(payload)) || result.FileChunks[0].Size != uint64(len(payload)) || result.FileChunks[0].ETag != digest || !result.FileChunks[0].IsCompressed || result.FileChunks[0].FileId != fid {
				t.Fatal("chunk clear metadata changed")
			}
			if len(stored) == 2 && !bytes.Equal(stored[0].Data, stored[1].Data) {
				t.Fatal("holders received different prepared payloads")
			}
			wire := stored[0].Data
			if cipher {
				wire, err = util.Decrypt(wire, util.CipherKey(result.FileChunks[0].CipherKey))
				if err != nil {
					t.Fatal("cannot decrypt chunk")
				}
			}
			decoded, err := util.DecompressData(wire)
			if err != nil || !bytes.Equal(decoded, payload) {
				t.Fatal("stored chunk differs from clear input")
			}
		})
	}
}

func TestUploadChunksPreserveCustomUploadContract(t *testing.T) {
	payload := bytes.Repeat([]byte{0}, 32768)
	calls := 0
	result, err := uploadReaderInChunks(context.Background(), bytes.NewReader(payload), &ChunkedUploadOption{ChunkSize: 65536, Cipher: true, MimeType: "application/octet-stream", AssignFunc: func(context.Context, int, uint64) (*VolumeAssignRequest, *AssignResult, error) {
		return nil, &AssignResult{Fid: "3,01637037d6", Url: "unused", Auth: "disposable-token", Fsync: true}, nil
	}, UploadFunc: func(_ context.Context, data []byte, opt *UploadOption) (*UploadResult, error) {
		calls++
		if !bytes.Equal(data, payload) || !opt.Cipher || opt.IsInputCompressed || opt.MimeType != "application/octet-stream" || opt.Jwt != "disposable-token" || opt.UploadUrl != "http://unused/3,01637037d6?fsync=true" || opt.Md5 == "" || opt.MaxAttempts != 1 {
			t.Error("custom uploader contract changed")
		}
		return &UploadResult{Size: uint32(len(data))}, nil
	}}, func([]byte, *UploadOption) (*preparedUpload, error) {
		t.Fatal("custom uploader must receive original clear bytes without internal preparation")
		return nil, nil
	})
	if err != nil || calls != 1 || len(result.FileChunks) != 1 {
		t.Fatal("custom upload did not complete")
	}
}

func TestUploadChunksReassignReusesPreparedPayload(t *testing.T) {
	for _, cipher := range []bool{false, true} {
		t.Run(map[bool]string{false: "relay", true: "cipher"}[cipher], func(t *testing.T) {
			payload := bytes.Repeat([]byte{0}, 128*1024)
			sum := md5.Sum(payload)
			digest := base64.StdEncoding.EncodeToString(sum[:])
			fids := []string{"3,01637037d6", "4,02637037d6"}
			key := []byte("disposable reassign test")
			var mu sync.Mutex
			var bodies [][]byte
			var postedFids, deletedFids []string
			var preparations, assignments atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fid := strings.TrimPrefix(r.URL.Path, "/")
				if r.Method == http.MethodDelete {
					mu.Lock()
					deletedFids = append(deletedFids, fid)
					mu.Unlock()
					w.WriteHeader(http.StatusAccepted)
					return
				}
				claims := &security.SeaweedFileIdClaims{}
				if _, err := jwt.ParseWithClaims(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), claims, func(*jwt.Token) (interface{}, error) { return key, nil }); err != nil || claims.Fid != fid {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				n, _, _, err := needle.CreateNeedleFromRequest(r, false, 1024*1024, &bytes.Buffer{})
				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				mu.Lock()
				bodies = append(bodies, n.Data)
				postedFids = append(postedFids, fid)
				mu.Unlock()
				if fid == fids[0] {
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = io.WriteString(w, `{"error":"volume unavailable"}`)
					return
				}
				w.Header().Set("Content-MD5", digest)
				w.WriteHeader(http.StatusCreated)
				_, _ = io.WriteString(w, `{"size":131072}`)
			}))
			defer server.Close()
			option := &ChunkedUploadOption{ChunkSize: 256 * 1024, Cipher: cipher, MimeType: "application/octet-stream", AssignFunc: func(context.Context, int, uint64) (*VolumeAssignRequest, *AssignResult, error) {
				fid := fids[assignments.Add(1)-1]
				encoded, err := jwt.NewWithClaims(jwt.SigningMethodHS256, security.SeaweedFileIdClaims{Fid: fid}).SignedString(key)
				if err != nil {
					return nil, nil, err
				}
				return nil, &AssignResult{Fid: fid, Url: strings.TrimPrefix(server.URL, "http://"), Auth: security.EncodedJwt(encoded)}, nil
			}}
			result, err := uploadReaderInChunks(context.Background(), bytes.NewReader(payload), option, func(data []byte, opt *UploadOption) (*preparedUpload, error) {
				preparations.Add(1)
				return prepareUploadData(data, opt)
			})
			if err != nil {
				t.Fatalf("upload after reassignment: %v", err)
			}
			if preparations.Load() != 1 || assignments.Load() != 2 {
				t.Fatalf("preparations=%d assignments=%d, want 1 and 2", preparations.Load(), assignments.Load())
			}
			if len(bodies) != 2 || !bytes.Equal(bodies[0], bodies[1]) || postedFids[0] != fids[0] || postedFids[1] != fids[1] {
				t.Fatalf("posted fids %v; want the same prepared bytes on both assignments", postedFids)
			}
			if len(deletedFids) != 1 || deletedFids[0] != fids[0] {
				t.Fatalf("deleted fids %v, want only the failed %s", deletedFids, fids[0])
			}
			if len(result.FileChunks) != 1 || result.FileChunks[0].FileId != fids[1] {
				t.Fatal("chunk does not name the reassigned fid")
			}
			wire := bodies[1]
			if cipher {
				if wire, err = util.Decrypt(wire, util.CipherKey(result.FileChunks[0].CipherKey)); err != nil {
					t.Fatalf("decrypt reassigned chunk: %v", err)
				}
			}
			if decoded, err := util.DecompressData(wire); err != nil || !bytes.Equal(decoded, payload) {
				t.Fatal("reassigned chunk differs from clear input")
			}
		})
	}
}
