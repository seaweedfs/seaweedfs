package weed_server

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"image"
	"image/jpeg"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/require"
)

// heldSyncBackend parks the first .dat fsync until released: the window where
// a durable write is appended but not indexed yet.
type heldSyncBackend struct {
	backend.BackendStorageFile
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (b *heldSyncBackend) Sync() error {
	b.once.Do(func() {
		close(b.entered)
		<-b.release
	})
	return b.BackendStorageFile.Sync()
}

// startHeldWrite starts a durable write of needle 1 on a fresh volume and
// returns once it is parked in its .dat fsync, appended but not indexed.
func startHeldWrite(t *testing.T) (store *storage.Store, fid string, release func(), written <-chan error) {
	store = storage.NewStore(nil, "localhost", 8080, 18080, "http://localhost:8080", "store-id",
		[]string{t.TempDir()}, []int32{10}, []util.MinFreeSpace{{}}, "", storage.NeedleMapInMemory,
		[]types.DiskType{types.HardDriveType}, nil, 0, stats.DefaultDiskIOProbeConfig())
	t.Cleanup(store.Close)
	require.NoError(t, store.AddVolume(1, "", storage.NeedleMapInMemory, "000", "", 0, needle.Version3, 0, types.HardDriveType, 0))
	v := store.GetVolume(1)
	held := &heldSyncBackend{BackendStorageFile: v.DataBackend, entered: make(chan struct{}), release: make(chan struct{})}
	v.DataBackend = held

	n := &needle.Needle{Id: 1, Cookie: 0x1234, Data: []byte("late write")}
	n.Checksum = needle.NewCRC(n.Data)
	done := make(chan error, 1)
	go func() {
		_, err := store.WriteVolumeNeedle(1, n, true, true)
		done <- err
	}()
	<-held.entered
	var releaseOnce sync.Once
	release = func() { releaseOnce.Do(func() { close(held.release) }) }
	t.Cleanup(release)
	return store, needle.NewFileIdFromNeedle(1, n).String(), release, done
}

func requireTakenBack(t *testing.T, store *storage.Store, written <-chan error, takenBack bool) {
	t.Helper()
	err := <-written
	_, readErr := store.ReadVolumeNeedle(1, &needle.Needle{Id: 1}, nil, nil)
	if takenBack {
		require.ErrorIs(t, err, storage.ErrorDeleted)
		require.ErrorIs(t, readErr, storage.ErrorDeleted)
	} else {
		require.NoError(t, err)
		require.NoError(t, readErr)
	}
}

// A replica delete that arrives while the write it undoes is in its fsync
// finds nothing, yet the write must not land live afterwards. A plain delete
// that finds nothing is not sent on to the replicas, so it leaves the write.
func TestDeleteHandlerCancelsAWriteInItsFsyncWindow(t *testing.T) {
	for _, tc := range []struct {
		query string
		live  bool
	}{
		{"?type=replicate", false},
		{"", true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			store, fid, release, written := startHeldWrite(t)
			resp := httptest.NewRecorder()
			vs := &VolumeServer{store: store, guard: security.NewGuard(nil, "", 0, "", 0)}
			vs.DeleteHandler(resp, httptest.NewRequest(http.MethodDelete, "/"+fid+tc.query, nil))
			require.Equal(t, http.StatusNotFound, resp.Code)
			release()
			requireTakenBack(t, store, written, !tc.live)
		})
	}
}

// A batch delete that skips the cookie check still cancels the in-flight
// writes of the file id it names, and only those.
func TestBatchDeleteCancelsAWriteInItsFsyncWindow(t *testing.T) {
	for _, tc := range []struct {
		cookie    string
		takenBack bool
	}{
		{"", true},
		{"00004321", false},
	} {
		t.Run(tc.cookie, func(t *testing.T) {
			store, fid, release, written := startHeldWrite(t)
			if tc.cookie != "" {
				fid = fid[:len(fid)-8] + tc.cookie
			}
			deleted := make(chan error, 1)
			go func() {
				_, err := (&VolumeServer{store: store}).BatchDelete(context.Background(), &volume_server_pb.BatchDeleteRequest{
					FileIds:         []string{fid},
					SkipCookieCheck: true,
				})
				deleted <- err
			}()
			// the delete waits behind the held write's sync lock
			require.Eventually(t, func() bool {
				buf := make([]byte, 1<<20)
				return bytes.Contains(buf[:runtime.Stack(buf, true)], []byte("(*Volume).syncDelete"))
			}, 10*time.Second, time.Millisecond)
			release()
			require.NoError(t, <-deleted)
			err := <-written
			if tc.takenBack {
				require.ErrorIs(t, err, storage.ErrorDeleted)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// A replica rejects a Content-MD5 that does not match the bytes it is sent.
func TestPostHandlerForwardsAContentMd5TheReplicaAccepts(t *testing.T) {
	var encoded bytes.Buffer
	require.NoError(t, jpeg.Encode(&encoded, image.NewRGBA(image.Rect(0, 0, 3, 2)), nil))
	plain := encoded.Bytes()
	exif := []byte("Exif\x00\x00MM\x00\x2a\x00\x00\x00\x08\x00\x01\x01\x12\x00\x03\x00\x00\x00\x01\x00\x03\x00\x00\x00\x00\x00\x00")
	tagged := append([]byte{0xFF, 0xD8, 0xFF, 0xE1, 0, byte(len(exif) + 2)}, exif...)
	tagged = append(tagged, plain[2:]...)

	vid := needle.VolumeId(849300201)
	allocate := func(vs *VolumeServer) {
		vs.store = newTraversalTestStore(t.TempDir())
		t.Cleanup(vs.store.Close)
		_, err := vs.AllocateVolume(context.Background(), &volume_server_pb.AllocateVolumeRequest{VolumeId: uint32(vid), Replication: "001", Version: uint32(needle.GetCurrentVersion())})
		require.NoError(t, err)
	}
	replicaVs := &VolumeServer{fileSizeLimitBytes: 1 << 20}
	allocate(replicaVs)
	replicaVs.guard = security.NewGuard(nil, "", 0, "", 0)
	replicaResp := make(chan *httptest.ResponseRecorder, 1)
	replica := newProxyTestHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := httptest.NewRecorder()
		replicaVs.PostHandler(rec, r)
		replicaResp <- rec
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	}))

	primary := &master_pb.Location{}
	vs := newProxyTestVolumeServer(t, vid.String(), []*master_pb.Location{primary, {Url: strings.TrimPrefix(replica.URL, "http://")}}, nil)
	allocate(vs)
	primary.Url = util.JoinHostPort(vs.store.Ip, vs.store.Port)
	vs.guard = security.NewGuard(nil, "", 0, "", 0)
	vs.fileSizeLimitBytes = 1 << 20

	for i, tc := range []struct {
		name      string
		fix       bool
		data      []byte
		rewritten bool
	}{
		{"rotated", true, tagged, true},
		{"unchanged jpeg", true, plain, false},
		{"not a jpeg", true, []byte("plain text"), false},
		{"fix off", false, tagged, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vs.FixJpgOrientation = tc.fix
			sum := md5.Sum(tc.data)
			clientMd5 := base64.StdEncoding.EncodeToString(sum[:])

			var body bytes.Buffer
			form := multipart.NewWriter(&body)
			part, err := form.CreatePart(textproto.MIMEHeader{
				"Content-Disposition": {`form-data; name="file"; filename="photo.jpg"`},
				"Content-Type":        {"image/jpeg"},
				"Content-Md5":         {clientMd5},
			})
			require.NoError(t, err)
			_, err = part.Write(tc.data)
			require.NoError(t, err)
			require.NoError(t, form.Close())
			fid := needle.NewFileId(vid, uint64(i+1), 0x1234)
			req := httptest.NewRequest(http.MethodPost, "/"+fid.String(), &body)
			req.Header.Set("Content-Type", form.FormDataContentType())

			resp := httptest.NewRecorder()
			vs.PostHandler(resp, req)
			var forwarded *httptest.ResponseRecorder
			select {
			case forwarded = <-replicaResp:
			default:
				t.Fatalf("primary did not forward the write: %d %s", resp.Code, resp.Body.String())
			}
			require.Equal(t, http.StatusCreated, forwarded.Code, forwarded.Body.String())
			require.Equal(t, http.StatusCreated, resp.Code, resp.Body.String())
			require.Equal(t, clientMd5, resp.Header().Get("Content-MD5"))
			if tc.rewritten {
				require.Empty(t, forwarded.Header().Get("Content-MD5"))
			} else {
				require.Equal(t, clientMd5, forwarded.Header().Get("Content-MD5"))
			}

			stored := func(s *storage.Store) []byte {
				n := &needle.Needle{Id: fid.Key, Cookie: fid.Cookie}
				_, err := s.ReadVolumeNeedle(vid, n, nil, nil)
				require.NoError(t, err)
				return n.Data
			}
			replicaData := stored(replicaVs.store)
			require.Equal(t, stored(vs.store), replicaData)
			require.Equal(t, tc.rewritten, !bytes.Equal(replicaData, tc.data))
		})
	}
}
