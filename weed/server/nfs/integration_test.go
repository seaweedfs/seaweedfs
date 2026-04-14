package nfs

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gonfs "github.com/willscott/go-nfs"
	nfsclient "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type fakeVolumeBlob struct {
	data            []byte
	contentEncoding string
}

type fakeVolumeServer struct {
	mu     sync.Mutex
	blobs  map[string]fakeVolumeBlob
	server *httptest.Server
}

type fakeVolumeControlPlane struct {
	filer_pb.UnimplementedSeaweedFilerServer

	mu      sync.Mutex
	host    string
	nextID  int
	assigns []*filer_pb.AssignVolumeRequest
	lookups []*filer_pb.LookupVolumeRequest
}

var initIntegrationHTTPClient sync.Once

func newFakeVolumeServer(t *testing.T) *fakeVolumeServer {
	t.Helper()

	fake := &fakeVolumeServer{
		blobs: make(map[string]fakeVolumeBlob),
	}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *fakeVolumeServer) host() string {
	return strings.TrimPrefix(f.server.URL, "http://")
}

func (f *fakeVolumeServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	fileID := strings.TrimPrefix(r.URL.Path, "/")
	if fileID == "" {
		http.NotFound(w, r)
		return
	}

	switch r.Method {
	case http.MethodPost:
		part, err := firstMultipartFile(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer part.Close()

		data, err := io.ReadAll(part)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		contentEncoding := part.Header.Get("Content-Encoding")
		sum := md5.Sum(data)

		f.mu.Lock()
		f.blobs[fileID] = fakeVolumeBlob{
			data:            bytes.Clone(data),
			contentEncoding: contentEncoding,
		}
		f.mu.Unlock()

		w.Header().Set("Content-MD5", base64.StdEncoding.EncodeToString(sum[:]))
		w.Header().Set("ETag", `"`+base64.StdEncoding.EncodeToString(sum[:])+`"`)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"name": path.Base(fileID),
			"size": len(data),
		})
	case http.MethodGet:
		f.mu.Lock()
		blob, found := f.blobs[fileID]
		f.mu.Unlock()
		if !found {
			http.NotFound(w, r)
			return
		}
		if blob.contentEncoding != "" {
			w.Header().Set("Content-Encoding", blob.contentEncoding)
		}
		http.ServeContent(w, r, fileID, time.Unix(0, 0), bytes.NewReader(blob.data))
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func firstMultipartFile(r *http.Request) (*multipart.Part, error) {
	reader, err := r.MultipartReader()
	if err != nil {
		return nil, err
	}

	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			return nil, io.ErrUnexpectedEOF
		}
		if err != nil {
			return nil, err
		}
		if part.FormName() == "file" {
			return part, nil
		}
		part.Close()
	}
}

func (f *fakeVolumeControlPlane) AssignVolume(_ context.Context, req *filer_pb.AssignVolumeRequest) (*filer_pb.AssignVolumeResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.assigns = append(f.assigns, req)
	f.nextID++
	fileID := fmt.Sprintf("7,%08x", f.nextID)
	return &filer_pb.AssignVolumeResponse{
		FileId: fileID,
		Count:  1,
		Location: &filer_pb.Location{
			Url: f.host,
		},
	}, nil
}

func (f *fakeVolumeControlPlane) LookupVolume(_ context.Context, req *filer_pb.LookupVolumeRequest) (*filer_pb.LookupVolumeResponse, error) {
	f.mu.Lock()
	f.lookups = append(f.lookups, req)
	f.mu.Unlock()

	locations := make(map[string]*filer_pb.Locations, len(req.GetVolumeIds()))
	for _, volumeID := range req.GetVolumeIds() {
		locations[volumeID] = &filer_pb.Locations{
			Locations: []*filer_pb.Location{
				{Url: f.host},
			},
		}
	}
	return &filer_pb.LookupVolumeResponse{LocationsMap: locations}, nil
}

func startFakeVolumeControlPlane(t *testing.T, controlPlane *fakeVolumeControlPlane) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(grpcServer, controlPlane)

	done := make(chan error, 1)
	go func() {
		done <- grpcServer.Serve(listener)
	}()

	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
		select {
		case err := <-done:
			if err != nil && !isClosedNetworkErr(err) {
				t.Errorf("fake control plane exited with error: %v", err)
			}
		case <-time.After(time.Second):
			t.Errorf("timed out waiting for fake control plane shutdown")
		}
	})

	return listener.Addr().String()
}

func mountTestTarget(t *testing.T, server *Server) (*nfsclient.Target, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	handler, err := server.newHandler()
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- gonfs.Serve(listener, handler)
	}()

	client, err := rpc.DialTCP(listener.Addr().Network(), listener.Addr().String(), false)
	require.NoError(t, err)

	mounter := &nfsclient.Mount{Client: client}
	target, err := mounter.Mount(string(server.exportRoot), rpc.AuthNull)
	require.NoError(t, err)

	cleanup := func() {
		_ = mounter.Unmount()
		client.Close()
		_ = listener.Close()

		select {
		case err := <-done:
			if err != nil && !isClosedNetworkErr(err) {
				t.Errorf("nfs server exited with error: %v", err)
			}
		case <-time.After(time.Second):
			t.Errorf("timed out waiting for nfs server shutdown")
		}
	}

	return target, cleanup
}

func isClosedNetworkErr(err error) bool {
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), "use of closed network connection") {
		return true
	}
	return strings.Contains(err.Error(), "listener closed")
}

func TestSeaweedNFSServesInlineRoundTripOverRPC(t *testing.T) {
	client := &fakeNFSFilerClient{
		kv: map[string][]byte{
			string(filer.InodeIndexKey(101)): testIndexRecord(t, 101, 1, "/exports"),
		},
		entries: map[util.FullPath]*filer_pb.Entry{
			"/exports": testEntry("exports", true, 101, uint32(0755), nil),
		},
	}

	server := newTestServer(t, "/exports", client)
	target, cleanup := mountTestTarget(t, server)
	defer cleanup()
	defer target.Close()

	_, err := target.Mkdir("/docs", 0o755)
	require.NoError(t, err)

	file, err := target.OpenFile("/docs/note.txt", 0o644)
	require.NoError(t, err)
	payload := []byte("hello over rpc")
	_, err = file.Write(payload)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	readFile, err := target.Open("/docs/note.txt")
	require.NoError(t, err)
	defer readFile.Close()

	data, err := io.ReadAll(readFile)
	require.NoError(t, err)
	assert.Equal(t, payload, data)

	entry := client.entries["/exports/docs/note.txt"]
	require.NotNil(t, entry)
	assert.Equal(t, payload, entry.Content)
	assert.Empty(t, entry.Chunks)

	_, beforeRenameHandle, err := target.Lookup("/docs/note.txt")
	require.NoError(t, err)

	entries, err := target.ReadDirPlus("/docs")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "note.txt", entries[0].Name())

	require.NoError(t, target.Rename("/docs/note.txt", "/docs/final.txt"))
	_, err = target.GetAttr(beforeRenameHandle)
	require.NoError(t, err)
	_, _, err = target.Lookup("/docs/final.txt")
	require.NoError(t, err)
	_, _, err = target.Lookup("/docs/note.txt")
	require.Error(t, err)

	require.NoError(t, target.Remove("/docs/final.txt"))
	_, _, err = target.Lookup("/docs/final.txt")
	require.Error(t, err)
}

func TestSeaweedNFSServesLargeChunkRoundTripOverRPC(t *testing.T) {
	initIntegrationHTTPClient.Do(util_http.InitGlobalHttpClient)

	client := &fakeNFSFilerClient{
		kv: map[string][]byte{
			string(filer.InodeIndexKey(101)): testIndexRecord(t, 101, 1, "/exports"),
		},
		entries: map[util.FullPath]*filer_pb.Entry{
			"/exports": testEntry("exports", true, 101, uint32(0755), nil),
		},
	}

	volumeServer := newFakeVolumeServer(t)
	controlPlane := &fakeVolumeControlPlane{host: volumeServer.host()}
	controlPlaneAddr := startFakeVolumeControlPlane(t, controlPlane)

	server := newTestServer(t, "/exports", client)
	server.withFilerClient = func(_ bool, fn func(filer_pb.SeaweedFilerClient) error) error {
		conn, err := grpc.NewClient(controlPlaneAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer conn.Close()
		return fn(filer_pb.NewSeaweedFilerClient(conn))
	}

	target, cleanup := mountTestTarget(t, server)
	defer cleanup()
	defer target.Close()

	payload := make([]byte, maxInlineWriteSize+4096)
	_, err := rand.New(rand.NewSource(1)).Read(payload)
	require.NoError(t, err)

	file, err := target.OpenFile("/big.bin", 0o644)
	require.NoError(t, err)
	_, err = file.Write(payload)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	entry := client.entries["/exports/big.bin"]
	require.NotNil(t, entry)
	require.Len(t, entry.GetChunks(), 1)
	assert.Nil(t, entry.Content)
	assert.Equal(t, uint64(len(payload)), entry.GetAttributes().GetFileSize())

	readFile, err := target.Open("/big.bin")
	require.NoError(t, err)
	defer readFile.Close()

	data, err := io.ReadAll(readFile)
	require.NoError(t, err)
	assert.Equal(t, payload, data)

	controlPlane.mu.Lock()
	defer controlPlane.mu.Unlock()
	require.Len(t, controlPlane.assigns, 1)
	assert.Equal(t, "/exports/big.bin", controlPlane.assigns[0].GetPath())
	assert.NotEmpty(t, controlPlane.lookups)
}

func TestSeaweedNFSRejectsStaleHandleAfterDeleteRecreate(t *testing.T) {
	client := &fakeNFSFilerClient{
		kv: map[string][]byte{
			string(filer.InodeIndexKey(101)): testIndexRecord(t, 101, 1, "/exports"),
		},
		entries: map[util.FullPath]*filer_pb.Entry{
			"/exports": testEntry("exports", true, 101, uint32(0755), nil),
		},
	}

	server := newTestServer(t, "/exports", client)
	target, cleanup := mountTestTarget(t, server)
	defer cleanup()
	defer target.Close()

	file, err := target.OpenFile("/stale.txt", 0o644)
	require.NoError(t, err)
	_, err = file.Write([]byte("old"))
	require.NoError(t, err)
	require.NoError(t, file.Close())

	_, oldHandle, err := target.Lookup("/stale.txt")
	require.NoError(t, err)

	require.NoError(t, target.Remove("/stale.txt"))

	file, err = target.OpenFile("/stale.txt", 0o644)
	require.NoError(t, err)
	_, err = file.Write([]byte("new"))
	require.NoError(t, err)
	require.NoError(t, file.Close())

	_, err = target.GetAttr(oldHandle)
	require.Error(t, err)
	nfsErr, ok := err.(*nfsclient.Error)
	require.True(t, ok)
	assert.Equal(t, uint32(nfsclient.NFS3ErrStale), nfsErr.ErrorNum)

	_, newHandle, err := target.Lookup("/stale.txt")
	require.NoError(t, err)
	_, err = target.GetAttr(newHandle)
	require.NoError(t, err)
}
