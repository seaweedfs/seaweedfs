package command

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

type copyTestFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	volumeHost string
	nextKey    atomic.Int32
	mu         sync.Mutex
	created    *filer_pb.Entry
}

func (f *copyTestFiler) AssignVolume(ctx context.Context, req *filer_pb.AssignVolumeRequest) (*filer_pb.AssignVolumeResponse, error) {
	return &filer_pb.AssignVolumeResponse{
		FileId:   fmt.Sprintf("1,%x637037d6", f.nextKey.Add(1)),
		Location: &filer_pb.Location{Url: f.volumeHost, PublicUrl: f.volumeHost},
	}, nil
}

func (f *copyTestFiler) CreateEntry(ctx context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = req.Entry
	return &filer_pb.CreateEntryResponse{}, nil
}

func TestUploadFileInChunksStoresChunksInOffsetOrder(t *testing.T) {
	// The first chunk finishes last, and the server echoes Content-MD5 back
	// the way a volume server does.
	volume := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		file, header, err := r.FormFile("file")
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		data, _ := io.ReadAll(file)
		if strings.HasSuffix(header.Filename, "-1") {
			time.Sleep(200 * time.Millisecond)
		}
		w.Header().Set("Content-MD5", header.Header.Get("Content-MD5"))
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{"name": header.Filename, "size": len(data)})
	}))
	defer volume.Close()

	fake := &copyTestFiler{volumeHost: strings.TrimPrefix(volume.URL, "http://")}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(srv, fake)
	go srv.Serve(lis)
	defer srv.Stop()

	const chunkSize, chunkCount = 64 * 1024, 3
	data := make([]byte, chunkSize*chunkCount)
	rand.Read(data)
	path := filepath.Join(t.TempDir(), "f.bin")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	empty, access, concurrent := "", "url", chunkCount
	worker := &FileCopyWorker{
		options: &CopyOptions{
			replication:        &empty,
			collection:         &empty,
			diskType:           &empty,
			concurrentChunks:   &concurrent,
			grpcDialOption:     grpc.WithTransportCredentials(insecure.NewCredentials()),
			volumeServerAccess: &access,
		},
		filerAddress: pb.ServerAddress(fmt.Sprintf("127.0.0.1:1.%d", lis.Addr().(*net.TCPAddr).Port)),
	}
	task := FileCopyTask{destinationUrlPath: "/dir/", fileSize: int64(len(data)), fileMode: 0o644}
	if err := worker.uploadFileInChunks(task, f, chunkCount, chunkSize); err != nil {
		t.Fatal(err)
	}

	fake.mu.Lock()
	chunks := fake.created.GetChunks()
	fake.mu.Unlock()
	if len(chunks) != chunkCount {
		t.Fatalf("stored %d chunks, want %d", len(chunks), chunkCount)
	}
	for i, c := range chunks {
		if want := int64(i * chunkSize); c.Offset != want {
			t.Fatalf("chunk %d is at offset %d, want %d", i, c.Offset, want)
		}
		sum := md5.Sum(data[i*chunkSize : (i+1)*chunkSize])
		if want := base64.StdEncoding.EncodeToString(sum[:]); c.ETag != want {
			t.Fatalf("chunk %d ETag = %q, want the md5 of its data %q", i, c.ETag, want)
		}
	}
}
