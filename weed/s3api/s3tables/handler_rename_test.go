package s3tables

import (
	"context"
	"encoding/json"
	"net"
	"sort"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// memFilerServer is an in-memory filer used to drive Manager operations
// end-to-end without a live cluster.
type memFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	entries map[string]map[string]*filer_pb.Entry // dir -> name -> entry
	client  filer_pb.SeaweedFilerClient
}

func newMemFilerServer() *memFilerServer {
	return &memFilerServer{entries: make(map[string]map[string]*filer_pb.Entry)}
}

func (f *memFilerServer) getEntry(dir, name string) *filer_pb.Entry {
	if d, ok := f.entries[dir]; ok {
		return d[name]
	}
	return nil
}

func (f *memFilerServer) putEntry(dir, name string, extended map[string][]byte) {
	if _, ok := f.entries[dir]; !ok {
		f.entries[dir] = make(map[string]*filer_pb.Entry)
	}
	f.entries[dir][name] = &filer_pb.Entry{Name: name, IsDirectory: true, Extended: extended}
}

func (f *memFilerServer) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if e := f.getEntry(req.Directory, req.Name); e != nil {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: e}, nil
	}
	// Carry the sentinel text so filer_pb.LookupEntry maps it to ErrNotFound.
	return nil, status.Errorf(codes.NotFound, "%s: %s/%s", filer_pb.ErrNotFound.Error(), req.Directory, req.Name)
}

func (f *memFilerServer) ListEntries(req *filer_pb.ListEntriesRequest, stream grpc.ServerStreamingServer[filer_pb.ListEntriesResponse]) error {
	d, ok := f.entries[req.Directory]
	if !ok {
		return nil
	}
	names := make([]string, 0, len(d))
	for name := range d {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: d[name]}); err != nil {
			return err
		}
	}
	return nil
}

func (f *memFilerServer) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	if _, ok := f.entries[req.Directory]; !ok {
		f.entries[req.Directory] = make(map[string]*filer_pb.Entry)
	}
	f.entries[req.Directory][req.Entry.Name] = req.Entry
	return &filer_pb.CreateEntryResponse{}, nil
}

func (f *memFilerServer) UpdateEntry(_ context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	if _, ok := f.entries[req.Directory]; !ok {
		f.entries[req.Directory] = make(map[string]*filer_pb.Entry)
	}
	f.entries[req.Directory][req.Entry.Name] = req.Entry
	return &filer_pb.UpdateEntryResponse{}, nil
}

func (f *memFilerServer) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	if d, ok := f.entries[req.Directory]; ok {
		delete(d, req.Name)
	}
	return &filer_pb.DeleteEntryResponse{}, nil
}

func (f *memFilerServer) Ping(_ context.Context, _ *filer_pb.PingRequest) (*filer_pb.PingResponse, error) {
	now := time.Now().UnixNano()
	return &filer_pb.PingResponse{StartTimeNs: now, RemoteTimeNs: now, StopTimeNs: now}, nil
}

func startMemFiler(t *testing.T) *memFilerServer {
	t.Helper()
	fs := newMemFilerServer()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	filer_pb.RegisterSeaweedFilerServer(server, fs)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.GracefulStop)

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	fs.client = filer_pb.NewSeaweedFilerClient(conn)
	deadline := time.Now().Add(5 * time.Second)
	for {
		pingCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_, err := fs.client.Ping(pingCtx, &filer_pb.PingRequest{})
		cancel()
		if err == nil {
			break
		}
		require.False(t, time.Now().After(deadline), "filer not ready: %v", err)
		time.Sleep(10 * time.Millisecond)
	}
	return fs
}

const renameTestBucket = "renamebkt"

func mustBucketARN(t *testing.T) string {
	t.Helper()
	arn, err := BuildBucketARN(DefaultRegion, DefaultAccountID, renameTestBucket)
	require.NoError(t, err)
	return arn
}

// startRenameManager seeds a bucket/namespace/table and returns a trusted Manager.
func startRenameManager(t *testing.T) (*memFilerServer, *Manager) {
	t.Helper()
	fs := startMemFiler(t)

	bucketMeta, _ := json.Marshal(tableBucketMetadata{Name: renameTestBucket, OwnerAccountID: DefaultAccountID})
	fs.putEntry(TablesPath, renameTestBucket, map[string][]byte{
		ExtendedKeyTableBucket: []byte("{}"),
		ExtendedKeyMetadata:    bucketMeta,
	})

	nsMeta, _ := json.Marshal(namespaceMetadata{Namespace: []string{"ns"}, OwnerAccountID: DefaultAccountID})
	fs.putEntry(GetTableBucketPath(renameTestBucket), "ns", map[string][]byte{ExtendedKeyMetadata: nsMeta})

	tableMeta, _ := json.Marshal(tableMetadataInternal{
		Name:             "t",
		Namespace:        "ns",
		Format:           "ICEBERG",
		OwnerAccountID:   DefaultAccountID,
		MetadataVersion:  3,
		MetadataLocation: "s3://" + renameTestBucket + "/ns/t/metadata/v3.metadata.json",
	})
	fs.putEntry(GetNamespacePath(renameTestBucket, "ns"), "t", map[string][]byte{
		ExtendedKeyMetadata:        tableMeta,
		ExtendedKeyMetadataVersion: []byte("3"),
	})

	m := NewManager()
	m.SetTrusted(true)
	return fs, m
}

func runRename(t *testing.T, m *Manager, fs *memFilerServer, req *RenameTableRequest) error {
	t.Helper()
	return m.Execute(context.Background(), NewManagerClient(fs.client), "RenameTable", req, nil, "")
}

func TestRenameTableMovesPointer(t *testing.T) {
	fs, m := startRenameManager(t)

	req := &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "t",
		DestNamespace:   []string{"ns"},
		DestName:        "t2",
	}
	require.NoError(t, runRename(t, m, fs, req))

	assert.Nil(t, fs.getEntry(GetNamespacePath(renameTestBucket, "ns"), "t"), "source entry should be removed")

	dest := fs.getEntry(GetNamespacePath(renameTestBucket, "ns"), "t2")
	require.NotNil(t, dest)
	var meta tableMetadataInternal
	require.NoError(t, json.Unmarshal(dest.Extended[ExtendedKeyMetadata], &meta))
	assert.Equal(t, "t2", meta.Name)
	assert.Equal(t, "ns", meta.Namespace)
	assert.Equal(t, "s3://"+renameTestBucket+"/ns/t/metadata/v3.metadata.json", meta.MetadataLocation)
	assert.Equal(t, []byte("3"), dest.Extended[ExtendedKeyMetadataVersion])
}

func TestRenameTableSourceMissing(t *testing.T) {
	fs, m := startRenameManager(t)
	err := runRename(t, m, fs, &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "ghost",
		DestNamespace:   []string{"ns"},
		DestName:        "t2",
	})
	require.Error(t, err)
	var s3Err *S3TablesError
	require.ErrorAs(t, err, &s3Err)
	assert.Equal(t, ErrCodeNoSuchTable, s3Err.Type)
}

func TestRenameTableDestExists(t *testing.T) {
	fs, m := startRenameManager(t)
	existing, _ := json.Marshal(tableMetadataInternal{Name: "t2", Namespace: "ns", OwnerAccountID: DefaultAccountID})
	fs.putEntry(GetNamespacePath(renameTestBucket, "ns"), "t2", map[string][]byte{ExtendedKeyMetadata: existing})

	err := runRename(t, m, fs, &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "t",
		DestNamespace:   []string{"ns"},
		DestName:        "t2",
	})
	require.Error(t, err)
	var s3Err *S3TablesError
	require.ErrorAs(t, err, &s3Err)
	assert.Equal(t, ErrCodeTableAlreadyExists, s3Err.Type)
	assert.NotNil(t, fs.getEntry(GetNamespacePath(renameTestBucket, "ns"), "t"), "source must be untouched on conflict")
}

func TestRenameTableDestNamespaceMissing(t *testing.T) {
	fs, m := startRenameManager(t)
	err := runRename(t, m, fs, &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "t",
		DestNamespace:   []string{"other"},
		DestName:        "t2",
	})
	require.Error(t, err)
	var s3Err *S3TablesError
	require.ErrorAs(t, err, &s3Err)
	assert.Equal(t, ErrCodeNoSuchNamespace, s3Err.Type)
	assert.NotNil(t, fs.getEntry(GetNamespacePath(renameTestBucket, "ns"), "t"), "source must be untouched")
}

func TestRenameTableInvalidName(t *testing.T) {
	fs, m := startRenameManager(t)
	err := runRename(t, m, fs, &RenameTableRequest{
		TableBucketARN:  mustBucketARN(t),
		SourceNamespace: []string{"ns"},
		SourceName:      "t",
		DestNamespace:   []string{"ns"},
		DestName:        "Bad/Name",
	})
	require.Error(t, err)
	var s3Err *S3TablesError
	require.ErrorAs(t, err, &s3Err)
	assert.Equal(t, ErrCodeInvalidRequest, s3Err.Type)
}
