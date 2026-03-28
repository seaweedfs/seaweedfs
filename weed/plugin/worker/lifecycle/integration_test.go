package lifecycle

import (
	"context"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// testFilerServer is an in-memory filer gRPC server for integration tests.
type testFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu      sync.RWMutex
	entries map[string]*filer_pb.Entry // key: "dir\x00name"
}

func newTestFilerServer() *testFilerServer {
	return &testFilerServer{entries: make(map[string]*filer_pb.Entry)}
}

func (s *testFilerServer) key(dir, name string) string { return dir + "\x00" + name }

func (s *testFilerServer) splitKey(key string) (string, string) {
	for i := range key {
		if key[i] == '\x00' {
			return key[:i], key[i+1:]
		}
	}
	return key, ""
}

func (s *testFilerServer) putEntry(dir string, entry *filer_pb.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[s.key(dir, entry.Name)] = proto.Clone(entry).(*filer_pb.Entry)
}

func (s *testFilerServer) getEntry(dir, name string) *filer_pb.Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e := s.entries[s.key(dir, name)]
	if e == nil {
		return nil
	}
	return proto.Clone(e).(*filer_pb.Entry)
}

func (s *testFilerServer) hasEntry(dir, name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.entries[s.key(dir, name)]
	return ok
}

func (s *testFilerServer) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, found := s.entries[s.key(req.Directory, req.Name)]
	if !found {
		return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: proto.Clone(entry).(*filer_pb.Entry)}, nil
}

func (s *testFilerServer) ListEntries(req *filer_pb.ListEntriesRequest, stream grpc.ServerStreamingServer[filer_pb.ListEntriesResponse]) error {
	// Snapshot entries under lock, then stream without holding the lock
	// (streaming callbacks may trigger DeleteEntry which needs a write lock).
	s.mu.RLock()
	var names []string
	for key := range s.entries {
		dir, name := s.splitKey(key)
		if dir == req.Directory {
			if req.StartFromFileName != "" && name <= req.StartFromFileName {
				continue
			}
			if req.Prefix != "" && !strings.HasPrefix(name, req.Prefix) {
				continue
			}
			names = append(names, name)
		}
	}
	sort.Strings(names)

	// Clone entries while still holding the lock.
	type namedEntry struct {
		name  string
		entry *filer_pb.Entry
	}
	snapshot := make([]namedEntry, 0, len(names))
	for _, name := range names {
		if req.Limit > 0 && uint32(len(snapshot)) >= req.Limit {
			break
		}
		snapshot = append(snapshot, namedEntry{
			name:  name,
			entry: proto.Clone(s.entries[s.key(req.Directory, name)]).(*filer_pb.Entry),
		})
	}
	s.mu.RUnlock()

	// Stream responses without holding any lock.
	for _, ne := range snapshot {
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: ne.entry}); err != nil {
			return err
		}
	}
	return nil
}

func (s *testFilerServer) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[s.key(req.Directory, req.Entry.Name)] = proto.Clone(req.Entry).(*filer_pb.Entry)
	return &filer_pb.CreateEntryResponse{}, nil
}

func (s *testFilerServer) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := s.key(req.Directory, req.Name)
	if _, found := s.entries[k]; !found {
		return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
	}
	delete(s.entries, k)
	if req.IsRecursive {
		// Delete all descendants: any entry whose directory starts with
		// the deleted path (handles nested subdirectories).
		deletedPath := req.Directory + "/" + req.Name
		for key := range s.entries {
			dir, _ := s.splitKey(key)
			if dir == deletedPath || strings.HasPrefix(dir, deletedPath+"/") {
				delete(s.entries, key)
			}
		}
	}
	return &filer_pb.DeleteEntryResponse{}, nil
}

// startTestFiler starts an in-memory filer gRPC server and returns a client.
func startTestFiler(t *testing.T) (*testFilerServer, filer_pb.SeaweedFilerClient) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	server := newTestFilerServer()
	grpcServer := pb.NewGrpcServer()
	filer_pb.RegisterSeaweedFilerServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()

	t.Cleanup(func() {
		grpcServer.Stop()
		_ = lis.Close()
	})

	host, portStr, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}
	addr := pb.NewServerAddress(host, 1, port)

	conn, err := pb.GrpcDial(context.Background(), addr.ToGrpcAddress(), false, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return server, filer_pb.NewSeaweedFilerClient(conn)
}

// Helper to create a version ID from a timestamp.
func testVersionId(ts time.Time) string {
	inverted := math.MaxInt64 - ts.UnixNano()
	return fmt.Sprintf("%016x", inverted) + "0000000000000000"
}

func TestIntegration_ListExpiredObjectsByRules(t *testing.T) {
	server, client := startTestFiler(t)
	bucketsPath := "/buckets"
	bucket := "test-bucket"
	bucketDir := bucketsPath + "/" + bucket

	now := time.Now()
	old := now.Add(-60 * 24 * time.Hour) // 60 days ago
	recent := now.Add(-5 * 24 * time.Hour) // 5 days ago

	// Create bucket directory.
	server.putEntry(bucketsPath, &filer_pb.Entry{Name: bucket, IsDirectory: true})

	// Create objects.
	server.putEntry(bucketDir, &filer_pb.Entry{
		Name:       "old-file.txt",
		Attributes: &filer_pb.FuseAttributes{Mtime: old.Unix(), FileSize: 1024},
	})
	server.putEntry(bucketDir, &filer_pb.Entry{
		Name:       "recent-file.txt",
		Attributes: &filer_pb.FuseAttributes{Mtime: recent.Unix(), FileSize: 1024},
	})

	rules := []s3lifecycle.Rule{{
		ID: "expire-30d", Status: "Enabled",
		ExpirationDays: 30,
	}}

	expired, scanned, err := listExpiredObjectsByRules(context.Background(), client, bucketsPath, bucket, rules, 100)
	if err != nil {
		t.Fatalf("listExpiredObjectsByRules: %v", err)
	}

	if scanned != 2 {
		t.Errorf("expected 2 scanned, got %d", scanned)
	}
	if len(expired) != 1 {
		t.Fatalf("expected 1 expired, got %d", len(expired))
	}
	if expired[0].name != "old-file.txt" {
		t.Errorf("expected old-file.txt expired, got %s", expired[0].name)
	}
}

func TestIntegration_ListExpiredObjectsByRules_TagFilter(t *testing.T) {
	server, client := startTestFiler(t)
	bucketsPath := "/buckets"
	bucket := "tag-bucket"
	bucketDir := bucketsPath + "/" + bucket

	old := time.Now().Add(-60 * 24 * time.Hour)

	server.putEntry(bucketsPath, &filer_pb.Entry{Name: bucket, IsDirectory: true})

	// Object with matching tag.
	server.putEntry(bucketDir, &filer_pb.Entry{
		Name:       "tagged.txt",
		Attributes: &filer_pb.FuseAttributes{Mtime: old.Unix(), FileSize: 100},
		Extended:   map[string][]byte{"X-Amz-Tagging-env": []byte("dev")},
	})
	// Object without tag.
	server.putEntry(bucketDir, &filer_pb.Entry{
		Name:       "untagged.txt",
		Attributes: &filer_pb.FuseAttributes{Mtime: old.Unix(), FileSize: 100},
	})

	rules := []s3lifecycle.Rule{{
		ID: "tag-expire", Status: "Enabled",
		ExpirationDays: 30,
		FilterTags:     map[string]string{"env": "dev"},
	}}

	expired, _, err := listExpiredObjectsByRules(context.Background(), client, bucketsPath, bucket, rules, 100)
	if err != nil {
		t.Fatalf("listExpiredObjectsByRules: %v", err)
	}

	if len(expired) != 1 {
		t.Fatalf("expected 1 expired (tagged only), got %d", len(expired))
	}
	if expired[0].name != "tagged.txt" {
		t.Errorf("expected tagged.txt, got %s", expired[0].name)
	}
}

func TestIntegration_ProcessVersionsDirectory(t *testing.T) {
	server, client := startTestFiler(t)
	bucketsPath := "/buckets"
	bucket := "versioned-bucket"
	bucketDir := bucketsPath + "/" + bucket
	versionsDir := bucketDir + "/key.versions"

	now := time.Now()
	t1 := now.Add(-90 * 24 * time.Hour) // oldest
	t2 := now.Add(-60 * 24 * time.Hour)
	t3 := now.Add(-1 * 24 * time.Hour)  // newest (latest)

	vid1 := testVersionId(t1)
	vid2 := testVersionId(t2)
	vid3 := testVersionId(t3)

	server.putEntry(bucketsPath, &filer_pb.Entry{Name: bucket, IsDirectory: true})
	server.putEntry(bucketDir, &filer_pb.Entry{
		Name: "key.versions", IsDirectory: true,
		Extended: map[string][]byte{
			s3_constants.ExtLatestVersionIdKey: []byte(vid3),
		},
	})

	// Three versions: vid3 (latest), vid2 (noncurrent), vid1 (noncurrent)
	server.putEntry(versionsDir, &filer_pb.Entry{
		Name:       "v_" + vid3,
		Attributes: &filer_pb.FuseAttributes{Mtime: t3.Unix(), FileSize: 100},
		Extended: map[string][]byte{
			s3_constants.ExtVersionIdKey: []byte(vid3),
		},
	})
	server.putEntry(versionsDir, &filer_pb.Entry{
		Name:       "v_" + vid2,
		Attributes: &filer_pb.FuseAttributes{Mtime: t2.Unix(), FileSize: 100},
		Extended: map[string][]byte{
			s3_constants.ExtVersionIdKey: []byte(vid2),
		},
	})
	server.putEntry(versionsDir, &filer_pb.Entry{
		Name:       "v_" + vid1,
		Attributes: &filer_pb.FuseAttributes{Mtime: t1.Unix(), FileSize: 100},
		Extended: map[string][]byte{
			s3_constants.ExtVersionIdKey: []byte(vid1),
		},
	})

	rules := []s3lifecycle.Rule{{
		ID: "noncurrent-30d", Status: "Enabled",
		NoncurrentVersionExpirationDays: 30,
	}}

	expired, scanned, err := processVersionsDirectory(
		context.Background(), client, versionsDir, bucketDir,
		rules, now, false, 100,
	)
	if err != nil {
		t.Fatalf("processVersionsDirectory: %v", err)
	}

	// vid3 is latest (not expired). vid2 became noncurrent when vid3 was created
	// (1 day ago), so vid2 is NOT old enough (< 30 days noncurrent).
	// vid1 became noncurrent when vid2 was created (60 days ago), so vid1 IS expired.
	if scanned != 2 {
		t.Errorf("expected 2 scanned (non-current versions), got %d", scanned)
	}
	if len(expired) != 1 {
		t.Fatalf("expected 1 expired (only vid1), got %d", len(expired))
	}
	if expired[0].name != "v_"+vid1 {
		t.Errorf("expected v_%s expired, got %s", vid1, expired[0].name)
	}
}

func TestIntegration_ProcessVersionsDirectory_NewerNoncurrentVersions(t *testing.T) {
	server, client := startTestFiler(t)
	bucketsPath := "/buckets"
	bucket := "keep-n-bucket"
	bucketDir := bucketsPath + "/" + bucket
	versionsDir := bucketDir + "/obj.versions"

	now := time.Now()
	// Create 5 versions, all old enough to expire by days alone.
	versions := make([]time.Time, 5)
	vids := make([]string, 5)
	for i := 0; i < 5; i++ {
		versions[i] = now.Add(time.Duration(-(90 - i*10)) * 24 * time.Hour)
		vids[i] = testVersionId(versions[i])
	}
	// vids[4] is newest (latest), vids[0] is oldest

	server.putEntry(bucketsPath, &filer_pb.Entry{Name: bucket, IsDirectory: true})
	server.putEntry(bucketDir, &filer_pb.Entry{
		Name: "obj.versions", IsDirectory: true,
		Extended: map[string][]byte{
			s3_constants.ExtLatestVersionIdKey: []byte(vids[4]),
		},
	})

	for i, vid := range vids {
		server.putEntry(versionsDir, &filer_pb.Entry{
			Name:       "v_" + vid,
			Attributes: &filer_pb.FuseAttributes{Mtime: versions[i].Unix(), FileSize: 100},
			Extended: map[string][]byte{
				s3_constants.ExtVersionIdKey: []byte(vid),
			},
		})
	}

	rules := []s3lifecycle.Rule{{
		ID: "keep-2", Status: "Enabled",
		NoncurrentVersionExpirationDays: 7,
		NewerNoncurrentVersions:         2,
	}}

	expired, _, err := processVersionsDirectory(
		context.Background(), client, versionsDir, bucketDir,
		rules, now, false, 100,
	)
	if err != nil {
		t.Fatalf("processVersionsDirectory: %v", err)
	}

	// 4 noncurrent versions (vids[0..3]). Keep newest 2 (vids[3], vids[2]).
	// Expire vids[1] and vids[0].
	if len(expired) != 2 {
		t.Fatalf("expected 2 expired (keep 2 newest noncurrent), got %d", len(expired))
	}
	expiredNames := map[string]bool{}
	for _, e := range expired {
		expiredNames[e.name] = true
	}
	if !expiredNames["v_"+vids[0]] {
		t.Errorf("expected vids[0] (oldest) to be expired")
	}
	if !expiredNames["v_"+vids[1]] {
		t.Errorf("expected vids[1] to be expired")
	}
}

func TestIntegration_AbortMPUsByRules(t *testing.T) {
	server, client := startTestFiler(t)
	bucketsPath := "/buckets"
	bucket := "mpu-bucket"
	uploadsDir := bucketsPath + "/" + bucket + "/.uploads"

	now := time.Now()
	old := now.Add(-10 * 24 * time.Hour)
	recent := now.Add(-2 * 24 * time.Hour)

	server.putEntry(bucketsPath, &filer_pb.Entry{Name: bucket, IsDirectory: true})
	server.putEntry(bucketsPath+"/"+bucket, &filer_pb.Entry{Name: ".uploads", IsDirectory: true})

	// Old upload under logs/ prefix.
	server.putEntry(uploadsDir, &filer_pb.Entry{
		Name: "logs_upload1", IsDirectory: true,
		Attributes: &filer_pb.FuseAttributes{Crtime: old.Unix()},
	})
	// Recent upload under logs/ prefix.
	server.putEntry(uploadsDir, &filer_pb.Entry{
		Name: "logs_upload2", IsDirectory: true,
		Attributes: &filer_pb.FuseAttributes{Crtime: recent.Unix()},
	})
	// Old upload under data/ prefix (should not match logs/ rule).
	server.putEntry(uploadsDir, &filer_pb.Entry{
		Name: "data_upload1", IsDirectory: true,
		Attributes: &filer_pb.FuseAttributes{Crtime: old.Unix()},
	})

	rules := []s3lifecycle.Rule{{
		ID: "abort-logs", Status: "Enabled",
		Prefix:                      "logs",
		AbortMPUDaysAfterInitiation: 7,
	}}

	aborted, errs, err := abortMPUsByRules(context.Background(), client, bucketsPath, bucket, rules, 100)
	if err != nil {
		t.Fatalf("abortMPUsByRules: %v", err)
	}
	if errs != 0 {
		t.Errorf("expected 0 errors, got %d", errs)
	}

	// Only logs_upload1 should be aborted (old + matches prefix).
	// logs_upload2 is too recent, data_upload1 doesn't match prefix.
	if aborted != 1 {
		t.Errorf("expected 1 aborted, got %d", aborted)
	}

	// Verify the correct upload was removed.
	if server.hasEntry(uploadsDir, "logs_upload1") {
		t.Error("logs_upload1 should have been removed")
	}
	if !server.hasEntry(uploadsDir, "logs_upload2") {
		t.Error("logs_upload2 should still exist")
	}
	if !server.hasEntry(uploadsDir, "data_upload1") {
		t.Error("data_upload1 should still exist (wrong prefix)")
	}
}

func TestIntegration_DeleteExpiredObjects(t *testing.T) {
	server, client := startTestFiler(t)
	bucketsPath := "/buckets"
	bucket := "delete-bucket"
	bucketDir := bucketsPath + "/" + bucket

	now := time.Now()
	old := now.Add(-60 * 24 * time.Hour)

	server.putEntry(bucketsPath, &filer_pb.Entry{Name: bucket, IsDirectory: true})
	server.putEntry(bucketDir, &filer_pb.Entry{
		Name:       "to-delete.txt",
		Attributes: &filer_pb.FuseAttributes{Mtime: old.Unix(), FileSize: 100},
	})
	server.putEntry(bucketDir, &filer_pb.Entry{
		Name:       "to-keep.txt",
		Attributes: &filer_pb.FuseAttributes{Mtime: now.Unix(), FileSize: 100},
	})

	rules := []s3lifecycle.Rule{{
		ID: "expire", Status: "Enabled",
		ExpirationDays: 30,
	}}

	expired, _, err := listExpiredObjectsByRules(context.Background(), client, bucketsPath, bucket, rules, 100)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	// Actually delete them.
	deleted, errs, err := deleteExpiredObjects(context.Background(), client, expired)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if deleted != 1 || errs != 0 {
		t.Errorf("expected 1 deleted 0 errors, got %d deleted %d errors", deleted, errs)
	}

	if server.hasEntry(bucketDir, "to-delete.txt") {
		t.Error("to-delete.txt should have been removed")
	}
	if !server.hasEntry(bucketDir, "to-keep.txt") {
		t.Error("to-keep.txt should still exist")
	}
}
