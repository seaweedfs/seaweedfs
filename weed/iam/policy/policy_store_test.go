package policy

import (
	"context"
	"encoding/json"
	"net"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type policyStoreTestFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu      sync.RWMutex
	entries map[string]*filer_pb.Entry
}

func newPolicyStoreTestFilerServer() *policyStoreTestFilerServer {
	return &policyStoreTestFilerServer{
		entries: make(map[string]*filer_pb.Entry),
	}
}

func (s *policyStoreTestFilerServer) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, found := s.entries[policyStoreTestEntryKey(req.Directory, req.Name)]
	if !found {
		return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
	}

	return &filer_pb.LookupDirectoryEntryResponse{Entry: clonePolicyStoreEntry(entry)}, nil
}

func (s *policyStoreTestFilerServer) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := policyStoreTestEntryKey(req.Directory, req.Entry.Name)
	if _, found := s.entries[key]; found {
		return nil, status.Error(codes.AlreadyExists, "entry already exists")
	}

	s.entries[key] = clonePolicyStoreEntry(req.Entry)
	return &filer_pb.CreateEntryResponse{}, nil
}

func (s *policyStoreTestFilerServer) UpdateEntry(_ context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := policyStoreTestEntryKey(req.Directory, req.Entry.Name)
	if _, found := s.entries[key]; !found {
		return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
	}

	s.entries[key] = clonePolicyStoreEntry(req.Entry)
	return &filer_pb.UpdateEntryResponse{}, nil
}

func (s *policyStoreTestFilerServer) ListEntries(req *filer_pb.ListEntriesRequest, stream grpc.ServerStreamingServer[filer_pb.ListEntriesResponse]) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]string, 0)
	for key := range s.entries {
		dir, name := splitPolicyStoreEntryKey(key)
		if dir != req.Directory {
			continue
		}
		if req.Prefix != "" && len(name) >= len(req.Prefix) && name[:len(req.Prefix)] != req.Prefix {
			continue
		}
		if req.Prefix != "" && len(name) < len(req.Prefix) {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		if err := stream.Send(&filer_pb.ListEntriesResponse{
			Entry: clonePolicyStoreEntry(s.entries[policyStoreTestEntryKey(req.Directory, name)]),
		}); err != nil {
			return err
		}
	}

	return nil
}

func (s *policyStoreTestFilerServer) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := policyStoreTestEntryKey(req.Directory, req.Name)
	if _, found := s.entries[key]; !found {
		return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
	}

	delete(s.entries, key)
	return &filer_pb.DeleteEntryResponse{}, nil
}

func (s *policyStoreTestFilerServer) putPolicyFile(t *testing.T, dir string, name string, document *PolicyDocument) {
	t.Helper()

	content, err := json.Marshal(document)
	require.NoError(t, err)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[policyStoreTestEntryKey(dir, name)] = &filer_pb.Entry{
		Name:    name,
		Content: content,
	}
}

func (s *policyStoreTestFilerServer) hasEntry(dir string, name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, found := s.entries[policyStoreTestEntryKey(dir, name)]
	return found
}

func newTestFilerPolicyStore(t *testing.T) (*FilerPolicyStore, *policyStoreTestFilerServer) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := newPolicyStoreTestFilerServer()
	grpcServer := pb.NewGrpcServer()
	filer_pb.RegisterSeaweedFilerServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(lis)
	}()

	t.Cleanup(func() {
		grpcServer.Stop()
		_ = lis.Close()
	})

	host, portString, err := net.SplitHostPort(lis.Addr().String())
	require.NoError(t, err)
	grpcPort, err := strconv.Atoi(portString)
	require.NoError(t, err)

	store, err := NewFilerPolicyStore(nil, func() string {
		return string(pb.NewServerAddress(host, 1, grpcPort))
	})
	require.NoError(t, err)
	store.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())

	return store, server
}

func TestFilerPolicyStoreGetPolicyPrefersCanonicalFiles(t *testing.T) {
	ctx := context.Background()
	store, server := newTestFilerPolicyStore(t)

	server.putPolicyFile(t, store.basePath, "cli-bucket-access-policy.json", testPolicyDocument("s3:ListBucket", "arn:aws:s3:::cli-allowed-bucket"))
	server.putPolicyFile(t, store.basePath, "policy_cli-bucket-access-policy.json", testPolicyDocument("s3:PutObject", "arn:aws:s3:::cli-forbidden-bucket/*"))

	document, err := store.GetPolicy(ctx, "", "cli-bucket-access-policy")
	require.NoError(t, err)
	require.Len(t, document.Statement, 1)
	assert.Equal(t, "s3:ListBucket", document.Statement[0].Action[0])
	assert.Equal(t, "arn:aws:s3:::cli-allowed-bucket", document.Statement[0].Resource[0])
}

func TestFilerPolicyStoreListPoliciesIncludesCanonicalAndLegacyFiles(t *testing.T) {
	ctx := context.Background()
	store, server := newTestFilerPolicyStore(t)

	server.putPolicyFile(t, store.basePath, "canonical-only.json", testPolicyDocument("s3:GetObject", "arn:aws:s3:::canonical-only/*"))
	server.putPolicyFile(t, store.basePath, "policy_legacy-only.json", testPolicyDocument("s3:PutObject", "arn:aws:s3:::legacy-only/*"))
	server.putPolicyFile(t, store.basePath, "shared.json", testPolicyDocument("s3:DeleteObject", "arn:aws:s3:::shared/*"))
	server.putPolicyFile(t, store.basePath, "policy_shared.json", testPolicyDocument("s3:ListBucket", "arn:aws:s3:::shared"))
	server.putPolicyFile(t, store.basePath, "policy_invalid:name.json", testPolicyDocument("s3:GetObject", "arn:aws:s3:::ignored/*"))
	server.putPolicyFile(t, store.basePath, "bucket-policy:bucket-a.json", testPolicyDocument("s3:ListBucket", "arn:aws:s3:::bucket-a"))

	names, err := store.ListPolicies(ctx, "")
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"canonical-only", "legacy-only", "shared", "bucket-policy:bucket-a"}, names)
}

func TestFilerPolicyStoreDeletePolicyRemovesCanonicalAndLegacyFiles(t *testing.T) {
	ctx := context.Background()
	store, server := newTestFilerPolicyStore(t)

	server.putPolicyFile(t, store.basePath, "dual-format.json", testPolicyDocument("s3:GetObject", "arn:aws:s3:::dual-format/*"))
	server.putPolicyFile(t, store.basePath, "policy_dual-format.json", testPolicyDocument("s3:PutObject", "arn:aws:s3:::dual-format/*"))

	require.NoError(t, store.DeletePolicy(ctx, "", "dual-format"))
	assert.False(t, server.hasEntry(store.basePath, "dual-format.json"))
	assert.False(t, server.hasEntry(store.basePath, "policy_dual-format.json"))
}

func TestFilerPolicyStoreStorePolicyWritesCanonicalFileAndRemovesLegacyTwin(t *testing.T) {
	ctx := context.Background()
	store, server := newTestFilerPolicyStore(t)

	server.putPolicyFile(t, store.basePath, "policy_dual-format.json", testPolicyDocument("s3:PutObject", "arn:aws:s3:::dual-format/*"))

	require.NoError(t, store.StorePolicy(ctx, "", "dual-format", testPolicyDocument("s3:GetObject", "arn:aws:s3:::dual-format/*")))

	assert.True(t, server.hasEntry(store.basePath, "dual-format.json"))
	assert.False(t, server.hasEntry(store.basePath, "policy_dual-format.json"))

	document, err := store.GetPolicy(ctx, "", "dual-format")
	require.NoError(t, err)
	require.Len(t, document.Statement, 1)
	assert.Equal(t, "s3:GetObject", document.Statement[0].Action[0])
}

func testPolicyDocument(action string, resource string) *PolicyDocument {
	return &PolicyDocument{
		Version: "2012-10-17",
		Statement: []Statement{
			{
				Effect:   "Allow",
				Action:   []string{action},
				Resource: []string{resource},
			},
		},
	}
}

func clonePolicyStoreEntry(entry *filer_pb.Entry) *filer_pb.Entry {
	if entry == nil {
		return nil
	}
	return proto.Clone(entry).(*filer_pb.Entry)
}

func policyStoreTestEntryKey(dir string, name string) string {
	return dir + "\x00" + name
}

func splitPolicyStoreEntryKey(key string) (string, string) {
	for i := 0; i < len(key); i++ {
		if key[i] == '\x00' {
			return key[:i], key[i+1:]
		}
	}
	return key, ""
}
