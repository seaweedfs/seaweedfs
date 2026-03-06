package filer_etc

import (
	"context"
	"net"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type policyTestFilerServer struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu                   sync.RWMutex
	entries              map[string]*filer_pb.Entry
	contentlessListEntry map[string]struct{}
	beforeLookup         func(context.Context, string, string) error
	afterListEntry       func(string, string)
	beforeDelete         func(string, string) error
	beforeUpdate         func(string, string) error
}

func newPolicyTestFilerServer() *policyTestFilerServer {
	return &policyTestFilerServer{
		entries:              make(map[string]*filer_pb.Entry),
		contentlessListEntry: make(map[string]struct{}),
	}
}

func (s *policyTestFilerServer) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	s.mu.RLock()
	beforeLookup := s.beforeLookup
	s.mu.RUnlock()
	if beforeLookup != nil {
		if err := beforeLookup(ctx, req.Directory, req.Name); err != nil {
			return nil, err
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, found := s.entries[filerEntryKey(req.Directory, req.Name)]
	if !found {
		return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
	}

	return &filer_pb.LookupDirectoryEntryResponse{Entry: cloneEntry(entry)}, nil
}

func (s *policyTestFilerServer) ListEntries(req *filer_pb.ListEntriesRequest, stream grpc.ServerStreamingServer[filer_pb.ListEntriesResponse]) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	names := make([]string, 0)
	for key := range s.entries {
		dir, name := splitFilerEntryKey(key)
		if dir != req.Directory {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		entry := cloneEntry(s.entries[filerEntryKey(req.Directory, name)])
		if _, found := s.contentlessListEntry[filerEntryKey(req.Directory, name)]; found {
			entry.Content = nil
		}
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: entry}); err != nil {
			return err
		}
		if s.afterListEntry != nil {
			s.afterListEntry(req.Directory, name)
		}
	}

	return nil
}

func (s *policyTestFilerServer) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries[filerEntryKey(req.Directory, req.Entry.Name)] = cloneEntry(req.Entry)
	return &filer_pb.CreateEntryResponse{}, nil
}

func (s *policyTestFilerServer) UpdateEntry(_ context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	s.mu.RLock()
	beforeUpdate := s.beforeUpdate
	s.mu.RUnlock()
	if beforeUpdate != nil {
		if err := beforeUpdate(req.Directory, req.Entry.Name); err != nil {
			return nil, err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries[filerEntryKey(req.Directory, req.Entry.Name)] = cloneEntry(req.Entry)
	return &filer_pb.UpdateEntryResponse{}, nil
}

func (s *policyTestFilerServer) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	s.mu.RLock()
	beforeDelete := s.beforeDelete
	s.mu.RUnlock()
	if beforeDelete != nil {
		if err := beforeDelete(req.Directory, req.Name); err != nil {
			return nil, err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := filerEntryKey(req.Directory, req.Name)
	if _, found := s.entries[key]; !found {
		return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
	}

	delete(s.entries, key)
	return &filer_pb.DeleteEntryResponse{}, nil
}

func newPolicyTestStore(t *testing.T) *FilerEtcStore {
	store, _ := newPolicyTestStoreWithServer(t)
	return store
}

func newPolicyTestStoreWithServer(t *testing.T) (*FilerEtcStore, *policyTestFilerServer) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := newPolicyTestFilerServer()
	grpcServer := pb.NewGrpcServer()
	filer_pb.RegisterSeaweedFilerServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(lis)
	}()

	t.Cleanup(func() {
		grpcServer.Stop()
		_ = lis.Close()
	})

	store := &FilerEtcStore{}
	host, portString, err := net.SplitHostPort(lis.Addr().String())
	require.NoError(t, err)
	grpcPort, err := strconv.Atoi(portString)
	require.NoError(t, err)
	store.SetFilerAddressFunc(func() pb.ServerAddress {
		return pb.NewServerAddress(host, 1, grpcPort)
	}, grpc.WithTransportCredentials(insecure.NewCredentials()))

	return store, server
}

func TestFilerEtcStoreListPolicyNamesIncludesLegacyPolicies(t *testing.T) {
	ctx := context.Background()
	store := newPolicyTestStore(t)

	legacyPolicies := newPoliciesCollection()
	legacyPolicies.Policies["legacy-only"] = testPolicyDocument("s3:GetObject", "arn:aws:s3:::legacy-only/*")
	legacyPolicies.Policies["shared"] = testPolicyDocument("s3:GetObject", "arn:aws:s3:::shared/*")
	require.NoError(t, store.saveLegacyPoliciesCollection(ctx, legacyPolicies))

	require.NoError(t, store.savePolicy(ctx, "multi-file-only", testPolicyDocument("s3:PutObject", "arn:aws:s3:::multi-file-only/*")))
	require.NoError(t, store.savePolicy(ctx, "shared", testPolicyDocument("s3:DeleteObject", "arn:aws:s3:::shared/*")))

	names, err := store.ListPolicyNames(ctx)
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"legacy-only", "multi-file-only", "shared"}, names)
}

func TestFilerEtcStoreDeletePolicyRemovesLegacyManagedCopy(t *testing.T) {
	ctx := context.Background()
	store := newPolicyTestStore(t)

	inlinePolicy := testPolicyDocument("s3:PutObject", "arn:aws:s3:::inline-user/*")
	legacyPolicies := newPoliciesCollection()
	legacyPolicies.Policies["legacy-only"] = testPolicyDocument("s3:GetObject", "arn:aws:s3:::legacy-only/*")
	legacyPolicies.InlinePolicies["inline-user"] = map[string]policy_engine.PolicyDocument{
		"PutOnly": inlinePolicy,
	}
	require.NoError(t, store.saveLegacyPoliciesCollection(ctx, legacyPolicies))

	managedPolicies, err := store.LoadManagedPolicies(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"legacy-only"}, managedPolicyNames(managedPolicies))

	require.NoError(t, store.DeletePolicy(ctx, "legacy-only"))

	managedPolicies, err = store.LoadManagedPolicies(ctx)
	require.NoError(t, err)
	assert.Empty(t, managedPolicies)

	inlinePolicies, err := store.LoadInlinePolicies(ctx)
	require.NoError(t, err)
	assertInlinePolicyPreserved(t, inlinePolicies, "inline-user", "PutOnly")

	loadedLegacyPolicies, foundLegacy, err := store.loadLegacyPoliciesCollection(ctx)
	require.NoError(t, err)
	require.True(t, foundLegacy)
	assert.Empty(t, loadedLegacyPolicies.Policies)
	assertInlinePolicyPreserved(t, loadedLegacyPolicies.InlinePolicies, "inline-user", "PutOnly")
}

func TestFilerEtcStoreLoadManagedPoliciesRespectsReadContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store, server := newPolicyTestStoreWithServer(t)

	require.NoError(t, store.savePolicy(context.Background(), "cancel-me", testPolicyDocument("s3:GetObject", "arn:aws:s3:::cancel-me/*")))

	server.mu.Lock()
	server.contentlessListEntry[filerEntryKey(filer.IamConfigDirectory+"/"+IamPoliciesDirectory, "cancel-me.json")] = struct{}{}
	server.beforeLookup = func(ctx context.Context, dir string, name string) error {
		if dir == filer.IamConfigDirectory+"/"+IamPoliciesDirectory && name == "cancel-me.json" {
			cancel()
			return status.Error(codes.Canceled, context.Canceled.Error())
		}
		return nil
	}
	server.mu.Unlock()

	managedPolicies, err := store.LoadManagedPolicies(ctx)
	require.NoError(t, err)
	assert.Empty(t, managedPolicies)
}

func testPolicyDocument(action string, resource string) policy_engine.PolicyDocument {
	return policy_engine.PolicyDocument{
		Version: policy_engine.PolicyVersion2012_10_17,
		Statement: []policy_engine.PolicyStatement{
			{
				Effect:   policy_engine.PolicyEffectAllow,
				Action:   policy_engine.NewStringOrStringSlice(action),
				Resource: policy_engine.NewStringOrStringSlice(resource),
			},
		},
	}
}

func managedPolicyNames(policies []*iam_pb.Policy) []string {
	names := make([]string, 0, len(policies))
	for _, policy := range policies {
		names = append(names, policy.Name)
	}
	sort.Strings(names)
	return names
}

func assertInlinePolicyPreserved(t *testing.T, inlinePolicies map[string]map[string]policy_engine.PolicyDocument, userName string, policyName string) {
	t.Helper()

	userPolicies, found := inlinePolicies[userName]
	require.True(t, found)

	policy, found := userPolicies[policyName]
	require.True(t, found)
	assert.Equal(t, policy_engine.PolicyVersion2012_10_17, policy.Version)
	require.Len(t, policy.Statement, 1)
	assert.Equal(t, policy_engine.PolicyEffectAllow, policy.Statement[0].Effect)
}

func cloneEntry(entry *filer_pb.Entry) *filer_pb.Entry {
	if entry == nil {
		return nil
	}
	return proto.Clone(entry).(*filer_pb.Entry)
}

func filerEntryKey(dir string, name string) string {
	return dir + "\x00" + name
}

func splitFilerEntryKey(key string) (dir string, name string) {
	for idx := 0; idx < len(key); idx++ {
		if key[idx] == '\x00' {
			return key[:idx], key[idx+1:]
		}
	}
	return key, ""
}
