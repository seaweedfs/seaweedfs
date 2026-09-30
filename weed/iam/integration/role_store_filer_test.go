package integration

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
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

// roleStoreTestFiler is a filer holding one directory. It evaluates the write
// conditions FilerRoleStore sends as the filer does, and can run another
// writer between a role's read and its write (afterLookup) or break a listing
// stream partway (failListAfter).
type roleStoreTestFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu            sync.Mutex
	entries       map[string]*filer_pb.Entry
	afterLookup   func()
	failListAfter int
}

func (s *roleStoreTestFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	s.mu.Lock()
	entry, found := s.entries[req.Name]
	hook := s.afterLookup
	s.afterLookup = nil
	s.mu.Unlock()
	if hook != nil {
		defer hook()
	}
	if !found {
		return nil, status.Error(codes.NotFound, filer_pb.ErrNotFound.Error())
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: proto.Clone(entry).(*filer_pb.Entry)}, nil
}

func (s *roleStoreTestFiler) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.entries[req.Entry.Name]
	for _, c := range req.GetCondition().GetClauses() {
		var ok bool
		switch c.Kind {
		case filer_pb.WriteCondition_IF_NOT_EXISTS:
			ok = !exists
		case filer_pb.WriteCondition_IF_ENTRY_EQUAL:
			ok = exists && proto.Equal(current, c.ExpectedEntry)
		default:
			return nil, fmt.Errorf("unexpected condition %v", c.Kind)
		}
		if !ok {
			return &filer_pb.CreateEntryResponse{Error: "precondition failed", ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED}, nil
		}
	}
	s.entries[req.Entry.Name] = proto.Clone(req.Entry).(*filer_pb.Entry)
	return &filer_pb.CreateEntryResponse{}, nil
}

func (s *roleStoreTestFiler) ListEntries(req *filer_pb.ListEntriesRequest, stream grpc.ServerStreamingServer[filer_pb.ListEntriesResponse]) error {
	s.mu.Lock()
	var names []string
	for name := range s.entries {
		if name > req.StartFromFileName {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	if req.Limit > 0 && len(names) > int(req.Limit) {
		names = names[:req.Limit]
	}
	page := make([]*filer_pb.Entry, 0, len(names))
	for _, name := range names {
		page = append(page, proto.Clone(s.entries[name]).(*filer_pb.Entry))
	}
	failAfter := s.failListAfter
	s.mu.Unlock()
	for i, entry := range page {
		if failAfter > 0 && i == failAfter {
			return status.Error(codes.Unavailable, "filer went away")
		}
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: entry}); err != nil {
			return err
		}
	}
	return nil
}

func (s *roleStoreTestFiler) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, req.Name)
	return &filer_pb.DeleteEntryResponse{}, nil
}

func newTestFilerRoleStore(t *testing.T) (*FilerRoleStore, *roleStoreTestFiler) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	filer := &roleStoreTestFiler{entries: map[string]*filer_pb.Entry{}}
	server := pb.NewGrpcServer()
	filer_pb.RegisterSeaweedFilerServer(server, filer)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})
	host, port, err := net.SplitHostPort(lis.Addr().String())
	require.NoError(t, err)
	grpcPort, err := strconv.Atoi(port)
	require.NoError(t, err)
	store, err := NewFilerRoleStore(nil, func() string { return string(pb.NewServerAddress(host, 1, grpcPort)) })
	require.NoError(t, err)
	store.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	return store, filer
}

func attachPolicy(policyName string) RoleUpdate {
	return func(current *RoleDefinition) (*RoleDefinition, error) {
		if current == nil {
			return nil, ErrRoleNotFound
		}
		current.AttachedPolicies = append(current.AttachedPolicies, policyName)
		return current, nil
	}
}

func createRole(roleID string) RoleUpdate {
	return func(current *RoleDefinition) (*RoleDefinition, error) {
		if current != nil {
			return nil, ErrRoleExists
		}
		return &RoleDefinition{RoleName: "app", RoleId: roleID}, nil
	}
}

// Two servers changing one role: the change written second is applied to
// what the first left, not to the role as it read it.
func TestFilerRoleUpdateIsNotLostToAConcurrentChange(t *testing.T) {
	ctx := context.Background()
	store, filer := newTestFilerRoleStore(t)
	require.NoError(t, store.UpdateRole(ctx, "", "app", createRole("AROA1")))

	filer.afterLookup = func() { assert.NoError(t, store.UpdateRole(ctx, "", "app", attachPolicy("peer"))) }
	require.NoError(t, store.UpdateRole(ctx, "", "app", attachPolicy("mine")))

	role, err := store.GetRole(ctx, "", "app")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"peer", "mine"}, role.AttachedPolicies, "one server's change was lost")
}

// A change racing a delete must not write the role back.
func TestFilerRoleUpdateDoesNotReviveADeletedRole(t *testing.T) {
	ctx := context.Background()
	store, filer := newTestFilerRoleStore(t)
	require.NoError(t, store.UpdateRole(ctx, "", "app", createRole("AROA1")))

	filer.afterLookup = func() { assert.NoError(t, store.DeleteRole(ctx, "", "app")) }
	err := store.UpdateRole(ctx, "", "app", attachPolicy("mine"))
	assert.ErrorIs(t, err, ErrRoleNotFound)

	_, err = store.GetRole(ctx, "", "app")
	assert.ErrorIs(t, err, ErrRoleNotFound, "the deleted role was written back")
}

// Of two creates of one name, the second sees the first's role.
func TestFilerRoleCreateRefusesARoleCreatedConcurrently(t *testing.T) {
	ctx := context.Background()
	store, filer := newTestFilerRoleStore(t)

	filer.afterLookup = func() { assert.NoError(t, store.UpdateRole(ctx, "", "app", createRole("AROA-FIRST"))) }
	err := store.UpdateRole(ctx, "", "app", createRole("AROA-SECOND"))
	assert.ErrorIs(t, err, ErrRoleExists)

	role, err := store.GetRole(ctx, "", "app")
	require.NoError(t, err)
	assert.Equal(t, "AROA-FIRST", role.RoleId, "the second create replaced the first role")
}

func TestFilerRoleListingPagesPastTheFirstThousand(t *testing.T) {
	store, filer := newTestFilerRoleStore(t)
	for i := range roleListPageSize + 1 {
		name := fmt.Sprintf("role-%04d.json", i)
		filer.entries[name] = &filer_pb.Entry{Name: name}
	}
	filer.entries["role-0500"] = &filer_pb.Entry{Name: "role-0500", IsDirectory: true}

	names, err := store.ListRoles(context.Background(), "")
	require.NoError(t, err)
	assert.Len(t, names, roleListPageSize+1)
	assert.True(t, slices.Contains(names, fmt.Sprintf("role-%04d", roleListPageSize)), "the role past the first page is missing")
}

// A listing cut short must fail: DeletePolicy decides from it whether any
// role still attaches the policy.
func TestFilerRoleListingFailsOnABrokenStream(t *testing.T) {
	store, filer := newTestFilerRoleStore(t)
	for i := range 5 {
		name := fmt.Sprintf("role-%d.json", i)
		filer.entries[name] = &filer_pb.Entry{Name: name}
	}
	filer.failListAfter = 3

	_, err := store.ListRoles(context.Background(), "")
	require.Error(t, err, "a partial listing was returned as complete")

	attaching, err := RolesAttachingPolicy(context.Background(), store, "read")
	assert.Error(t, err)
	assert.Nil(t, attaching)
	assert.False(t, errors.Is(err, ErrRoleNotFound))
}
