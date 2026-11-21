package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// memFiler holds one directory level of entries in memory.
type memFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	mu      sync.Mutex
	entries map[string]*filer_pb.Entry // keyed by directory + "/" + name
}

func newMemFiler() *memFiler { return &memFiler{entries: map[string]*filer_pb.Entry{}} }

func (f *memFiler) put(dir string, entry *filer_pb.Entry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[dir+"/"+entry.Name] = entry
}

func (f *memFiler) names(dir string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var names []string
	for key, e := range f.entries {
		if key == dir+"/"+e.Name {
			names = append(names, e.Name)
		}
	}
	sort.Strings(names)
	return names
}

func (f *memFiler) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e, ok := f.entries[req.Directory+"/"+req.Name]; ok {
		return &filer_pb.LookupDirectoryEntryResponse{Entry: e}, nil
	}
	return nil, status.Error(codes.NotFound, "not found")
}

func (f *memFiler) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest) (*filer_pb.CreateEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := req.Directory + "/" + req.Entry.Name
	for _, c := range req.GetCondition().GetClauses() {
		if _, exists := f.entries[key]; exists && c.Kind == filer_pb.WriteCondition_IF_NOT_EXISTS {
			return &filer_pb.CreateEntryResponse{Error: "exists", ErrorCode: filer_pb.FilerError_PRECONDITION_FAILED}, nil
		}
	}
	f.entries[key] = req.Entry
	return &filer_pb.CreateEntryResponse{}, nil
}

func (f *memFiler) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.entries, req.Directory+"/"+req.Name)
	return &filer_pb.DeleteEntryResponse{}, nil
}

func (f *memFiler) ListEntries(req *filer_pb.ListEntriesRequest, stream filer_pb.SeaweedFiler_ListEntriesServer) error {
	for _, name := range f.names(req.Directory) {
		if name <= req.StartFromFileName {
			continue
		}
		f.mu.Lock()
		e := f.entries[req.Directory+"/"+name]
		f.mu.Unlock()
		if err := stream.Send(&filer_pb.ListEntriesResponse{Entry: e}); err != nil {
			return err
		}
	}
	return nil
}

// otherModeName is a record name the other fips140 mode could have derived.
func otherModeName(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:]) + ".json"
}

func storeAs(t *testing.T, filer *memFiler, dir, name string, record any) {
	data, err := json.Marshal(record)
	require.NoError(t, err)
	filer.put(dir, &filer_pb.Entry{Name: name, Attributes: &filer_pb.FuseAttributes{FileMode: 0o600}, Content: data})
}

func TestRevocationWrittenInTheOtherModeStillRevokes(t *testing.T) {
	ctx := context.Background()
	filer := newMemFiler()
	store := NewFilerSessionRevocationStore(nil, startTestFiler(t, filer))
	store.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	storeAs(t, filer, store.basePath, otherModeName("session-1"), &RevocationEntry{JTI: "session-1", ExpiresAt: time.Now().Add(time.Hour)})

	revoked, err := store.IsRevoked(ctx, "", "session-1")
	require.NoError(t, err)
	require.True(t, revoked)
	require.Equal(t, []string{store.fileName("session-1")}, filer.names(store.basePath))
}

func TestProviderStoredInTheOtherModeCanBeDeleted(t *testing.T) {
	ctx := context.Background()
	filer := newMemFiler()
	store := NewFilerOIDCProviderStore(nil, startTestFiler(t, filer))
	store.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	const arn = "arn:aws:iam::123456789012:oidc-provider/issuer.example.com"
	const issuer = "https://issuer.example.com"
	storeAs(t, filer, store.basePath, otherModeName(arn), newRecord(arn, issuer))

	got, err := store.GetProviderByARN(ctx, "", arn)
	require.NoError(t, err)
	require.Equal(t, issuer, got.URL)

	require.NoError(t, store.DeleteProvider(ctx, "", arn))
	_, err = store.GetProviderByIssuer(ctx, "", issuer)
	require.Error(t, err, "a deleted provider must not stay trusted")
	require.Empty(t, filer.names(store.basePath))
}

// A record already under this mode's name is newer than its other-mode copy.
func TestRenameKeepsTheRecordUnderThisModesName(t *testing.T) {
	ctx := context.Background()
	filer := newMemFiler()
	store := NewFilerOIDCProviderStore(nil, startTestFiler(t, filer))
	store.grpcDialOption = grpc.WithTransportCredentials(insecure.NewCredentials())
	const arn = "arn:aws:iam::123456789012:oidc-provider/issuer.example.com"
	storeAs(t, filer, store.basePath, otherModeName(arn), newRecord(arn, "https://old.example.com"))
	storeAs(t, filer, store.basePath, store.fileName(arn), newRecord(arn, "https://new.example.com"))

	records, err := store.ListProviders(ctx, "")
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, "https://new.example.com", records[0].URL)
}
