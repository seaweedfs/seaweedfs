package iceberg

import (
	"context"
	"errors"
	"path"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
)

// stubFilerClient answers only the calls the metadata writers make, tracking
// which paths exist so exclusive creates can be exercised.
type stubFilerClient struct {
	filer_pb.SeaweedFilerClient
	entries map[string][]byte
}

func newStubFilerClient() *stubFilerClient {
	return &stubFilerClient{entries: make(map[string][]byte)}
}

func (c *stubFilerClient) WithFilerClient(_ bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	return fn(c)
}

func (c *stubFilerClient) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest, _ ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	key := path.Join(req.Directory, req.Name)
	if _, ok := c.entries[key]; !ok {
		return nil, filer_pb.ErrNotFound
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: &filer_pb.Entry{Name: req.Name}}, nil
}

func (c *stubFilerClient) CreateEntry(_ context.Context, req *filer_pb.CreateEntryRequest, _ ...grpc.CallOption) (*filer_pb.CreateEntryResponse, error) {
	key := path.Join(req.Directory, req.Entry.Name)
	if _, exists := c.entries[key]; exists && req.OExcl {
		return &filer_pb.CreateEntryResponse{
			Error:     "entry already exists",
			ErrorCode: filer_pb.FilerError_ENTRY_ALREADY_EXISTS,
		}, nil
	}
	c.entries[key] = req.Entry.Content
	return &filer_pb.CreateEntryResponse{}, nil
}

// Two commits racing off the same base metadata pick the same v{N} file name.
// The loser must be told, not allowed to replace the winner's metadata.
func TestSaveNewMetadataFileRefusesToOverwrite(t *testing.T) {
	client := newStubFilerClient()
	s := &Server{filerClient: client}
	ctx := context.Background()

	if err := s.saveNewMetadataFile(ctx, "bkt", "ns/tbl", "v2.metadata.json", []byte(`{"winner":true}`)); err != nil {
		t.Fatalf("first write failed: %v", err)
	}

	err := s.saveNewMetadataFile(ctx, "bkt", "ns/tbl", "v2.metadata.json", []byte(`{"loser":true}`))
	if !errors.Is(err, filer_pb.ErrEntryAlreadyExists) {
		t.Fatalf("second write err = %v, want ErrEntryAlreadyExists", err)
	}

	stored := string(client.entries[path.Join(metadataDirPath("bkt", "ns/tbl"), "v2.metadata.json")])
	if stored != `{"winner":true}` {
		t.Errorf("stored metadata = %s, want the first writer's content", stored)
	}
}

// A metadata file left behind by an interrupted commit must not wedge the
// table: the next commit stages under a unique name rather than failing or
// overwriting, and the catalog pointer still decides the winner.
func TestStageCommitMetadataFallsBackToAUniqueName(t *testing.T) {
	client := newStubFilerClient()
	s := &Server{filerClient: client}
	ctx := context.Background()

	name, location, err := s.stageCommitMetadata(ctx, "bkt", "ns/tbl", "s3://bkt/ns/tbl", "v2.metadata.json", []byte(`{"orphan":true}`))
	if err != nil {
		t.Fatalf("first stage failed: %v", err)
	}
	if name != "v2.metadata.json" {
		t.Errorf("first stage used %q, want the plain versioned name", name)
	}
	if location != "s3://bkt/ns/tbl/metadata/v2.metadata.json" {
		t.Errorf("location = %q", location)
	}

	name, location, err = s.stageCommitMetadata(ctx, "bkt", "ns/tbl", "s3://bkt/ns/tbl", "v2.metadata.json", []byte(`{"second":true}`))
	if err != nil {
		t.Fatalf("second stage failed: %v", err)
	}
	if !strings.HasPrefix(name, "v2-") || !strings.HasSuffix(name, ".metadata.json") {
		t.Errorf("second stage used %q, want a unique v2-* name", name)
	}
	if location != "s3://bkt/ns/tbl/metadata/"+name {
		t.Errorf("location = %q, want it to match the staged name", location)
	}

	stored := string(client.entries[path.Join(metadataDirPath("bkt", "ns/tbl"), "v2.metadata.json")])
	if stored != `{"orphan":true}` {
		t.Errorf("the first file was overwritten: %s", stored)
	}
}

// Paths that legitimately rewrite a file, such as manifest repair, keep the
// overwriting behaviour.
func TestSaveMetadataFileOverwrites(t *testing.T) {
	client := newStubFilerClient()
	s := &Server{filerClient: client}
	ctx := context.Background()

	if err := s.saveMetadataFile(ctx, "bkt", "ns/tbl", "v2.metadata.json", []byte(`{"first":true}`)); err != nil {
		t.Fatalf("first write failed: %v", err)
	}
	if err := s.saveMetadataFile(ctx, "bkt", "ns/tbl", "v2.metadata.json", []byte(`{"second":true}`)); err != nil {
		t.Fatalf("second write failed: %v", err)
	}

	stored := string(client.entries[path.Join(metadataDirPath("bkt", "ns/tbl"), "v2.metadata.json")])
	if stored != `{"second":true}` {
		t.Errorf("stored metadata = %s, want the second write", stored)
	}
}
