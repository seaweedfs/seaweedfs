package s3tables

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
)

// A directory under a table bucket that carries no namespace metadata is not a
// namespace. Reporting that as an internal error, which is what the three
// callers of this did while each testing only for a missing entry, turns a
// client mistake into a 500 and hides it behind "attribute not found".
func TestLoadNamespaceMetadataTreatsMissingAttributeAsAbsent(t *testing.T) {
	filer := s3tablestest.Start(t)
	handler := NewS3TablesHandler()
	client := NewManagerClient(filer.Client)
	ctx := context.Background()

	filer.Put(TablesPath, "bkt", map[string][]byte{ExtendedKeyTableBucket: []byte("{}")})

	// A stray directory where a namespace would live, with no metadata on it.
	filer.Put(GetTableBucketPath("bkt"), "stray", nil)

	if _, err := handler.loadNamespaceMetadata(ctx, client, "bkt", "stray"); !errors.Is(err, filer_pb.ErrNotFound) {
		t.Fatalf("loadNamespaceMetadata on a directory without metadata = %v, want ErrNotFound", err)
	}

	if _, err := handler.loadNamespaceMetadata(ctx, client, "bkt", "absent"); !errors.Is(err, filer_pb.ErrNotFound) {
		t.Fatalf("loadNamespaceMetadata on a missing directory = %v, want ErrNotFound", err)
	}

	filer.Put(GetTableBucketPath("bkt"), "real", map[string][]byte{
		ExtendedKeyMetadata: []byte(`{"namespace":["real"],"ownerAccountId":"000000000000"}`),
	})
	metadata, err := handler.loadNamespaceMetadata(ctx, client, "bkt", "real")
	if err != nil {
		t.Fatalf("loadNamespaceMetadata on a real namespace: %v", err)
	}
	if metadata.OwnerAccountID != "000000000000" {
		t.Fatalf("owner = %q, want the stored one", metadata.OwnerAccountID)
	}
}
