package credential

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"google.golang.org/protobuf/proto"
)

type snapshotConfigurationStore struct {
	CredentialStore
	saved *iam_pb.S3ApiConfiguration
}

func (s *snapshotConfigurationStore) SaveConfiguration(_ context.Context, config *iam_pb.S3ApiConfiguration) error {
	s.saved = config
	return nil
}

func TestSaveConfigurationFiltersStaticIdentitySnapshot(t *testing.T) {
	t.Parallel()
	store := &snapshotConfigurationStore{}
	manager := &CredentialManager{Store: store}
	manager.SetStaticIdentities([]*iam_pb.Identity{{Name: "static"}})
	source := &iam_pb.S3ApiConfiguration{Identities: []*iam_pb.Identity{
		{Name: "alice", Actions: []string{"Read"}},
		{Name: "static"},
		{Name: "bob", Actions: []string{"Write"}},
	}}
	before := proto.Clone(source)
	if err := manager.SaveConfiguration(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	if store.saved == nil || len(store.saved.Identities) != 2 || store.saved.Identities[0].Name != "alice" || store.saved.Identities[1].Name != "bob" {
		t.Fatalf("unexpected saved configuration: %v", store.saved)
	}
	if !proto.Equal(source, before) {
		t.Fatal("filtering mutated the caller's configuration")
	}
	store.saved.Identities[0].Actions[0] = "Admin"
	store.saved.Identities[1].Name = "changed"
	if !proto.Equal(source, before) {
		t.Fatal("saved identities alias the caller's configuration")
	}
}
