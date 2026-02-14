package s3api

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/memory"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
)

func TestOnIamConfigChangeLegacyIdentityDeletionReloadsConfiguration(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{
		{
			Name: "anonymous",
			Actions: []string{
				"Read:test",
			},
		},
	})

	err := s3a.onIamConfigChange(
		filer.IamConfigDirectory,
		&filer_pb.Entry{Name: filer.IamIdentityFile},
		nil,
	)
	if err != nil {
		t.Fatalf("onIamConfigChange returned error for legacy identity deletion: %v", err)
	}

	if !hasIdentity(s3a.iam, "anonymous") {
		t.Fatalf("expected anonymous identity to remain loaded after legacy identity deletion event")
	}
}

func TestOnIamConfigChangeReloadsOnIamIdentityDirectoryChanges(t *testing.T) {
	s3a := newTestS3ApiServerWithMemoryIAM(t, []*iam_pb.Identity{
		{Name: "anonymous"},
	})

	// Seed initial in-memory IAM state.
	if err := s3a.iam.LoadS3ApiConfigurationFromCredentialManager(); err != nil {
		t.Fatalf("failed to load initial IAM configuration: %v", err)
	}
	if hasIdentity(s3a.iam, "alice") {
		t.Fatalf("did not expect alice identity before creating user")
	}

	if err := s3a.iam.credentialManager.CreateUser(context.Background(), &iam_pb.Identity{Name: "alice"}); err != nil {
		t.Fatalf("failed to create alice in memory credential manager: %v", err)
	}

	err := s3a.onIamConfigChange(
		filer.IamConfigDirectory+"/identities",
		nil,
		&filer_pb.Entry{Name: "alice.json"},
	)
	if err != nil {
		t.Fatalf("onIamConfigChange returned error for identities directory update: %v", err)
	}

	if !hasIdentity(s3a.iam, "alice") {
		t.Fatalf("expected alice identity to be loaded after /etc/iam/identities update")
	}
}

func newTestS3ApiServerWithMemoryIAM(t *testing.T, identities []*iam_pb.Identity) *S3ApiServer {
	t.Helper()

	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")

	cm, err := credential.NewCredentialManager(credential.StoreTypeMemory, nil, "")
	if err != nil {
		t.Fatalf("failed to create memory credential manager: %v", err)
	}
	if err := cm.SaveConfiguration(context.Background(), &iam_pb.S3ApiConfiguration{}); err != nil {
		t.Fatalf("failed to reset memory credential manager: %v", err)
	}

	for _, identity := range identities {
		if err := cm.CreateUser(context.Background(), identity); err != nil {
			t.Fatalf("failed to seed identity %q: %v", identity.Name, err)
		}
	}

	return &S3ApiServer{
		iam: &IdentityAccessManagement{
			credentialManager: cm,
		},
	}
}

func hasIdentity(iam *IdentityAccessManagement, identityName string) bool {
	iam.m.RLock()
	defer iam.m.RUnlock()

	_, ok := iam.nameToIdentity[identityName]
	return ok
}
