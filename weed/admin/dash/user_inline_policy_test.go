package dash

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/memory" // register the memory credential store
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

// TestGetObjectStoreUserDetailsIncludesInlinePolicies verifies that inline user
// policies — which are stored separately from the identity record and are
// authoritative at S3 enforcement time (they take precedence over the legacy
// Actions list) — are surfaced in the admin user details. They were previously
// invisible in the admin API/UI, so an operator could not tell what actually
// governed a user's access.
func TestGetObjectStoreUserDetailsIncludesInlinePolicies(t *testing.T) {
	cm, err := credential.NewCredentialManagerWithDefaults(credential.StoreTypeMemory)
	if err != nil {
		t.Fatalf("failed to init credential manager: %v", err)
	}
	ctx := context.Background()

	const username = "svc-app"
	if err := cm.CreateUser(ctx, &iam_pb.Identity{Name: username}); err != nil {
		t.Fatalf("failed to create user: %v", err)
	}

	const inlinePolicyName = "svc-app-policy"
	doc := policy_engine.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy_engine.PolicyStatement{
			{
				Effect:   "Allow",
				Action:   policy_engine.NewStringOrStringSlice("s3:GetObject"),
				Resource: policy_engine.NewStringOrStringSlicePtr("arn:aws:s3:::ml-artifacts/*"),
			},
		},
	}
	if err := cm.PutUserInlinePolicy(ctx, username, inlinePolicyName, doc); err != nil {
		t.Fatalf("failed to put inline policy: %v", err)
	}

	s := &AdminServer{credentialManager: cm}
	details, err := s.GetObjectStoreUserDetails(username)
	if err != nil {
		t.Fatalf("GetObjectStoreUserDetails failed: %v", err)
	}

	found := false
	for _, p := range details.PolicyNames {
		if p == inlinePolicyName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected inline policy %q in details.PolicyNames, got %v", inlinePolicyName, details.PolicyNames)
	}
}
