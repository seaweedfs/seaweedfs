package dash

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/memory" // register memory store
	"github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"
)

func newAdminServerWithMemoryStore(t *testing.T) *AdminServer {
	t.Helper()
	cm, err := credential.NewCredentialManagerWithDefaults(credential.StoreTypeMemory)
	if err != nil {
		t.Fatalf("failed to create credential manager: %v", err)
	}
	return &AdminServer{credentialManager: cm}
}

func samplePolicyDocument() policy_engine.PolicyDocument {
	return policy_engine.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy_engine.PolicyStatement{{
			Effect:   policy_engine.PolicyEffectAllow,
			Action:   policy_engine.NewStringOrStringSlice("s3:GetObject"),
			Resource: policy_engine.NewStringOrStringSlicePtr("arn:aws:s3:::test/*"),
		}},
	}
}

func TestIsPolicyAttached(t *testing.T) {
	server := newAdminServerWithMemoryStore(t)
	ctx := context.Background()
	const policyName = "policy_a"

	if err := server.CreatePolicy(policyName, samplePolicyDocument()); err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}

	if attached, err := server.IsPolicyAttached(ctx, policyName); err != nil {
		t.Fatalf("IsPolicyAttached: %v", err)
	} else if len(attached) != 0 {
		t.Fatalf("expected no attachments, got %v", attached)
	}

	if err := server.credentialManager.CreateUser(ctx, &iam_pb.Identity{Name: "alice"}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if err := server.credentialManager.AttachUserPolicy(ctx, "alice", policyName); err != nil {
		t.Fatalf("AttachUserPolicy: %v", err)
	}

	attached, err := server.IsPolicyAttached(ctx, policyName)
	if err != nil {
		t.Fatalf("IsPolicyAttached: %v", err)
	}
	if len(attached) != 1 || attached[0] != "user:alice" {
		t.Fatalf("expected [user:alice], got %v", attached)
	}

	if err := server.credentialManager.CreateGroup(ctx, &iam_pb.Group{Name: "devs", PolicyNames: []string{policyName}}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	attached, err = server.IsPolicyAttached(ctx, policyName)
	if err != nil {
		t.Fatalf("IsPolicyAttached: %v", err)
	}
	if len(attached) != 2 {
		t.Fatalf("expected 2 attachments, got %v", attached)
	}
}

func TestDeletePolicyRejectsWhenAttachedToUser(t *testing.T) {
	server := newAdminServerWithMemoryStore(t)
	ctx := context.Background()
	const policyName = "policy_u"

	if err := server.CreatePolicy(policyName, samplePolicyDocument()); err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}
	if err := server.credentialManager.CreateUser(ctx, &iam_pb.Identity{Name: "bob"}); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if err := server.credentialManager.AttachUserPolicy(ctx, "bob", policyName); err != nil {
		t.Fatalf("AttachUserPolicy: %v", err)
	}

	if err := server.DeletePolicy(policyName); !errors.Is(err, ErrPolicyStillAttached) {
		t.Fatalf("expected ErrPolicyStillAttached, got %v", err)
	}

	if _, err := server.GetPolicy(policyName); err != nil {
		t.Fatalf("policy should still exist after rejected deletion: %v", err)
	}

	attached, err := server.credentialManager.ListAttachedUserPolicies(ctx, "bob")
	if err != nil {
		t.Fatalf("ListAttachedUserPolicies: %v", err)
	}
	if len(attached) != 1 || attached[0] != policyName {
		t.Fatalf("expected policy %q to remain attached, got %v", policyName, attached)
	}

	if err := server.credentialManager.DetachUserPolicy(ctx, "bob", policyName); err != nil {
		t.Fatalf("DetachUserPolicy: %v", err)
	}
	if err := server.DeletePolicy(policyName); err != nil {
		t.Fatalf("DeletePolicy after detach failed: %v", err)
	}
}

func TestDeletePolicyRejectsWhenAttachedToGroup(t *testing.T) {
	server := newAdminServerWithMemoryStore(t)
	ctx := context.Background()
	const policyName = "policy_g"

	if err := server.CreatePolicy(policyName, samplePolicyDocument()); err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}
	if err := server.credentialManager.CreateGroup(ctx, &iam_pb.Group{Name: "team_g", PolicyNames: []string{policyName}}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}

	if err := server.DeletePolicy(policyName); !errors.Is(err, ErrPolicyStillAttached) {
		t.Fatalf("expected ErrPolicyStillAttached, got %v", err)
	}

	if _, err := server.GetPolicy(policyName); err != nil {
		t.Fatalf("policy should still exist after rejected deletion: %v", err)
	}
}

func TestDeletePolicySucceedsWhenNotAttached(t *testing.T) {
	server := newAdminServerWithMemoryStore(t)
	const policyName = "policy_free"

	if err := server.CreatePolicy(policyName, samplePolicyDocument()); err != nil {
		t.Fatalf("CreatePolicy: %v", err)
	}
	if err := server.DeletePolicy(policyName); err != nil {
		t.Fatalf("DeletePolicy for unattached policy failed: %v", err)
	}
	if _, err := server.GetPolicy(policyName); err != nil {
		t.Fatalf("GetPolicy after delete: %v", err)
	}
}
