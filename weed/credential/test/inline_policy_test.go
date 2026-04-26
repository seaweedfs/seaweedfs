package test

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/credential/memory"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"

	_ "github.com/seaweedfs/seaweedfs/weed/credential/filer_etc"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/memory"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/postgres"
)

func TestInlinePolicyOperations(t *testing.T) {
	ctx := context.Background()

	credentialManager, err := credential.NewCredentialManager(credential.StoreTypeMemory, nil, "")
	if err != nil {
		t.Fatalf("Failed to create credential manager: %v", err)
	}

	store, ok := credentialManager.GetStore().(*memory.MemoryStore)
	if !ok {
		t.Fatal("Store is not a memory store")
	}

	userName := "testuser"
	policyName := "read-bucket"
	doc := policy_engine.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy_engine.PolicyStatement{
			{
				Effect:   policy_engine.PolicyEffectAllow,
				Action:   policy_engine.NewStringOrStringSlice("s3:GetObject"),
				Resource: policy_engine.NewStringOrStringSlicePtr("arn:aws:s3:::test-bucket/*"),
			},
		},
	}

	// Put
	if err := store.PutUserInlinePolicy(ctx, userName, policyName, doc); err != nil {
		t.Fatalf("PutUserInlinePolicy failed: %v", err)
	}

	// Get
	got, err := store.GetUserInlinePolicy(ctx, userName, policyName)
	if err != nil {
		t.Fatalf("GetUserInlinePolicy failed: %v", err)
	}
	if got == nil {
		t.Fatal("GetUserInlinePolicy returned nil")
	}
	if got.Version != "2012-10-17" {
		t.Errorf("Expected version '2012-10-17', got '%s'", got.Version)
	}
	if len(got.Statement) != 1 {
		t.Errorf("Expected 1 statement, got %d", len(got.Statement))
	}

	// Get non-existent
	missing, err := store.GetUserInlinePolicy(ctx, userName, "no-such-policy")
	if err != nil {
		t.Fatalf("GetUserInlinePolicy for missing policy failed: %v", err)
	}
	if missing != nil {
		t.Error("Expected nil for non-existent policy")
	}

	// Put second policy, same user
	doc2 := policy_engine.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy_engine.PolicyStatement{
			{
				Effect:   policy_engine.PolicyEffectAllow,
				Action:   policy_engine.NewStringOrStringSlice("s3:PutObject"),
				Resource: policy_engine.NewStringOrStringSlicePtr("arn:aws:s3:::other-bucket/*"),
			},
		},
	}
	if err := store.PutUserInlinePolicy(ctx, userName, "write-bucket", doc2); err != nil {
		t.Fatalf("PutUserInlinePolicy second policy failed: %v", err)
	}

	// List
	names, err := store.ListUserInlinePolicies(ctx, userName)
	if err != nil {
		t.Fatalf("ListUserInlinePolicies failed: %v", err)
	}
	if len(names) != 2 {
		t.Errorf("Expected 2 policies, got %d", len(names))
	}

	// List for non-existent user
	emptyNames, err := store.ListUserInlinePolicies(ctx, "nobody")
	if err != nil {
		t.Fatalf("ListUserInlinePolicies for missing user failed: %v", err)
	}
	if len(emptyNames) != 0 {
		t.Errorf("Expected 0 policies for missing user, got %d", len(emptyNames))
	}

	// LoadInlinePolicies (bulk)
	all, err := store.LoadInlinePolicies(ctx)
	if err != nil {
		t.Fatalf("LoadInlinePolicies failed: %v", err)
	}
	if len(all) != 1 {
		t.Errorf("Expected 1 user in LoadInlinePolicies, got %d", len(all))
	}
	if len(all[userName]) != 2 {
		t.Errorf("Expected 2 policies for user in LoadInlinePolicies, got %d", len(all[userName]))
	}

	// Overwrite existing policy (upsert)
	updatedDoc := policy_engine.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy_engine.PolicyStatement{
			{
				Effect:   policy_engine.PolicyEffectDeny,
				Action:   policy_engine.NewStringOrStringSlice("s3:GetObject"),
				Resource: policy_engine.NewStringOrStringSlicePtr("arn:aws:s3:::test-bucket/secret/*"),
			},
		},
	}
	if err := store.PutUserInlinePolicy(ctx, userName, policyName, updatedDoc); err != nil {
		t.Fatalf("PutUserInlinePolicy overwrite failed: %v", err)
	}
	overwritten, err := store.GetUserInlinePolicy(ctx, userName, policyName)
	if err != nil {
		t.Fatalf("GetUserInlinePolicy after overwrite failed: %v", err)
	}
	if overwritten.Statement[0].Effect != policy_engine.PolicyEffectDeny {
		t.Errorf("Expected Deny after overwrite, got %s", overwritten.Statement[0].Effect)
	}

	// Delete one policy
	if err := store.DeleteUserInlinePolicy(ctx, userName, policyName); err != nil {
		t.Fatalf("DeleteUserInlinePolicy failed: %v", err)
	}
	deleted, err := store.GetUserInlinePolicy(ctx, userName, policyName)
	if err != nil {
		t.Fatalf("GetUserInlinePolicy after delete failed: %v", err)
	}
	if deleted != nil {
		t.Error("Expected nil after delete")
	}

	// Remaining policy still there
	remaining, err := store.ListUserInlinePolicies(ctx, userName)
	if err != nil {
		t.Fatalf("ListUserInlinePolicies after delete failed: %v", err)
	}
	if len(remaining) != 1 {
		t.Errorf("Expected 1 remaining policy, got %d", len(remaining))
	}

	// Delete last policy — user entry should be cleaned up
	if err := store.DeleteUserInlinePolicy(ctx, userName, "write-bucket"); err != nil {
		t.Fatalf("DeleteUserInlinePolicy last policy failed: %v", err)
	}
	allAfter, err := store.LoadInlinePolicies(ctx)
	if err != nil {
		t.Fatalf("LoadInlinePolicies after full cleanup failed: %v", err)
	}
	if len(allAfter) != 0 {
		t.Errorf("Expected empty LoadInlinePolicies after cleanup, got %d users", len(allAfter))
	}
}
