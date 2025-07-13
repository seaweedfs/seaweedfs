package test

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/credential/memory"
	"github.com/seaweedfs/seaweedfs/weed/s3api/policy_engine"

	// Import all store implementations to register them
	_ "github.com/seaweedfs/seaweedfs/weed/credential/filer_etc"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/memory"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/postgres"
)

// TestPolicyManagement tests policy management across all credential stores
func TestPolicyManagement(t *testing.T) {
	ctx := context.Background()

	// Test with memory store (easiest to test)
	credentialManager, err := credential.NewCredentialManager(credential.StoreTypeMemory, nil, "")
	if err != nil {
		t.Fatalf("Failed to create credential manager: %v", err)
	}

	// Test policy operations
	testPolicyOperations(t, ctx, credentialManager)
}

func testPolicyOperations(t *testing.T, ctx context.Context, credentialManager *credential.CredentialManager) {
	store := credentialManager.GetStore()

	// Cast to memory store to access policy methods
	memoryStore, ok := store.(*memory.MemoryStore)
	if !ok {
		t.Skip("Store is not a memory store")
	}

	// Test GetPolicies (should be empty initially)
	policies, err := memoryStore.GetPolicies(ctx)
	if err != nil {
		t.Fatalf("Failed to get policies: %v", err)
	}
	if len(policies) != 0 {
		t.Errorf("Expected 0 policies, got %d", len(policies))
	}

	// Test CreatePolicy
	testPolicy := policy_engine.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy_engine.PolicyStatement{
			{
				Effect:   policy_engine.PolicyEffectAllow,
				Action:   policy_engine.NewStringOrStringSlice("s3:GetObject"),
				Resource: policy_engine.NewStringOrStringSlice("arn:aws:s3:::test-bucket/*"),
			},
		},
	}

	err = memoryStore.CreatePolicy(ctx, "test-policy", testPolicy)
	if err != nil {
		t.Fatalf("Failed to create policy: %v", err)
	}

	// Test GetPolicies (should have 1 policy now)
	policies, err = memoryStore.GetPolicies(ctx)
	if err != nil {
		t.Fatalf("Failed to get policies: %v", err)
	}
	if len(policies) != 1 {
		t.Errorf("Expected 1 policy, got %d", len(policies))
	}

	// Verify policy content
	policy, exists := policies["test-policy"]
	if !exists {
		t.Error("test-policy not found")
	}
	if policy.Version != "2012-10-17" {
		t.Errorf("Expected policy version '2012-10-17', got '%s'", policy.Version)
	}
	if len(policy.Statement) != 1 {
		t.Errorf("Expected 1 statement, got %d", len(policy.Statement))
	}

	// Test UpdatePolicy
	updatedPolicy := policy_engine.PolicyDocument{
		Version: "2012-10-17",
		Statement: []policy_engine.PolicyStatement{
			{
				Effect:   policy_engine.PolicyEffectAllow,
				Action:   policy_engine.NewStringOrStringSlice("s3:GetObject", "s3:PutObject"),
				Resource: policy_engine.NewStringOrStringSlice("arn:aws:s3:::test-bucket/*"),
			},
		},
	}

	err = memoryStore.UpdatePolicy(ctx, "test-policy", updatedPolicy)
	if err != nil {
		t.Fatalf("Failed to update policy: %v", err)
	}

	// Verify the update
	policies, err = memoryStore.GetPolicies(ctx)
	if err != nil {
		t.Fatalf("Failed to get policies after update: %v", err)
	}

	updatedPolicyResult, exists := policies["test-policy"]
	if !exists {
		t.Error("test-policy not found after update")
	}
	if len(updatedPolicyResult.Statement) != 1 {
		t.Errorf("Expected 1 statement after update, got %d", len(updatedPolicyResult.Statement))
	}
	if len(updatedPolicyResult.Statement[0].Action.Strings()) != 2 {
		t.Errorf("Expected 2 actions after update, got %d", len(updatedPolicyResult.Statement[0].Action.Strings()))
	}

	// Test DeletePolicy
	err = memoryStore.DeletePolicy(ctx, "test-policy")
	if err != nil {
		t.Fatalf("Failed to delete policy: %v", err)
	}

	// Verify deletion
	policies, err = memoryStore.GetPolicies(ctx)
	if err != nil {
		t.Fatalf("Failed to get policies after deletion: %v", err)
	}
	if len(policies) != 0 {
		t.Errorf("Expected 0 policies after deletion, got %d", len(policies))
	}
}

// TestPolicyManagementWithFilerEtc tests policy management with filer_etc store
func TestPolicyManagementWithFilerEtc(t *testing.T) {
	// Skip this test if we can't connect to a filer
	t.Skip("Filer connection required for filer_etc store testing")
}

// TestPolicyManagementWithPostgres tests policy management with postgres store
func TestPolicyManagementWithPostgres(t *testing.T) {
	// Skip this test if we can't connect to PostgreSQL
	t.Skip("PostgreSQL connection required for postgres store testing")
}
