package integration

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/iam/sts"
)

// TestIAMManagerInitializeWithoutSTSConfig tests that IAM manager can initialize
// without an explicit STS configuration by using defaults
func TestIAMManagerInitializeWithoutSTSConfig(t *testing.T) {
	manager := NewIAMManager()

	// Create config without STS section (nil)
	// But with minimal policy and role configs to satisfy other components
	config := &IAMConfig{
		STS: nil, // No STS config provided - will use defaults
		Policy: &policy.PolicyEngineConfig{
			DefaultEffect: "Deny",
			StoreType:     "memory",
		},
		Roles: &RoleStoreConfig{
			StoreType: "memory",
		},
	}

	// Initialize should succeed with default STS config
	err := manager.Initialize(config, func() string { return "localhost:8888" })
	if err != nil {
		t.Fatalf("Failed to initialize IAM manager without STS config: %v", err)
	}

	// Verify STS service is initialized
	stsService := manager.GetSTSService()
	if stsService == nil {
		t.Fatal("STS service is nil after initialization")
	}

	if !stsService.IsInitialized() {
		t.Fatal("STS service is not initialized")
	}

	// Verify the default config was applied
	if stsService.Config.Issuer != "seaweedfs-sts" {
		t.Errorf("Expected default issuer 'seaweedfs-sts', got '%s'", stsService.Config.Issuer)
	}

	if stsService.Config.TokenDuration.Duration != 1*time.Hour {
		t.Errorf("Expected default token duration 1h, got %v", stsService.Config.TokenDuration.Duration)
	}

	if stsService.Config.MaxSessionLength.Duration != 12*time.Hour {
		t.Errorf("Expected default max session length 12h, got %v", stsService.Config.MaxSessionLength.Duration)
	}

	t.Logf("Successfully initialized STS service with default configuration")
}

// TestIAMManagerInitializeWithExplicitSTSConfig tests that explicit STS config still works
func TestIAMManagerInitializeWithExplicitSTSConfig(t *testing.T) {
	manager := NewIAMManager()

	// Create config with explicit STS section
	config := &IAMConfig{
		STS: &sts.STSConfig{
			TokenDuration:    sts.FlexibleDuration{Duration: 30 * time.Minute},
			MaxSessionLength: sts.FlexibleDuration{Duration: 2 * time.Hour},
			Issuer:           "custom-issuer",
			SigningKey:       []byte("custom-signing-key-for-testing-purposes-only"),
			AccountId:        "999888777666",
		},
		Policy: &policy.PolicyEngineConfig{
			DefaultEffect: "Deny",
			StoreType:     "memory",
		},
		Roles: &RoleStoreConfig{
			StoreType: "memory",
		},
	}

	// Initialize should succeed with explicit config
	err := manager.Initialize(config, func() string { return "localhost:8888" })
	if err != nil {
		t.Fatalf("Failed to initialize IAM manager with explicit STS config: %v", err)
	}

	// Verify STS service uses the custom config
	stsService := manager.GetSTSService()
	if stsService == nil {
		t.Fatal("STS service is nil after initialization")
	}

	if !stsService.IsInitialized() {
		t.Fatal("STS service is not initialized")
	}

	if stsService.Config.Issuer != "custom-issuer" {
		t.Errorf("Expected issuer 'custom-issuer', got '%s'", stsService.Config.Issuer)
	}

	if stsService.Config.AccountId != "999888777666" {
		t.Errorf("Expected account ID '999888777666', got '%s'", stsService.Config.AccountId)
	}

	t.Logf("Successfully initialized STS service with explicit configuration")
}
