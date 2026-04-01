package s3api

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/iam/integration"
	"github.com/stretchr/testify/assert"
)

func TestLoadIAMManagerFromConfig_Defaults(t *testing.T) {
	// Create a temporary config file with minimal content (just policy)
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "iam_config.json")

	configContent := `{
		"sts": {
			"providers": []
		},
		"policy": {
			"storeType": "memory",
			"defaultEffect": "Allow"
		}
	}`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	assert.NoError(t, err)

	// dummy filer address provider
	filerProvider := func() string { return "localhost:8888" }
	defaultSigningKeyProvider := func() string { return "default-secure-signing-key" }

	// Load the manager
	manager, err := loadIAMManagerFromConfig(configPath, filerProvider, defaultSigningKeyProvider)
	assert.NoError(t, err)
	assert.NotNil(t, manager)
}

func TestLoadIAMManagerFromConfig_Overrides(t *testing.T) {
	// Create a temporary config file with EXPLICIT values
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "iam_config_explicit.json")

	configContent := `{
		"sts": {
			"tokenDuration": "2h",
			"maxSessionLength": "24h",
			"issuer": "custom-issuer",
			"signingKey": "ZXhwbGljaXQtc2lnbmluZy1rZXktMTIzNDU=" 
		},
		"policy": {
			"storeType": "memory",
			"defaultEffect": "Allow"
		}
	}`
	// Base64 encoded "explicit-signing-key-12345" is "ZXhwbGljaXQtc2lnbmluZy1rZXktMTIzNDU="

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	assert.NoError(t, err)

	filerProvider := func() string { return "localhost:8888" }
	defaultSigningKeyProvider := func() string { return "default-secure-signing-key" }

	// Load
	manager, err := loadIAMManagerFromConfig(configPath, filerProvider, defaultSigningKeyProvider)
	assert.NoError(t, err)
	assert.NotNil(t, manager)
}

func TestLoadIAMManagerFromConfig_PartialDefaults(t *testing.T) {
	// Test that partial configs (e.g. providing SigningKey but not Duration) work
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "iam_config_partial.json")

	// Signing key provided in JSON, others missing
	configContent := `{
		"sts": {
			"signingKey": "anNvbi1wcm92aWRlZC1rZXktMTIzNDU="
		},
		"policy": {
			"storeType": "memory",
			"defaultEffect": "Allow"
		}
	}`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	assert.NoError(t, err)

	filerProvider := func() string { return "localhost:8888" }
	// Default signing key provided but should be IGNORED because JSON has one
	defaultSigningKeyProvider := func() string { return "server-default-key-should-be-ignored" }

	manager, err := loadIAMManagerFromConfig(configPath, filerProvider, defaultSigningKeyProvider)
	assert.NoError(t, err)
	assert.NotNil(t, manager)
}

func TestLoadIAMManagerFromConfig_ExplicitEmptyKey(t *testing.T) {
	// Test that if JSON has empty signing key string, it still falls back
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "iam_config_empty_key.json")

	// Signing key explicitly empty
	configContent := `{
		"sts": {
			"signingKey": ""
		},
		"policy": {
			"storeType": "memory",
			"defaultEffect": "Allow"
		}
	}`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	assert.NoError(t, err)

	filerProvider := func() string { return "localhost:8888" }
	defaultSigningKeyProvider := func() string { return "fallback-key-should-be-used" }

	manager, err := loadIAMManagerFromConfig(configPath, filerProvider, defaultSigningKeyProvider)
	assert.NoError(t, err)
	assert.NotNil(t, manager)
}

func TestLoadIAMManagerFromConfig_MissingKeyError(t *testing.T) {
	// Test that if BOTH keys are empty, it fails with a clear error
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "iam_config_all_empty.json")

	// Signing key explicitly empty in JSON
	configContent := `{
		"sts": {
			"signingKey": ""
		},
		"policy": {
			"storeType": "memory",
			"defaultEffect": "Allow"
		}
	}`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	assert.NoError(t, err)

	filerProvider := func() string { return "localhost:8888" }
	defaultSigningKeyProvider := func() string { return "" } // Empty default too

	// Ensure no SSE-S3 key interferes (global state in tests is tricky, but let's assume clean state or no mock)
	// Ideally we would mock GetSSES3KeyManager().GetMasterKey() but it's a global singleton.
	// For this unit test, if the global key manager has no key, it should fail.

	_, err = loadIAMManagerFromConfig(configPath, filerProvider, defaultSigningKeyProvider)

	// Should return a clear error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no signing key found for STS service")
}

func TestLoadIAMManagerFromConfig_ExplicitFileDefaultsToDeny(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "iam_config_implicit_policy_defaults.json")

	// Explicit config file with no policy.defaultEffect should default to Deny.
	configContent := `{
		"sts": {
			"issuer": "explicit-config"
		}
	}`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	assert.NoError(t, err)

	filerProvider := func() string { return "localhost:8888" }
	defaultSigningKeyProvider := func() string { return "fallback-key-for-explicit-config" }

	manager, err := loadIAMManagerFromConfig(configPath, filerProvider, defaultSigningKeyProvider)
	assert.NoError(t, err)
	assert.NotNil(t, manager)
	assert.False(t, manager.DefaultAllow())
}

func TestLoadIAMManagerFromConfig_NoFileDefaultsToAllow(t *testing.T) {
	// No explicit IAM file should preserve zero-config startup behavior.
	filerProvider := func() string { return "localhost:8888" }
	defaultSigningKeyProvider := func() string { return "fallback-key-for-zero-config" }

	manager, err := loadIAMManagerFromConfig("", filerProvider, defaultSigningKeyProvider)
	assert.NoError(t, err)
	assert.NotNil(t, manager)
	assert.True(t, manager.DefaultAllow())
}

func TestLoadIAMManagerFromConfig_ExplicitFileEnforcesUserScopedPolicy(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "iam_regression_8366.json")

	// Regression coverage for #8366:
	// with explicit IAM config and omitted policy.defaultEffect, unrestricted bucket creation
	// must NOT be allowed.
	configContent := `{
		"sts": {
			"issuer": "seaweedfs-sts"
		},
		"policies": [
			{
				"name": "S3UserPolicy",
				"document": {
					"Version": "2012-10-17",
					"Statement": [
						{
							"Effect": "Allow",
							"Action": ["s3:ListAllMyBuckets"],
							"Resource": ["arn:aws:s3:::*"]
						},
						{
							"Effect": "Allow",
							"Action": ["s3:*"],
							"Resource": [
								"arn:aws:s3:::user-${jwt:preferred_username}",
								"arn:aws:s3:::user-${jwt:preferred_username}/*"
							]
						}
					]
				}
			}
		],
		"roles": [
			{
				"roleName": "S3UserRole",
				"roleArn": "arn:aws:iam::role/S3UserRole",
				"attachedPolicies": ["S3UserPolicy"],
				"trustPolicy": {
					"Version": "2012-10-17",
					"Statement": [
						{
							"Effect": "Allow",
							"Principal": {"Federated": "*"},
							"Action": ["sts:AssumeRoleWithWebIdentity"]
						}
					]
				}
			}
		]
	}`

	err := os.WriteFile(configPath, []byte(configContent), 0644)
	assert.NoError(t, err)

	filerProvider := func() string { return "localhost:8888" }
	defaultSigningKeyProvider := func() string { return "fallback-key-for-regression-8366" }

	manager, err := loadIAMManagerFromConfig(configPath, filerProvider, defaultSigningKeyProvider)
	assert.NoError(t, err)
	assert.NotNil(t, manager)
	assert.False(t, manager.DefaultAllow())

	ctx := context.Background()
	principal := "arn:aws:sts::000000000000:assumed-role/S3UserRole/alice-session"
	reqCtx := map[string]interface{}{"jwt:preferred_username": "alice"}

	allowed, err := manager.IsActionAllowed(ctx, &integration.ActionRequest{
		Principal:      principal,
		Action:         "s3:CreateBucket",
		Resource:       "arn:aws:s3:::arbitrary-bucket",
		RequestContext: reqCtx,
	})
	assert.NoError(t, err)
	assert.False(t, allowed, "arbitrary bucket creation should be denied")

	allowed, err = manager.IsActionAllowed(ctx, &integration.ActionRequest{
		Principal:      principal,
		Action:         "s3:CreateBucket",
		Resource:       "arn:aws:s3:::user-alice",
		RequestContext: reqCtx,
	})
	assert.NoError(t, err)
	assert.True(t, allowed, "user-scoped bucket creation should be allowed")
}
