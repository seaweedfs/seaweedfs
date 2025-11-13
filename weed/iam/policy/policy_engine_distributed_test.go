package policy

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDistributedPolicyEngine verifies that multiple PolicyEngine instances with identical configurations
// behave consistently across distributed environments
func TestDistributedPolicyEngine(t *testing.T) {
	ctx := context.Background()

	// Common configuration for all instances
	commonConfig := &PolicyEngineConfig{
		DefaultEffect: "Deny",
		StoreType:     "memory", // For testing - would be "filer" in production
		StoreConfig:   map[string]interface{}{},
	}

	// Create multiple PolicyEngine instances simulating distributed deployment
	instance1 := NewPolicyEngine()
	instance2 := NewPolicyEngine()
	instance3 := NewPolicyEngine()

	// Initialize all instances with identical configuration
	err := instance1.Initialize(commonConfig)
	require.NoError(t, err, "Instance 1 should initialize successfully")

	err = instance2.Initialize(commonConfig)
	require.NoError(t, err, "Instance 2 should initialize successfully")

	err = instance3.Initialize(commonConfig)
	require.NoError(t, err, "Instance 3 should initialize successfully")

	// Test policy consistency across instances
	t.Run("policy_storage_consistency", func(t *testing.T) {
		// Define a test policy
		testPolicy := &PolicyDocument{
			Version: "2012-10-17",
			Statement: []Statement{
				{
					Sid:      "AllowS3Read",
					Effect:   "Allow",
					Action:   []string{"s3:GetObject", "s3:ListBucket"},
					Resource: []string{"arn:aws:s3:::test-bucket/*", "arn:aws:s3:::test-bucket"},
				},
				{
					Sid:      "DenyS3Write",
					Effect:   "Deny",
					Action:   []string{"s3:PutObject", "s3:DeleteObject"},
					Resource: []string{"arn:aws:s3:::test-bucket/*"},
				},
			},
		}

		// Store policy on instance 1
		err := instance1.AddPolicy("", "TestPolicy", testPolicy)
		require.NoError(t, err, "Should be able to store policy on instance 1")

		// For memory storage, each instance has separate storage
		// In production with filer storage, all instances would share the same policies

		// Verify policy exists on instance 1
		storedPolicy1, err := instance1.store.GetPolicy(ctx, "", "TestPolicy")
		require.NoError(t, err, "Policy should exist on instance 1")
		assert.Equal(t, "2012-10-17", storedPolicy1.Version)
		assert.Len(t, storedPolicy1.Statement, 2)

		// For demonstration: store same policy on other instances
		err = instance2.AddPolicy("", "TestPolicy", testPolicy)
		require.NoError(t, err, "Should be able to store policy on instance 2")

		err = instance3.AddPolicy("", "TestPolicy", testPolicy)
		require.NoError(t, err, "Should be able to store policy on instance 3")
	})

	// Test policy evaluation consistency
	t.Run("evaluation_consistency", func(t *testing.T) {
		// Create evaluation context
		evalCtx := &EvaluationContext{
			Principal: "arn:aws:sts::assumed-role/TestRole/session",
			Action:    "s3:GetObject",
			Resource:  "arn:aws:s3:::test-bucket/file.txt",
			RequestContext: map[string]interface{}{
				"sourceIp": "192.168.1.100",
			},
		}

		// Evaluate policy on all instances
		result1, err1 := instance1.Evaluate(ctx, "", evalCtx, []string{"TestPolicy"})
		result2, err2 := instance2.Evaluate(ctx, "", evalCtx, []string{"TestPolicy"})
		result3, err3 := instance3.Evaluate(ctx, "", evalCtx, []string{"TestPolicy"})

		require.NoError(t, err1, "Evaluation should succeed on instance 1")
		require.NoError(t, err2, "Evaluation should succeed on instance 2")
		require.NoError(t, err3, "Evaluation should succeed on instance 3")

		// All instances should return identical results
		assert.Equal(t, result1.Effect, result2.Effect, "Instance 1 and 2 should have same effect")
		assert.Equal(t, result2.Effect, result3.Effect, "Instance 2 and 3 should have same effect")
		assert.Equal(t, EffectAllow, result1.Effect, "Should allow s3:GetObject")

		// Matching statements should be identical
		assert.Len(t, result1.MatchingStatements, 1, "Should have one matching statement")
		assert.Len(t, result2.MatchingStatements, 1, "Should have one matching statement")
		assert.Len(t, result3.MatchingStatements, 1, "Should have one matching statement")

		assert.Equal(t, "AllowS3Read", result1.MatchingStatements[0].StatementSid)
		assert.Equal(t, "AllowS3Read", result2.MatchingStatements[0].StatementSid)
		assert.Equal(t, "AllowS3Read", result3.MatchingStatements[0].StatementSid)
	})

	// Test explicit deny precedence
	t.Run("deny_precedence_consistency", func(t *testing.T) {
		evalCtx := &EvaluationContext{
			Principal: "arn:aws:sts::assumed-role/TestRole/session",
			Action:    "s3:PutObject",
			Resource:  "arn:aws:s3:::test-bucket/newfile.txt",
		}

		// All instances should consistently apply deny precedence
		result1, err1 := instance1.Evaluate(ctx, "", evalCtx, []string{"TestPolicy"})
		result2, err2 := instance2.Evaluate(ctx, "", evalCtx, []string{"TestPolicy"})
		result3, err3 := instance3.Evaluate(ctx, "", evalCtx, []string{"TestPolicy"})

		require.NoError(t, err1)
		require.NoError(t, err2)
		require.NoError(t, err3)

		// All should deny due to explicit deny statement
		assert.Equal(t, EffectDeny, result1.Effect, "Instance 1 should deny write operation")
		assert.Equal(t, EffectDeny, result2.Effect, "Instance 2 should deny write operation")
		assert.Equal(t, EffectDeny, result3.Effect, "Instance 3 should deny write operation")

		// Should have matching deny statement
		assert.Len(t, result1.MatchingStatements, 1)
		assert.Equal(t, "DenyS3Write", result1.MatchingStatements[0].StatementSid)
		assert.Equal(t, EffectDeny, result1.MatchingStatements[0].Effect)
	})

	// Test default effect consistency
	t.Run("default_effect_consistency", func(t *testing.T) {
		evalCtx := &EvaluationContext{
			Principal: "arn:aws:sts::assumed-role/TestRole/session",
			Action:    "filer:CreateEntry", // Action not covered by any policy
			Resource:  "arn:aws:filer::path/test",
		}

		result1, err1 := instance1.Evaluate(ctx, "", evalCtx, []string{"TestPolicy"})
		result2, err2 := instance2.Evaluate(ctx, "", evalCtx, []string{"TestPolicy"})
		result3, err3 := instance3.Evaluate(ctx, "", evalCtx, []string{"TestPolicy"})

		require.NoError(t, err1)
		require.NoError(t, err2)
		require.NoError(t, err3)

		// All should use default effect (Deny)
		assert.Equal(t, EffectDeny, result1.Effect, "Should use default effect")
		assert.Equal(t, EffectDeny, result2.Effect, "Should use default effect")
		assert.Equal(t, EffectDeny, result3.Effect, "Should use default effect")

		// No matching statements
		assert.Empty(t, result1.MatchingStatements, "Should have no matching statements")
		assert.Empty(t, result2.MatchingStatements, "Should have no matching statements")
		assert.Empty(t, result3.MatchingStatements, "Should have no matching statements")
	})
}

// TestPolicyEngineConfigurationConsistency tests configuration validation for distributed deployments
func TestPolicyEngineConfigurationConsistency(t *testing.T) {
	t.Run("consistent_default_effects_required", func(t *testing.T) {
		// Different default effects could lead to inconsistent authorization
		config1 := &PolicyEngineConfig{
			DefaultEffect: "Allow",
			StoreType:     "memory",
		}

		config2 := &PolicyEngineConfig{
			DefaultEffect: "Deny", // Different default!
			StoreType:     "memory",
		}

		instance1 := NewPolicyEngine()
		instance2 := NewPolicyEngine()

		err1 := instance1.Initialize(config1)
		err2 := instance2.Initialize(config2)

		require.NoError(t, err1)
		require.NoError(t, err2)

		// Test with an action not covered by any policy
		evalCtx := &EvaluationContext{
			Principal: "arn:aws:sts::assumed-role/TestRole/session",
			Action:    "uncovered:action",
			Resource:  "arn:aws:test:::resource",
		}

		result1, _ := instance1.Evaluate(context.Background(), "", evalCtx, []string{})
		result2, _ := instance2.Evaluate(context.Background(), "", evalCtx, []string{})

		// Results should be different due to different default effects
		assert.NotEqual(t, result1.Effect, result2.Effect, "Different default effects should produce different results")
		assert.Equal(t, EffectAllow, result1.Effect, "Instance 1 should allow by default")
		assert.Equal(t, EffectDeny, result2.Effect, "Instance 2 should deny by default")
	})

	t.Run("invalid_configuration_handling", func(t *testing.T) {
		invalidConfigs := []*PolicyEngineConfig{
			{
				DefaultEffect: "Maybe", // Invalid effect
				StoreType:     "memory",
			},
			{
				DefaultEffect: "Allow",
				StoreType:     "nonexistent", // Invalid store type
			},
		}

		for i, config := range invalidConfigs {
			t.Run(fmt.Sprintf("invalid_config_%d", i), func(t *testing.T) {
				instance := NewPolicyEngine()
				err := instance.Initialize(config)
				assert.Error(t, err, "Should reject invalid configuration")
			})
		}
	})
}

// TestPolicyStoreDistributed tests policy store behavior in distributed scenarios
func TestPolicyStoreDistributed(t *testing.T) {
	ctx := context.Background()

	t.Run("memory_store_isolation", func(t *testing.T) {
		// Memory stores are isolated per instance (not suitable for distributed)
		store1 := NewMemoryPolicyStore()
		store2 := NewMemoryPolicyStore()

		policy := &PolicyDocument{
			Version: "2012-10-17",
			Statement: []Statement{
				{
					Effect:   "Allow",
					Action:   []string{"s3:GetObject"},
					Resource: []string{"*"},
				},
			},
		}

		// Store policy in store1
		err := store1.StorePolicy(ctx, "", "TestPolicy", policy)
		require.NoError(t, err)

		// Policy should exist in store1
		_, err = store1.GetPolicy(ctx, "", "TestPolicy")
		assert.NoError(t, err, "Policy should exist in store1")

		// Policy should NOT exist in store2 (different instance)
		_, err = store2.GetPolicy(ctx, "", "TestPolicy")
		assert.Error(t, err, "Policy should not exist in store2")
		assert.Contains(t, err.Error(), "not found", "Should be a not found error")
	})

	t.Run("policy_loading_error_handling", func(t *testing.T) {
		engine := NewPolicyEngine()
		config := &PolicyEngineConfig{
			DefaultEffect: "Deny",
			StoreType:     "memory",
		}

		err := engine.Initialize(config)
		require.NoError(t, err)

		evalCtx := &EvaluationContext{
			Principal: "arn:aws:sts::assumed-role/TestRole/session",
			Action:    "s3:GetObject",
			Resource:  "arn:aws:s3:::bucket/key",
		}

		// Evaluate with non-existent policies
		result, err := engine.Evaluate(ctx, "", evalCtx, []string{"NonExistentPolicy1", "NonExistentPolicy2"})
		require.NoError(t, err, "Should not error on missing policies")

		// Should use default effect when no policies can be loaded
		assert.Equal(t, EffectDeny, result.Effect, "Should use default effect")
		assert.Empty(t, result.MatchingStatements, "Should have no matching statements")
	})
}

// TestFilerPolicyStoreConfiguration tests filer policy store configuration for distributed deployments
func TestFilerPolicyStoreConfiguration(t *testing.T) {
	t.Run("filer_store_creation", func(t *testing.T) {
		// Test with minimal configuration
		config := map[string]interface{}{
			"filerAddress": "localhost:8888",
		}

		store, err := NewFilerPolicyStore(config, nil)
		require.NoError(t, err, "Should create filer policy store with minimal config")
		assert.NotNil(t, store)
	})

	t.Run("filer_store_custom_path", func(t *testing.T) {
		config := map[string]interface{}{
			"filerAddress": "prod-filer:8888",
			"basePath":     "/custom/iam/policies",
		}

		store, err := NewFilerPolicyStore(config, nil)
		require.NoError(t, err, "Should create filer policy store with custom path")
		assert.NotNil(t, store)
	})

	t.Run("filer_store_missing_address", func(t *testing.T) {
		config := map[string]interface{}{
			"basePath": "/seaweedfs/iam/policies",
		}

		store, err := NewFilerPolicyStore(config, nil)
		assert.NoError(t, err, "Should create filer store without filerAddress in config")
		assert.NotNil(t, store, "Store should be created successfully")
	})
}

// TestPolicyEvaluationPerformance tests performance considerations for distributed policy evaluation
func TestPolicyEvaluationPerformance(t *testing.T) {
	ctx := context.Background()

	// Create engine with memory store (for performance baseline)
	engine := NewPolicyEngine()
	config := &PolicyEngineConfig{
		DefaultEffect: "Deny",
		StoreType:     "memory",
	}

	err := engine.Initialize(config)
	require.NoError(t, err)

	// Add multiple policies
	for i := 0; i < 10; i++ {
		policy := &PolicyDocument{
			Version: "2012-10-17",
			Statement: []Statement{
				{
					Sid:      fmt.Sprintf("Statement%d", i),
					Effect:   "Allow",
					Action:   []string{"s3:GetObject", "s3:ListBucket"},
					Resource: []string{fmt.Sprintf("arn:aws:s3:::bucket%d/*", i)},
				},
			},
		}

		err := engine.AddPolicy("", fmt.Sprintf("Policy%d", i), policy)
		require.NoError(t, err)
	}

	// Test evaluation performance
	evalCtx := &EvaluationContext{
		Principal: "arn:aws:sts::assumed-role/TestRole/session",
		Action:    "s3:GetObject",
		Resource:  "arn:aws:s3:::bucket5/file.txt",
	}

	policyNames := make([]string, 10)
	for i := 0; i < 10; i++ {
		policyNames[i] = fmt.Sprintf("Policy%d", i)
	}

	// Measure evaluation time
	start := time.Now()
	for i := 0; i < 100; i++ {
		_, err := engine.Evaluate(ctx, "", evalCtx, policyNames)
		require.NoError(t, err)
	}
	duration := time.Since(start)

	// Should be reasonably fast (less than 10ms per evaluation on average)
	avgDuration := duration / 100
	t.Logf("Average policy evaluation time: %v", avgDuration)
	assert.Less(t, avgDuration, 10*time.Millisecond, "Policy evaluation should be fast")
}
