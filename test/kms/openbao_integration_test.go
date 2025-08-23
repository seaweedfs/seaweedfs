package kms_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/vault/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/kms"
	_ "github.com/seaweedfs/seaweedfs/weed/kms/openbao"
)

const (
	OpenBaoAddress = "http://127.0.0.1:8200"
	OpenBaoToken   = "root-token-for-testing"
	TransitPath    = "transit"
)

// Test configuration for OpenBao KMS provider
type testConfig struct {
	config map[string]interface{}
}

func (c *testConfig) GetString(key string) string {
	if val, ok := c.config[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

func (c *testConfig) GetBool(key string) bool {
	if val, ok := c.config[key]; ok {
		if b, ok := val.(bool); ok {
			return b
		}
	}
	return false
}

func (c *testConfig) GetInt(key string) int {
	if val, ok := c.config[key]; ok {
		if i, ok := val.(int); ok {
			return i
		}
		if f, ok := val.(float64); ok {
			return int(f)
		}
	}
	return 0
}

func (c *testConfig) GetStringSlice(key string) []string {
	if val, ok := c.config[key]; ok {
		if slice, ok := val.([]string); ok {
			return slice
		}
	}
	return nil
}

func (c *testConfig) SetDefault(key string, value interface{}) {
	if c.config == nil {
		c.config = make(map[string]interface{})
	}
	if _, exists := c.config[key]; !exists {
		c.config[key] = value
	}
}

// setupOpenBao starts OpenBao in development mode for testing
func setupOpenBao(t *testing.T) (*exec.Cmd, func()) {
	// Check if OpenBao is running in Docker (via make dev-openbao)
	client, err := api.NewClient(&api.Config{Address: OpenBaoAddress})
	if err == nil {
		client.SetToken(OpenBaoToken)
		_, err = client.Sys().Health()
		if err == nil {
			glog.V(1).Infof("Using existing OpenBao server at %s", OpenBaoAddress)
			// Return dummy command and cleanup function for existing server
			return nil, func() {}
		}
	}

	// Check if OpenBao binary is available for starting locally
	_, err = exec.LookPath("bao")
	if err != nil {
		t.Skip("OpenBao not running and bao binary not found. Run 'cd test/kms && make dev-openbao' first")
	}

	// Start OpenBao in dev mode
	cmd := exec.Command("bao", "server", "-dev", "-dev-root-token-id="+OpenBaoToken, "-dev-listen-address=127.0.0.1:8200")
	cmd.Env = append(os.Environ(), "BAO_DEV_ROOT_TOKEN_ID="+OpenBaoToken)

	// Capture output for debugging
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Start()
	require.NoError(t, err, "Failed to start OpenBao server")

	// Wait for OpenBao to be ready
	client, err = api.NewClient(&api.Config{Address: OpenBaoAddress})
	require.NoError(t, err)
	client.SetToken(OpenBaoToken)

	// Wait up to 30 seconds for OpenBao to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			cmd.Process.Kill()
			t.Fatal("Timeout waiting for OpenBao to start")
		default:
			// Try to check health
			resp, err := client.Sys().Health()
			if err == nil && resp.Initialized {
				glog.V(1).Infof("OpenBao server ready")
				goto ready
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

ready:
	// Setup cleanup function
	cleanup := func() {
		if cmd != nil && cmd.Process != nil {
			glog.V(1).Infof("Stopping OpenBao server")
			cmd.Process.Kill()
			cmd.Wait()
		}
	}

	return cmd, cleanup
}

// setupTransitEngine enables and configures the transit secrets engine
func setupTransitEngine(t *testing.T) {
	client, err := api.NewClient(&api.Config{Address: OpenBaoAddress})
	require.NoError(t, err)
	client.SetToken(OpenBaoToken)

	// Enable transit secrets engine
	err = client.Sys().Mount(TransitPath, &api.MountInput{
		Type:        "transit",
		Description: "Transit engine for KMS testing",
	})
	if err != nil && !strings.Contains(err.Error(), "path is already in use") {
		require.NoError(t, err, "Failed to enable transit engine")
	}

	// Create test encryption keys
	testKeys := []string{"test-key-1", "test-key-2", "seaweedfs-test-key"}

	for _, keyName := range testKeys {
		keyData := map[string]interface{}{
			"type": "aes256-gcm96",
		}

		path := fmt.Sprintf("%s/keys/%s", TransitPath, keyName)
		_, err = client.Logical().Write(path, keyData)
		if err != nil && !strings.Contains(err.Error(), "key already exists") {
			require.NoError(t, err, "Failed to create test key %s", keyName)
		}

		glog.V(2).Infof("Created/verified test key: %s", keyName)
	}
}

func TestOpenBaoKMSProvider_Integration(t *testing.T) {
	// Start OpenBao server
	_, cleanup := setupOpenBao(t)
	defer cleanup()

	// Setup transit engine and keys
	setupTransitEngine(t)

	t.Run("CreateProvider", func(t *testing.T) {
		config := &testConfig{
			config: map[string]interface{}{
				"address":      OpenBaoAddress,
				"token":        OpenBaoToken,
				"transit_path": TransitPath,
			},
		}

		provider, err := kms.GetProvider("openbao", config)
		require.NoError(t, err)
		require.NotNil(t, provider)

		defer provider.Close()
	})

	t.Run("ProviderRegistration", func(t *testing.T) {
		// Test that the provider is registered
		providers := kms.ListProviders()
		assert.Contains(t, providers, "openbao")
		assert.Contains(t, providers, "vault") // Compatibility alias
	})

	t.Run("GenerateDataKey", func(t *testing.T) {
		config := &testConfig{
			config: map[string]interface{}{
				"address":      OpenBaoAddress,
				"token":        OpenBaoToken,
				"transit_path": TransitPath,
			},
		}

		provider, err := kms.GetProvider("openbao", config)
		require.NoError(t, err)
		defer provider.Close()

		ctx := context.Background()
		req := &kms.GenerateDataKeyRequest{
			KeyID:   "test-key-1",
			KeySpec: kms.KeySpecAES256,
			EncryptionContext: map[string]string{
				"test": "context",
				"env":  "integration",
			},
		}

		resp, err := provider.GenerateDataKey(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		assert.Equal(t, "test-key-1", resp.KeyID)
		assert.Len(t, resp.Plaintext, 32) // 256 bits
		assert.NotEmpty(t, resp.CiphertextBlob)

		// Verify the response is in standardized envelope format
		envelope, err := kms.ParseEnvelope(resp.CiphertextBlob)
		assert.NoError(t, err)
		assert.Equal(t, "openbao", envelope.Provider)
		assert.Equal(t, "test-key-1", envelope.KeyID)
		assert.True(t, strings.HasPrefix(envelope.Ciphertext, "vault:")) // Raw OpenBao format inside envelope
	})

	t.Run("DecryptDataKey", func(t *testing.T) {
		config := &testConfig{
			config: map[string]interface{}{
				"address":      OpenBaoAddress,
				"token":        OpenBaoToken,
				"transit_path": TransitPath,
			},
		}

		provider, err := kms.GetProvider("openbao", config)
		require.NoError(t, err)
		defer provider.Close()

		ctx := context.Background()

		// First generate a data key
		genReq := &kms.GenerateDataKeyRequest{
			KeyID:   "test-key-1",
			KeySpec: kms.KeySpecAES256,
			EncryptionContext: map[string]string{
				"test": "decrypt",
				"env":  "integration",
			},
		}

		genResp, err := provider.GenerateDataKey(ctx, genReq)
		require.NoError(t, err)

		// Now decrypt it
		decReq := &kms.DecryptRequest{
			CiphertextBlob: genResp.CiphertextBlob,
			EncryptionContext: map[string]string{
				"openbao:key:name": "test-key-1",
				"test":             "decrypt",
				"env":              "integration",
			},
		}

		decResp, err := provider.Decrypt(ctx, decReq)
		require.NoError(t, err)
		require.NotNil(t, decResp)

		assert.Equal(t, "test-key-1", decResp.KeyID)
		assert.Equal(t, genResp.Plaintext, decResp.Plaintext)
	})

	t.Run("DescribeKey", func(t *testing.T) {
		config := &testConfig{
			config: map[string]interface{}{
				"address":      OpenBaoAddress,
				"token":        OpenBaoToken,
				"transit_path": TransitPath,
			},
		}

		provider, err := kms.GetProvider("openbao", config)
		require.NoError(t, err)
		defer provider.Close()

		ctx := context.Background()
		req := &kms.DescribeKeyRequest{
			KeyID: "test-key-1",
		}

		resp, err := provider.DescribeKey(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		assert.Equal(t, "test-key-1", resp.KeyID)
		assert.Contains(t, resp.ARN, "openbao:")
		assert.Equal(t, kms.KeyStateEnabled, resp.KeyState)
		assert.Equal(t, kms.KeyUsageEncryptDecrypt, resp.KeyUsage)
	})

	t.Run("NonExistentKey", func(t *testing.T) {
		config := &testConfig{
			config: map[string]interface{}{
				"address":      OpenBaoAddress,
				"token":        OpenBaoToken,
				"transit_path": TransitPath,
			},
		}

		provider, err := kms.GetProvider("openbao", config)
		require.NoError(t, err)
		defer provider.Close()

		ctx := context.Background()
		req := &kms.DescribeKeyRequest{
			KeyID: "non-existent-key",
		}

		_, err = provider.DescribeKey(ctx, req)
		require.Error(t, err)

		kmsErr, ok := err.(*kms.KMSError)
		require.True(t, ok)
		assert.Equal(t, kms.ErrCodeNotFoundException, kmsErr.Code)
	})

	t.Run("MultipleKeys", func(t *testing.T) {
		config := &testConfig{
			config: map[string]interface{}{
				"address":      OpenBaoAddress,
				"token":        OpenBaoToken,
				"transit_path": TransitPath,
			},
		}

		provider, err := kms.GetProvider("openbao", config)
		require.NoError(t, err)
		defer provider.Close()

		ctx := context.Background()

		// Test with multiple keys
		testKeys := []string{"test-key-1", "test-key-2", "seaweedfs-test-key"}

		for _, keyName := range testKeys {
			t.Run(fmt.Sprintf("Key_%s", keyName), func(t *testing.T) {
				// Generate data key
				genReq := &kms.GenerateDataKeyRequest{
					KeyID:   keyName,
					KeySpec: kms.KeySpecAES256,
					EncryptionContext: map[string]string{
						"key": keyName,
					},
				}

				genResp, err := provider.GenerateDataKey(ctx, genReq)
				require.NoError(t, err)
				assert.Equal(t, keyName, genResp.KeyID)

				// Decrypt data key
				decReq := &kms.DecryptRequest{
					CiphertextBlob: genResp.CiphertextBlob,
					EncryptionContext: map[string]string{
						"openbao:key:name": keyName,
						"key":              keyName,
					},
				}

				decResp, err := provider.Decrypt(ctx, decReq)
				require.NoError(t, err)
				assert.Equal(t, genResp.Plaintext, decResp.Plaintext)
			})
		}
	})
}

func TestOpenBaoKMSProvider_ErrorHandling(t *testing.T) {
	// Start OpenBao server
	_, cleanup := setupOpenBao(t)
	defer cleanup()

	setupTransitEngine(t)

	t.Run("InvalidToken", func(t *testing.T) {
		t.Skip("Skipping invalid token test - OpenBao dev mode may be too permissive")

		config := &testConfig{
			config: map[string]interface{}{
				"address":      OpenBaoAddress,
				"token":        "invalid-token",
				"transit_path": TransitPath,
			},
		}

		provider, err := kms.GetProvider("openbao", config)
		require.NoError(t, err) // Provider creation doesn't validate token
		defer provider.Close()

		ctx := context.Background()
		req := &kms.GenerateDataKeyRequest{
			KeyID:   "test-key-1",
			KeySpec: kms.KeySpecAES256,
		}

		_, err = provider.GenerateDataKey(ctx, req)
		require.Error(t, err)

		// Check that it's a KMS error (could be access denied or other auth error)
		kmsErr, ok := err.(*kms.KMSError)
		require.True(t, ok, "Expected KMSError but got: %T", err)
		// OpenBao might return different error codes for invalid tokens
		assert.Contains(t, []string{kms.ErrCodeAccessDenied, kms.ErrCodeKMSInternalFailure}, kmsErr.Code)
	})

}

func TestKMSManager_WithOpenBao(t *testing.T) {
	// Start OpenBao server
	_, cleanup := setupOpenBao(t)
	defer cleanup()

	setupTransitEngine(t)

	t.Run("KMSManagerIntegration", func(t *testing.T) {
		manager := kms.InitializeKMSManager()

		// Add OpenBao provider to manager
		kmsConfig := &kms.KMSConfig{
			Provider: "openbao",
			Config: map[string]interface{}{
				"address":      OpenBaoAddress,
				"token":        OpenBaoToken,
				"transit_path": TransitPath,
			},
			CacheEnabled: true,
			CacheTTL:     time.Hour,
		}

		err := manager.AddKMSProvider("openbao-test", kmsConfig)
		require.NoError(t, err)

		// Set as default provider
		err = manager.SetDefaultKMSProvider("openbao-test")
		require.NoError(t, err)

		// Test bucket-specific assignment
		err = manager.SetBucketKMSProvider("test-bucket", "openbao-test")
		require.NoError(t, err)

		// Test key operations through manager
		ctx := context.Background()
		resp, err := manager.GenerateDataKeyForBucket(ctx, "test-bucket", "test-key-1", kms.KeySpecAES256, map[string]string{
			"bucket": "test-bucket",
		})
		require.NoError(t, err)
		require.NotNil(t, resp)

		assert.Equal(t, "test-key-1", resp.KeyID)
		assert.Len(t, resp.Plaintext, 32)

		// Test decryption through manager
		decResp, err := manager.DecryptForBucket(ctx, "test-bucket", resp.CiphertextBlob, map[string]string{
			"bucket": "test-bucket",
		})
		require.NoError(t, err)
		assert.Equal(t, resp.Plaintext, decResp.Plaintext)

		// Test health check
		health := manager.GetKMSHealth(ctx)
		assert.Contains(t, health, "openbao-test")
		assert.NoError(t, health["openbao-test"]) // Should be healthy

		// Cleanup
		manager.Close()
	})
}

// Benchmark tests for performance
func BenchmarkOpenBaoKMS_GenerateDataKey(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	// Start OpenBao server
	_, cleanup := setupOpenBao(&testing.T{})
	defer cleanup()

	setupTransitEngine(&testing.T{})

	config := &testConfig{
		config: map[string]interface{}{
			"address":      OpenBaoAddress,
			"token":        OpenBaoToken,
			"transit_path": TransitPath,
		},
	}

	provider, err := kms.GetProvider("openbao", config)
	if err != nil {
		b.Fatal(err)
	}
	defer provider.Close()

	ctx := context.Background()
	req := &kms.GenerateDataKeyRequest{
		KeyID:   "test-key-1",
		KeySpec: kms.KeySpecAES256,
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := provider.GenerateDataKey(ctx, req)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkOpenBaoKMS_Decrypt(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	// Start OpenBao server
	_, cleanup := setupOpenBao(&testing.T{})
	defer cleanup()

	setupTransitEngine(&testing.T{})

	config := &testConfig{
		config: map[string]interface{}{
			"address":      OpenBaoAddress,
			"token":        OpenBaoToken,
			"transit_path": TransitPath,
		},
	}

	provider, err := kms.GetProvider("openbao", config)
	if err != nil {
		b.Fatal(err)
	}
	defer provider.Close()

	ctx := context.Background()

	// Generate a data key for decryption testing
	genResp, err := provider.GenerateDataKey(ctx, &kms.GenerateDataKeyRequest{
		KeyID:   "test-key-1",
		KeySpec: kms.KeySpecAES256,
	})
	if err != nil {
		b.Fatal(err)
	}

	decReq := &kms.DecryptRequest{
		CiphertextBlob: genResp.CiphertextBlob,
		EncryptionContext: map[string]string{
			"openbao:key:name": "test-key-1",
		},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := provider.Decrypt(ctx, decReq)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
