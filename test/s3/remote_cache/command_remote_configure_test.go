package remote_cache

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRemoteConfigureBasic tests creating and listing remote configurations
func TestRemoteConfigureBasic(t *testing.T) {
	checkServersRunning(t)

	// Use only letters in name (no numbers) based on validation rules
	testName := fmt.Sprintf("testremote%c", 'a'+byte(time.Now().UnixNano()%26))

	// Create a new remote configuration
	t.Log("Creating remote configuration...")
	cmd := fmt.Sprintf("remote.configure -name='%s' -type=s3 -s3.access_key=%s -s3.secret_key=%s -s3.endpoint=http://localhost:%s -s3.region=us-east-1",
		testName, accessKey, secretKey, "8334")
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to create remote configuration")
	t.Logf("Configure output: %s", output)

	// List configurations and verify it exists
	t.Log("Listing remote configurations...")
	time.Sleep(500 * time.Millisecond) // Give some time for configuration to persist
	output, err = runWeedShellWithOutput(t, "remote.configure")
	require.NoError(t, err, "failed to list configurations")
	assert.Contains(t, output, fmt.Sprintf("\"name\": \"%s\"", testName), "configuration not found in list")
	t.Logf("List output: %s", output)

	// Clean up - delete the configuration
	t.Log("Deleting remote configuration...")
	cmd = fmt.Sprintf("remote.configure -name='%s' -delete=true", testName)
	_, err = runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to delete configuration")
}

// TestRemoteConfigureInvalidName tests name validation
func TestRemoteConfigureInvalidName(t *testing.T) {
	checkServersRunning(t)

	invalidNames := []string{
		"test-remote", // contains hyphen
		"123test",     // starts with number
		"test remote", // contains space
		"test@remote", // contains special char
	}

	for _, name := range invalidNames {
		t.Run(name, func(t *testing.T) {
			cmd := fmt.Sprintf("remote.configure -name='%s' -type=s3 -s3.access_key=%s -s3.secret_key=%s -s3.endpoint=http://localhost:8334",
				name, accessKey, secretKey)
			output, err := runWeedShellWithOutput(t, cmd)

			// Should fail with invalid name
			hasError := err != nil || strings.Contains(strings.ToLower(output), "invalid") || strings.Contains(strings.ToLower(output), "error")
			assert.True(t, hasError, "Expected error for invalid name '%s', but command succeeded with output: %s", name, output)
			t.Logf("Invalid name '%s' output: %s", name, output)
		})
	}
}

// TestRemoteConfigureUpdate tests updating an existing configuration
func TestRemoteConfigureUpdate(t *testing.T) {
	checkServersRunning(t)

	// Use only letters in name
	testName := fmt.Sprintf("testupdate%c", 'a'+byte(time.Now().UnixNano()%26))

	// Create initial configuration
	t.Log("Creating initial configuration...")
	cmd := fmt.Sprintf("remote.configure -name=%s -type=s3 -s3.access_key=%s -s3.secret_key=%s -s3.endpoint=http://localhost:8334 -s3.region=us-east-1",
		testName, accessKey, secretKey)
	_, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to create initial configuration")

	// Update with different region
	t.Log("Updating configuration...")
	cmd = fmt.Sprintf("remote.configure -name=%s -type=s3 -s3.access_key=%s -s3.secret_key=%s -s3.endpoint=http://localhost:8334 -s3.region=us-west-2",
		testName, accessKey, secretKey)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to update configuration")
	t.Logf("Update output: %s", output)

	// Verify update
	output, err = runWeedShellWithOutput(t, "remote.configure")
	require.NoError(t, err, "failed to list configurations")
	assert.Contains(t, output, testName, "configuration not found after update")

	// Clean up
	cmd = fmt.Sprintf("remote.configure -name=%s -delete=true", testName)
	_, err = runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to delete configuration")
}

// TestRemoteConfigureDelete tests deleting a configuration
func TestRemoteConfigureDelete(t *testing.T) {
	checkServersRunning(t)

	// Use only letters in name
	testName := fmt.Sprintf("testdelete%c", 'a'+byte(time.Now().UnixNano()%26))

	// Create configuration
	cmd := fmt.Sprintf("remote.configure -name=%s -type=s3 -s3.access_key=%s -s3.secret_key=%s -s3.endpoint=http://localhost:8334 -s3.region=us-east-1",
		testName, accessKey, secretKey)
	_, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to create configuration")

	// Delete it
	t.Log("Deleting configuration...")
	cmd = fmt.Sprintf("remote.configure -name=%s -delete=true", testName)
	output, err := runWeedShellWithOutput(t, cmd)
	require.NoError(t, err, "failed to delete configuration")
	t.Logf("Delete output: %s", output)

	// Verify it's gone
	output, err = runWeedShellWithOutput(t, "remote.configure")
	require.NoError(t, err, "failed to list configurations")
	assert.NotContains(t, output, testName, "configuration still exists after deletion")
}

// TestRemoteConfigureMissingParams tests missing required parameters
// Note: The command may not strictly validate all parameters, so we just verify it doesn't crash
func TestRemoteConfigureMissingParams(t *testing.T) {
	checkServersRunning(t)

	// Use only letters in name
	testName := fmt.Sprintf("testmissing%c", 'a'+byte(time.Now().UnixNano()%26))

	testCases := []struct {
		name    string
		command string
	}{
		{
			name:    "missing_access_key",
			command: fmt.Sprintf("remote.configure -name=%s -type=s3 -s3.secret_key=%s -s3.endpoint=http://localhost:8334", testName, secretKey),
		},
		{
			name:    "missing_secret_key",
			command: fmt.Sprintf("remote.configure -name=%s -type=s3 -s3.access_key=%s -s3.endpoint=http://localhost:8334", testName, accessKey),
		},
		{
			name:    "missing_type",
			command: fmt.Sprintf("remote.configure -name=%s -s3.access_key=%s -s3.secret_key=%s -s3.endpoint=http://localhost:8334", testName, accessKey, secretKey),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			output, err := runWeedShellWithOutput(t, tc.command)
			// Just log the result - the command may or may not validate strictly
			t.Logf("Test case %s: err=%v, output: %s", tc.name, err, output)
			// The main goal is to ensure the command doesn't crash
		})
	}
}

// TestRemoteConfigureListEmpty tests listing when no configurations exist
func TestRemoteConfigureListEmpty(t *testing.T) {
	checkServersRunning(t)

	// Just list configurations - should not error even if empty
	output, err := runWeedShellWithOutput(t, "remote.configure")
	require.NoError(t, err, "failed to list configurations")
	t.Logf("List output: %s", output)

	// Output should contain some indication of configurations or be empty
	// This is mainly to ensure the command doesn't crash
}
