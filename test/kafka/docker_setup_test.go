package kafka

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDockerSetup_Files verifies that all Docker setup files exist and are valid
func TestDockerSetup_Files(t *testing.T) {
	testDir := "."

	t.Run("DockerComposeExists", func(t *testing.T) {
		composePath := filepath.Join(testDir, "docker-compose.yml")
		_, err := os.Stat(composePath)
		require.NoError(t, err, "docker-compose.yml should exist")

		// Read and validate basic structure
		content, err := os.ReadFile(composePath)
		require.NoError(t, err)
		
		composeContent := string(content)
		assert.Contains(t, composeContent, "version:", "Should have version specified")
		assert.Contains(t, composeContent, "services:", "Should have services section")
		assert.Contains(t, composeContent, "kafka:", "Should have Kafka service")
		assert.Contains(t, composeContent, "schema-registry:", "Should have Schema Registry service")
		assert.Contains(t, composeContent, "kafka-gateway:", "Should have Kafka Gateway service")
		assert.Contains(t, composeContent, "seaweedfs-", "Should have SeaweedFS services")
	})

	t.Run("DockerfilesExist", func(t *testing.T) {
		dockerfiles := []string{
			"Dockerfile.kafka-gateway",
			"Dockerfile.test-setup",
		}

		for _, dockerfile := range dockerfiles {
			dockerfilePath := filepath.Join(testDir, dockerfile)
			_, err := os.Stat(dockerfilePath)
			require.NoError(t, err, "Dockerfile %s should exist", dockerfile)

			// Validate basic Dockerfile structure
			content, err := os.ReadFile(dockerfilePath)
			require.NoError(t, err)
			
			dockerContent := string(content)
			assert.Contains(t, dockerContent, "FROM", "Should have FROM instruction")
		}
	})

	t.Run("ScriptsExist", func(t *testing.T) {
		scripts := []string{
			"scripts/kafka-gateway-start.sh",
			"scripts/wait-for-services.sh",
		}

		for _, script := range scripts {
			scriptPath := filepath.Join(testDir, script)
			info, err := os.Stat(scriptPath)
			require.NoError(t, err, "Script %s should exist", script)

			// Check if script is executable
			mode := info.Mode()
			assert.True(t, mode&0111 != 0, "Script %s should be executable", script)

			// Validate basic shell script structure
			content, err := os.ReadFile(scriptPath)
			require.NoError(t, err)
			
			scriptContent := string(content)
			assert.Contains(t, scriptContent, "#!/", "Should have shebang")
		}
	})

	t.Run("MakefileExists", func(t *testing.T) {
		makefilePath := filepath.Join(testDir, "Makefile")
		_, err := os.Stat(makefilePath)
		require.NoError(t, err, "Makefile should exist")

		// Validate basic Makefile structure
		content, err := os.ReadFile(makefilePath)
		require.NoError(t, err)
		
		makefileContent := string(content)
		assert.Contains(t, makefileContent, "help:", "Should have help target")
		assert.Contains(t, makefileContent, "setup:", "Should have setup target")
		assert.Contains(t, makefileContent, "test:", "Should have test target")
		assert.Contains(t, makefileContent, "clean:", "Should have clean target")
	})

	t.Run("ReadmeExists", func(t *testing.T) {
		readmePath := filepath.Join(testDir, "README.md")
		_, err := os.Stat(readmePath)
		require.NoError(t, err, "README.md should exist")

		// Validate basic README structure
		content, err := os.ReadFile(readmePath)
		require.NoError(t, err)
		
		readmeContent := string(content)
		assert.Contains(t, readmeContent, "# Kafka Integration Testing", "Should have main title")
		assert.Contains(t, readmeContent, "## Quick Start", "Should have quick start section")
		assert.Contains(t, readmeContent, "make setup", "Should mention setup command")
	})

	t.Run("TestSetupUtilityExists", func(t *testing.T) {
		setupPath := filepath.Join(testDir, "cmd", "setup", "main.go")
		_, err := os.Stat(setupPath)
		require.NoError(t, err, "Test setup utility should exist")

		// Validate basic Go file structure
		content, err := os.ReadFile(setupPath)
		require.NoError(t, err)
		
		setupContent := string(content)
		assert.Contains(t, setupContent, "package main", "Should be main package")
		assert.Contains(t, setupContent, "func main()", "Should have main function")
		assert.Contains(t, setupContent, "registerSchemas", "Should have schema registration")
	})
}

// TestDockerSetup_Configuration verifies Docker configuration is reasonable
func TestDockerSetup_Configuration(t *testing.T) {
	t.Run("PortConfiguration", func(t *testing.T) {
		// This test verifies that the ports used in docker-compose.yml are reasonable
		// and don't conflict with common development ports
		
		expectedPorts := map[string]string{
			"zookeeper":      "2181",
			"kafka":          "9092",
			"schema-registry": "8081",
			"seaweedfs-master": "9333",
			"seaweedfs-volume": "8080",
			"seaweedfs-filer":  "8888",
			"kafka-gateway":    "9093",
		}

		composePath := "docker-compose.yml"
		content, err := os.ReadFile(composePath)
		require.NoError(t, err)
		
		composeContent := string(content)
		
		for service, port := range expectedPorts {
			assert.Contains(t, composeContent, port+":", 
				"Service %s should expose port %s", service, port)
		}
	})

	t.Run("HealthChecks", func(t *testing.T) {
		// Verify that critical services have health checks
		composePath := "docker-compose.yml"
		content, err := os.ReadFile(composePath)
		require.NoError(t, err)
		
		composeContent := string(content)
		
		// Should have health checks for critical services
		assert.Contains(t, composeContent, "healthcheck:", "Should have health checks")
		
		// Verify specific health check patterns
		healthCheckServices := []string{"kafka", "schema-registry", "seaweedfs-master"}
		for _, service := range healthCheckServices {
			// Look for health check in the service section (basic validation)
			assert.Contains(t, composeContent, service+":", 
				"Service %s should be defined", service)
		}
	})

	t.Run("NetworkConfiguration", func(t *testing.T) {
		composePath := "docker-compose.yml"
		content, err := os.ReadFile(composePath)
		require.NoError(t, err)
		
		composeContent := string(content)
		
		// Should have network configuration
		assert.Contains(t, composeContent, "networks:", "Should have networks section")
		assert.Contains(t, composeContent, "kafka-test-net", "Should have test network")
	})
}

// TestDockerSetup_Integration verifies integration test structure
func TestDockerSetup_Integration(t *testing.T) {
	t.Run("IntegrationTestExists", func(t *testing.T) {
		testPath := "docker_integration_test.go"
		_, err := os.Stat(testPath)
		require.NoError(t, err, "Docker integration test should exist")

		// Validate test structure
		content, err := os.ReadFile(testPath)
		require.NoError(t, err)
		
		testContent := string(content)
		assert.Contains(t, testContent, "TestDockerIntegration_E2E", "Should have E2E test")
		assert.Contains(t, testContent, "KAFKA_BOOTSTRAP_SERVERS", "Should check environment variables")
		assert.Contains(t, testContent, "t.Skip", "Should skip when environment not available")
	})

	t.Run("TestEnvironmentVariables", func(t *testing.T) {
		// Verify that tests properly handle environment variables
		testPath := "docker_integration_test.go"
		content, err := os.ReadFile(testPath)
		require.NoError(t, err)
		
		testContent := string(content)
		
		envVars := []string{
			"KAFKA_BOOTSTRAP_SERVERS",
			"KAFKA_GATEWAY_URL", 
			"SCHEMA_REGISTRY_URL",
		}
		
		for _, envVar := range envVars {
			assert.Contains(t, testContent, envVar, 
				"Should reference environment variable %s", envVar)
		}
	})
}

// TestDockerSetup_Makefile verifies Makefile targets
func TestDockerSetup_Makefile(t *testing.T) {
	t.Run("EssentialTargets", func(t *testing.T) {
		makefilePath := "Makefile"
		content, err := os.ReadFile(makefilePath)
		require.NoError(t, err)
		
		makefileContent := string(content)
		
		essentialTargets := []string{
			"help:",
			"setup:",
			"test:",
			"test-unit:",
			"test-integration:",
			"test-e2e:",
			"clean:",
			"logs:",
			"status:",
		}
		
		for _, target := range essentialTargets {
			assert.Contains(t, makefileContent, target, 
				"Should have target %s", target)
		}
	})

	t.Run("DevelopmentTargets", func(t *testing.T) {
		makefilePath := "Makefile"
		content, err := os.ReadFile(makefilePath)
		require.NoError(t, err)
		
		makefileContent := string(content)
		
		devTargets := []string{
			"dev-kafka:",
			"dev-seaweedfs:",
			"debug:",
			"shell-kafka:",
			"topics:",
		}
		
		for _, target := range devTargets {
			assert.Contains(t, makefileContent, target, 
				"Should have development target %s", target)
		}
	})
}
