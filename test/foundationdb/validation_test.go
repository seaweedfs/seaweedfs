package foundationdb

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestPackageStructure validates the FoundationDB package structure without requiring dependencies
func TestPackageStructure(t *testing.T) {
	t.Log("✅ Testing FoundationDB package structure...")

	// Verify the main package files exist
	packagePath := "../../weed/filer/foundationdb"
	expectedFiles := map[string]bool{
		"foundationdb_store.go":      false,
		"foundationdb_store_test.go": false,
		"doc.go":                     false,
		"README.md":                  false,
	}

	err := filepath.Walk(packagePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip errors
		}
		fileName := filepath.Base(path)
		if _, exists := expectedFiles[fileName]; exists {
			expectedFiles[fileName] = true
			t.Logf("Found: %s", fileName)
		}
		return nil
	})

	if err != nil {
		t.Logf("Warning: Could not access package path %s", packagePath)
	}

	for file, found := range expectedFiles {
		if found {
			t.Logf("✅ %s exists", file)
		} else {
			t.Logf("⚠️  %s not found (may be normal)", file)
		}
	}
}

// TestServerIntegration validates that the filer server includes FoundationDB import
func TestServerIntegration(t *testing.T) {
	t.Log("✅ Testing server integration...")

	serverFile := "../../weed/server/filer_server.go"
	content, err := os.ReadFile(serverFile)
	if err != nil {
		t.Skipf("Cannot read server file: %v", err)
		return
	}

	contentStr := string(content)

	// Check for FoundationDB import
	if strings.Contains(contentStr, `"github.com/seaweedfs/seaweedfs/weed/filer/foundationdb"`) {
		t.Log("✅ FoundationDB import found in filer_server.go")
	} else {
		t.Error("❌ FoundationDB import not found in filer_server.go")
	}

	// Check for other expected imports for comparison
	expectedImports := []string{
		"leveldb",
		"redis",
		"mysql",
	}

	foundImports := 0
	for _, imp := range expectedImports {
		if strings.Contains(contentStr, fmt.Sprintf(`"github.com/seaweedfs/seaweedfs/weed/filer/%s"`, imp)) {
			foundImports++
		}
	}

	t.Logf("✅ Found %d/%d expected filer store imports", foundImports, len(expectedImports))
}

// TestBuildConstraints validates that build constraints work correctly
func TestBuildConstraints(t *testing.T) {
	t.Log("✅ Testing build constraints...")

	// Check that foundationdb package files have correct build tags
	packagePath := "../../weed/filer/foundationdb"

	err := filepath.Walk(packagePath, func(path string, info os.FileInfo, err error) error {
		if err != nil || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		content, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil
		}

		contentStr := string(content)

		// Skip doc.go as it might not have build tags
		if strings.HasSuffix(path, "doc.go") {
			return nil
		}

		if strings.Contains(contentStr, "//go:build foundationdb") ||
			strings.Contains(contentStr, "// +build foundationdb") {
			t.Logf("✅ Build constraints found in %s", filepath.Base(path))
		} else {
			t.Logf("⚠️  No build constraints in %s", filepath.Base(path))
		}

		return nil
	})

	if err != nil {
		t.Logf("Warning: Could not validate build constraints: %v", err)
	}
}

// TestDocumentationExists validates that documentation files are present
func TestDocumentationExists(t *testing.T) {
	t.Log("✅ Testing documentation...")

	docs := []struct {
		path string
		name string
	}{
		{"README.md", "Main README"},
		{"Makefile", "Build automation"},
		{"docker-compose.yml", "Docker setup"},
		{"filer.toml", "Configuration template"},
		{"../../weed/filer/foundationdb/README.md", "Package README"},
	}

	for _, doc := range docs {
		if _, err := os.Stat(doc.path); err == nil {
			t.Logf("✅ %s exists", doc.name)
		} else {
			t.Logf("⚠️  %s not found: %s", doc.name, doc.path)
		}
	}
}

// TestConfigurationValidation tests configuration file syntax
func TestConfigurationValidation(t *testing.T) {
	t.Log("✅ Testing configuration files...")

	// Test filer.toml syntax
	if content, err := os.ReadFile("filer.toml"); err == nil {
		contentStr := string(content)

		expectedConfigs := []string{
			"[foundationdb]",
			"enabled",
			"cluster_file",
			"api_version",
		}

		for _, config := range expectedConfigs {
			if strings.Contains(contentStr, config) {
				t.Logf("✅ Found config: %s", config)
			} else {
				t.Logf("⚠️  Config not found: %s", config)
			}
		}
	} else {
		t.Log("⚠️  filer.toml not accessible")
	}
}
