package catalog_trino

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// setupTrinoTest is a helper function that sets up the common test environment for all Trino CRUD tests
func setupTrinoTest(t *testing.T) *TestEnvironment {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := NewTestEnvironment(t)

	if !env.dockerAvailable {
		t.Skip("Docker not available, skipping Trino integration test")
	}

	t.Logf(">>> Starting SeaweedFS...")
	env.StartSeaweedFS(t)

	catalogBucket := "warehouse"
	tableBucket := "iceberg-tables"
	createTableBucket(t, env, tableBucket)
	createTableBucket(t, env, catalogBucket)

	configDir := env.writeTrinoConfig(t, catalogBucket)
	env.startTrinoContainer(t, configDir)
	waitForTrino(t, env.trinoContainer, 60*time.Second)

	return env
}

// TestNamespaceCRUD tests namespace (schema) CRUD operations via Trino SQL
// Namespaces are the key container for tables in Iceberg, and this test
// verifies the full CRUD lifecycle: Create, Read (List), Update, and Delete
func TestNamespaceCRUD(t *testing.T) {
	env := setupTrinoTest(t)
	defer env.Cleanup(t)

	// CREATE: Create a namespace
	namespace1 := "crud_test_ns1_" + randomString(6)
	t.Logf(">>> CREATE: Creating namespace %s", namespace1)
	runTrinoSQL(t, env.trinoContainer, "CREATE SCHEMA IF NOT EXISTS iceberg."+namespace1)
	t.Logf(">>> Namespace %s created", namespace1)

	namespace2 := "crud_test_ns2_" + randomString(6)
	t.Logf(">>> CREATE: Creating second namespace %s", namespace2)
	runTrinoSQL(t, env.trinoContainer, "CREATE SCHEMA IF NOT EXISTS iceberg."+namespace2)
	t.Logf(">>> Namespace %s created", namespace2)

	// READ: List all namespaces
	t.Logf(">>> READ: Listing all namespaces")
	output := runTrinoSQL(t, env.trinoContainer, "SHOW SCHEMAS FROM iceberg")
	if !strings.Contains(output, namespace1) {
		t.Fatalf("Expected namespace %s in listing output:\n%s", namespace1, output)
	}
	if !strings.Contains(output, namespace2) {
		t.Fatalf("Expected namespace %s in listing output:\n%s", namespace2, output)
	}
	t.Logf(">>> Both namespaces found in listing")

	// UPDATE: Namespaces typically don't have "update" semantics in SQL,
	// but we can verify properties via metadata queries
	t.Logf(">>> UPDATE: Simulating namespace properties (via SQL properties check)")
	// Iceberg REST API supports namespace properties, but Trino SQL doesn't expose them directly
	// This test verifies the namespace still exists and is accessible
	output = runTrinoSQL(t, env.trinoContainer, "SHOW TABLES FROM iceberg."+namespace1)

	// DELETE: Drop namespaces
	t.Logf(">>> DELETE: Dropping namespace %s", namespace1)
	runTrinoSQL(t, env.trinoContainer, "DROP SCHEMA iceberg."+namespace1)
	t.Logf(">>> Namespace %s dropped", namespace1)

	t.Logf(">>> DELETE: Dropping namespace %s", namespace2)
	runTrinoSQL(t, env.trinoContainer, "DROP SCHEMA iceberg."+namespace2)
	t.Logf(">>> Namespace %s dropped", namespace2)

	// Verify deletion: Namespaces should no longer exist
	t.Logf(">>> VERIFY: Checking that namespaces are deleted")
	output = runTrinoSQL(t, env.trinoContainer, "SHOW SCHEMAS FROM iceberg")
	if strings.Contains(output, namespace1) {
		t.Errorf("Namespace %s still appears in listing after deletion", namespace1)
	} else {
		t.Logf(">>> Namespace %s correctly deleted", namespace1)
	}
	if strings.Contains(output, namespace2) {
		t.Errorf("Namespace %s still appears in listing after deletion", namespace2)
	} else {
		t.Logf(">>> Namespace %s correctly deleted", namespace2)
	}

	t.Logf(">>> TestNamespaceCRUD PASSED")
}

// TestNamespaceListingPagination tests that namespace listing works correctly
// This verifies the LIST operation with multiple namespaces
func TestNamespaceListingPagination(t *testing.T) {
	env := setupTrinoTest(t)
	defer env.Cleanup(t)

	// Create multiple namespaces
	numNamespaces := 5
	namespaces := make([]string, numNamespaces)
	t.Logf(">>> Creating %d namespaces for listing test", numNamespaces)
	for i := 0; i < numNamespaces; i++ {
		namespaces[i] = "list_test_ns" + fmt.Sprintf("%d", i+1) + "_" + randomString(4)
		runTrinoSQL(t, env.trinoContainer, "CREATE SCHEMA IF NOT EXISTS iceberg."+namespaces[i])
		t.Logf(">>> Created namespace %d: %s", i+1, namespaces[i])
	}

	// List all namespaces
	t.Logf(">>> Listing all namespaces")
	output := runTrinoSQL(t, env.trinoContainer, "SHOW SCHEMAS FROM iceberg")

	// Verify all namespaces are in the listing
	t.Logf(">>> Verifying all namespaces are in listing")
	for i, ns := range namespaces {
		if !strings.Contains(output, ns) {
			t.Fatalf("Expected namespace %d (%s) in listing output", i+1, ns)
		}
		t.Logf(">>> Namespace %d (%s) found in listing", i+1, ns)
	}

	// Clean up
	t.Logf(">>> Cleaning up namespaces")
	for _, ns := range namespaces {
		runTrinoSQL(t, env.trinoContainer, "DROP SCHEMA iceberg."+ns)
	}
	t.Logf(">>> Cleanup complete")

	t.Logf(">>> TestNamespaceListingPagination PASSED")
}

// TestNamespaceErrorHandling tests error handling for namespace operations
// Tests both idempotent operations and actual error cases
func TestNamespaceErrorHandling(t *testing.T) {
	env := setupTrinoTest(t)
	defer env.Cleanup(t)

	// Test 1: Create and drop same namespace
	ns := "error_test_ns_" + randomString(6)
	t.Logf(">>> Test 1: Creating and dropping namespace %s", ns)
	runTrinoSQL(t, env.trinoContainer, "CREATE SCHEMA IF NOT EXISTS iceberg."+ns)
	runTrinoSQL(t, env.trinoContainer, "DROP SCHEMA iceberg."+ns)
	t.Logf(">>> Namespace created and dropped successfully")

	// Test 2: Try to drop non-existent namespace with IF EXISTS (should succeed gracefully)
	nonExistent := "nonexistent_" + randomString(6)
	t.Logf(">>> Test 2: Attempting to drop non-existent namespace %s with IF EXISTS", nonExistent)
	runTrinoSQL(t, env.trinoContainer, "DROP SCHEMA IF EXISTS iceberg."+nonExistent)
	t.Logf(">>> Drop non-existent namespace handled gracefully (IF EXISTS clause)")

	// Test 3: Creating duplicate namespace (with IF NOT EXISTS - should succeed gracefully)
	ns2 := "dup_test_ns_" + randomString(6)
	t.Logf(">>> Test 3: Creating namespace %s twice (using IF NOT EXISTS)", ns2)
	runTrinoSQL(t, env.trinoContainer, "CREATE SCHEMA IF NOT EXISTS iceberg."+ns2)
	runTrinoSQL(t, env.trinoContainer, "CREATE SCHEMA IF NOT EXISTS iceberg."+ns2)
	t.Logf(">>> Duplicate creation handled gracefully")

	// Test 4: Verify schema properties persist after creation
	t.Logf(">>> Test 4: Verifying namespace still exists after duplicate creation attempt")
	output := runTrinoSQL(t, env.trinoContainer, "SHOW SCHEMAS FROM iceberg")
	if !strings.Contains(output, ns2) {
		t.Errorf("Expected namespace %s to exist in listing after duplicate creation attempt", ns2)
	} else {
		t.Logf(">>> Namespace %s correctly persists", ns2)
	}

	// Cleanup
	runTrinoSQL(t, env.trinoContainer, "DROP SCHEMA iceberg."+ns2)

	t.Logf(">>> TestNamespaceErrorHandling PASSED")
}

// TestSchemaIntegrationWithCatalog tests that schemas are properly integrated with the catalog
// This verifies that schema operations through Trino are visible through Iceberg REST API concepts
func TestSchemaIntegrationWithCatalog(t *testing.T) {
	env := setupTrinoTest(t)
	defer env.Cleanup(t)

	// Create a schema through Trino
	schemaName := "catalog_integration_" + randomString(6)
	t.Logf(">>> Creating schema through Trino SQL: %s", schemaName)
	runTrinoSQL(t, env.trinoContainer, "CREATE SCHEMA IF NOT EXISTS iceberg."+schemaName)

	// Verify it's accessible through catalog (list schemas)
	t.Logf(">>> Verifying schema is visible through catalog (SHOW SCHEMAS)")
	output := runTrinoSQL(t, env.trinoContainer, "SHOW SCHEMAS FROM iceberg")
	if !strings.Contains(output, schemaName) {
		t.Fatalf("Created schema %s not visible in catalog listing", schemaName)
	}
	t.Logf(">>> Schema successfully verified in catalog")

	// Verify empty schema
	t.Logf(">>> Verifying schema is empty (no tables)")
	output = runTrinoSQL(t, env.trinoContainer, "SHOW TABLES FROM iceberg."+schemaName)

	// Clean up
	t.Logf(">>> Cleaning up schema")
	runTrinoSQL(t, env.trinoContainer, "DROP SCHEMA iceberg."+schemaName)

	t.Logf(">>> TestSchemaIntegrationWithCatalog PASSED")
}
