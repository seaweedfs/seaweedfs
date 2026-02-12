package catalog_risingwave

import (
	"fmt"
	"strings"
	"testing"
)

func TestRisingWaveIcebergCatalog(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	if !env.dockerAvailable {
		t.Skip("Docker not available, skipping RisingWave integration test")
	}

	fmt.Printf(">>> Starting SeaweedFS...\n")
	env.StartSeaweedFS(t)
	fmt.Printf(">>> SeaweedFS started.\n")

	tableBucket := "iceberg-tables"
	fmt.Printf(">>> Creating table bucket: %s\n", tableBucket)
	createTableBucket(t, env, tableBucket)

	fmt.Printf(">>> Starting RisingWave...\n")
	env.StartRisingWave(t)
	fmt.Printf(">>> RisingWave started.\n")

	// Create Iceberg namespace
	createIcebergNamespace(t, env, tableBucket, "default")

	// Create a catalog in RisingWave that points to SeaweedFS Iceberg REST API
	icebergUri := env.dockerIcebergEndpoint()
	s3Endpoint := env.dockerS3Endpoint()

	tableName := "test_table_" + randomString(6)
	createIcebergTable(t, env, tableBucket, "default", tableName)

	sourceName := "test_source_" + randomString(6)
	createSourceSql := fmt.Sprintf(`
CREATE SOURCE %s WITH (
    connector = 'iceberg',
    catalog.type = 'rest',
    catalog.uri = '%s',
    catalog.name = 'default',
    database.name = 'default',
    table.name = '%s',
    warehouse.path = 's3://%s',
    s3.endpoint = '%s',
    s3.region = 'us-east-1',
    s3.access.key = '%s',
    s3.secret.key = '%s',
    s3.path.style.access = 'true',
    catalog.rest.sigv4_enabled = 'true',
    catalog.rest.signing_region = 'us-east-1',
    catalog.rest.signing_name = 's3'
);`, sourceName, icebergUri, tableName, tableBucket, s3Endpoint, env.accessKey, env.secretKey)

	fmt.Printf(">>> Creating source %s...\n", sourceName)
	runRisingWaveSQL(t, env.risingwaveContainer, createSourceSql)

	showSourcesOutput := runRisingWaveSQL(t, env.risingwaveContainer, "SHOW SOURCES;")
	if !strings.Contains(showSourcesOutput, sourceName) {
		t.Fatalf("Expected source %s in SHOW SOURCES output:\n%s", sourceName, showSourcesOutput)
	}

	describeOutput := runRisingWaveSQL(t, env.risingwaveContainer, fmt.Sprintf("DESCRIBE %s;", sourceName))
	if !strings.Contains(describeOutput, "id") || !strings.Contains(describeOutput, "name") {
		t.Fatalf("Expected id/name columns in DESCRIBE output:\n%s", describeOutput)
	}

	runRisingWaveSQL(t, env.risingwaveContainer, fmt.Sprintf("SELECT * FROM %s LIMIT 0;", sourceName))

	fmt.Printf(">>> RisingWave Iceberg Catalog test passed!\n")
}
