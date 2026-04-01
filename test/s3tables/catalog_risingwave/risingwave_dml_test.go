package catalog_risingwave

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestRisingWaveIcebergDML(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env := NewTestEnvironment(t)
	defer env.Cleanup(t)

	if !env.dockerAvailable {
		t.Skip("Docker not available, skipping RisingWave integration test")
	}

	t.Log(">>> Starting SeaweedFS...")
	env.StartSeaweedFS(t)
	t.Log(">>> SeaweedFS started.")

	tableBucket := "iceberg-tables"
	t.Logf(">>> Creating table bucket: %s", tableBucket)
	createTableBucket(t, env, tableBucket)

	t.Log(">>> Starting RisingWave...")
	env.StartRisingWave(t)
	t.Log(">>> RisingWave started.")

	// Create Iceberg namespace
	createIcebergNamespace(t, env, "default")

	icebergUri := env.dockerIcebergEndpoint()
	s3Endpoint := env.dockerS3Endpoint()

	// 1. Test INSERT (Append-only)
	t.Run("TestInsert", func(t *testing.T) {
		tableName := "test_insert_" + randomString(6)
		createIcebergTable(t, env, tableBucket, "default", tableName)

		rwTableName := "rw_insert_" + randomString(6)
		runRisingWaveSQL(t, env.postgresSidecar, fmt.Sprintf("CREATE TABLE %s (id int, name varchar);", rwTableName))

		sinkName := "test_sink_insert_" + randomString(6)
		createSinkSql := fmt.Sprintf(`
CREATE SINK %s FROM %s
WITH (
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
    catalog.rest.signing_name = 's3',
    type = 'append-only',
    force_append_only = 'true'
);`, sinkName, rwTableName, icebergUri, tableName, tableBucket, s3Endpoint, env.accessKey, env.secretKey)

		t.Logf(">>> Creating sink %s...", sinkName)
		runRisingWaveSQL(t, env.postgresSidecar, createSinkSql)

		t.Log(">>> Inserting into RisingWave table...")
		runRisingWaveSQL(t, env.postgresSidecar, fmt.Sprintf("INSERT INTO %s VALUES (1, 'Alice'), (2, 'Bob');", rwTableName))
		runRisingWaveSQL(t, env.postgresSidecar, "FLUSH;")

		// Verify with Source
		sourceName := "test_source_insert_" + randomString(6)
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

		runRisingWaveSQL(t, env.postgresSidecar, createSourceSql)

		t.Log(">>> Selecting from source to verify INSERT...")
		verifyQuery(t, env, sourceName, "1 | Alice", "2 | Bob")
	})

	// 2. Test UPSERT (Update/Delete)
	t.Run("TestUpsert", func(t *testing.T) {
		tableName := "test_upsert_" + randomString(6)
		// We need a table with PK for upsert to work effectively in RW logic,
		// effectively maps to Iceberg v2 table.
		createIcebergTable(t, env, tableBucket, "default", tableName)

		rwTableName := "rw_upsert_" + randomString(6)
		runRisingWaveSQL(t, env.postgresSidecar, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, name varchar);", rwTableName))

		sinkName := "test_sink_upsert_" + randomString(6)
		createSinkSql := fmt.Sprintf(`
CREATE SINK %s FROM %s
WITH (
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
    catalog.rest.signing_name = 's3',
    type = 'upsert', -- Upsert mode
    primary_key = 'id'
);`, sinkName, rwTableName, icebergUri, tableName, tableBucket, s3Endpoint, env.accessKey, env.secretKey)

		t.Logf(">>> Creating upsert sink %s...", sinkName)
		runRisingWaveSQL(t, env.postgresSidecar, createSinkSql)

		t.Log(">>> Inserting initial data...")
		runRisingWaveSQL(t, env.postgresSidecar, fmt.Sprintf("INSERT INTO %s VALUES (1, 'Charlie'), (2, 'Dave');", rwTableName))
		runRisingWaveSQL(t, env.postgresSidecar, "FLUSH;")

		// Update 1, Delete 2
		t.Log(">>> Updating and Deleting data...")
		runRisingWaveSQL(t, env.postgresSidecar, fmt.Sprintf("UPDATE %s SET name = 'Charles' WHERE id = 1;", rwTableName))
		runRisingWaveSQL(t, env.postgresSidecar, fmt.Sprintf("DELETE FROM %s WHERE id = 2;", rwTableName))
		runRisingWaveSQL(t, env.postgresSidecar, "FLUSH;")

		// Verify with Source
		sourceName := "test_source_upsert_" + randomString(6)
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

		runRisingWaveSQL(t, env.postgresSidecar, createSourceSql)

		t.Log(">>> Selecting from source to verify UPSERT...")
		// Should see (1, 'Charles') and NOT (2, 'Dave')
		verifyQuery(t, env, sourceName, "1 | Charles")
		verifyQueryAbsence(t, env, sourceName, "2 | Dave")
	})
}

func verifyQuery(t *testing.T, env *TestEnvironment, sourceName string, expectedSubstrings ...string) {
	t.Helper()
	var output string
	for i := 0; i < 15; i++ {
		output = runRisingWaveSQL(t, env.postgresSidecar, fmt.Sprintf("SELECT * FROM %s ORDER BY id;", sourceName))
		allFound := true
		for _, s := range expectedSubstrings {
			if !strings.Contains(output, s) {
				allFound = false
				break
			}
		}
		if allFound {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("Failed to find expected data %v in output:\n%s", expectedSubstrings, output)
}

func verifyQueryAbsence(t *testing.T, env *TestEnvironment, sourceName string, unexpectedSubstrings ...string) {
	t.Helper()
	var output string
	for i := 0; i < 15; i++ {
		output = runRisingWaveSQL(t, env.postgresSidecar, fmt.Sprintf("SELECT * FROM %s ORDER BY id;", sourceName))
		noneFound := true
		for _, s := range unexpectedSubstrings {
			if strings.Contains(output, s) {
				noneFound = false
				break
			}
		}
		if noneFound {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("Found unexpected data %v in output:\n%s", unexpectedSubstrings, output)
}
