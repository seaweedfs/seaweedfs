package catalog_trino

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestTrinoBlogOperations(t *testing.T) {
	env := setupTrinoTest(t)
	defer env.Cleanup(t)

	schemaName := "blog_ns_" + randomString(6)
	customersTable := "customers_" + randomString(6)
	trinoCustomersTable := "trino_customers_" + randomString(6)
	warehouseBucket := "iceberg-tables"
	customersLocation := fmt.Sprintf("s3://%s/%s/%s_%s", warehouseBucket, schemaName, customersTable, randomString(6))
	trinoCustomersLocation := fmt.Sprintf("s3://%s/%s/%s_%s", warehouseBucket, schemaName, trinoCustomersTable, randomString(6))

	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS iceberg.%s", schemaName))
	defer runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP SCHEMA IF EXISTS iceberg.%s", schemaName))
	defer runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP TABLE IF EXISTS iceberg.%s.%s", schemaName, trinoCustomersTable))
	defer runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP TABLE IF EXISTS iceberg.%s.%s", schemaName, customersTable))
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP TABLE IF EXISTS iceberg.%s.%s", schemaName, trinoCustomersTable))
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP TABLE IF EXISTS iceberg.%s.%s", schemaName, customersTable))

	createCustomersSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS iceberg.%s.%s (
    customer_sk INT,
    customer_id VARCHAR,
    salutation VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    preferred_cust_flag VARCHAR,
    birth_day INT,
    birth_month INT,
    birth_year INT,
    birth_country VARCHAR,
    login VARCHAR
) WITH (
    format = 'PARQUET',
    sorted_by = ARRAY['customer_id'],
    location = '%s'
)`, schemaName, customersTable, customersLocation)
	runTrinoSQLAllowExists(t, env.trinoContainer, createCustomersSQL)

	insertCustomersSQL := fmt.Sprintf(`INSERT INTO iceberg.%s.%s VALUES
    (1, 'AAAAA', 'Mrs', 'Amanda', 'Olson', 'Y', 8, 4, 1984, 'US', 'aolson'),
    (2, 'AAAAB', 'Mr',  'Leonard', 'Eads',  'N', 22, 6, 2001, 'US', 'leads'),
    (3, 'BAAAA', 'Mr',  'David',   'White', 'Y', 16, 2, 1999, 'US', 'dwhite'),
    (4, 'BBAAA', 'Mr',  'Melvin',  'Lee',   'N', 30, 3, 1973, 'US', 'mlee'),
    (5, 'AACAA', 'Mr',  'Donald',  'Holt',  'N', 2,  6, 1982, 'CA', 'dholt'),
    (6, 'ABAAA', 'Mrs', 'Jacqueline', 'Harvey', 'N', 5, 12, 1988, 'US', 'jharvey'),
    (7, 'BBAAA', 'Ms',  'Debbie',  'Ward',  'N', 6,  1, 2006, 'MX', 'dward'),
    (8, 'ACAAA', 'Mr',  'Tim',     'Strong', 'N', 15, 7, 1976, 'US', 'tstrong')
`, schemaName, customersTable)
	runTrinoSQL(t, env.trinoContainer, insertCustomersSQL)

	countOutput := runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("SELECT count(*) FROM iceberg.%s.%s", schemaName, customersTable))
	rowCount := mustParseCSVInt64(t, countOutput)
	if rowCount != 8 {
		t.Fatalf("expected 8 rows in customers table, got %d", rowCount)
	}

	output := runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("SELECT first_name FROM iceberg.%s.%s WHERE customer_sk = 1", schemaName, customersTable))
	if !strings.Contains(output, "Amanda") {
		t.Fatalf("expected sample query to include Amanda, got: %s", output)
	}

	ctasSQL := fmt.Sprintf(`CREATE TABLE iceberg.%s.%s
WITH (
    format = 'PARQUET',
    location = '%s'
)
AS SELECT * FROM iceberg.%s.%s`, schemaName, trinoCustomersTable, trinoCustomersLocation, schemaName, customersTable)
	ctasInsertSQL := fmt.Sprintf("INSERT INTO iceberg.%s.%s SELECT * FROM iceberg.%s.%s", schemaName, trinoCustomersTable, schemaName, customersTable)
	ctasDeleteSQL := fmt.Sprintf("DELETE FROM iceberg.%s.%s", schemaName, trinoCustomersTable)
	runTrinoCTAS(t, env.trinoContainer, ctasSQL, ctasDeleteSQL, ctasInsertSQL)

	countOutput = runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("SELECT count(*) FROM iceberg.%s.%s", schemaName, trinoCustomersTable))
	rowCount = mustParseCSVInt64(t, countOutput)
	if rowCount != 8 {
		t.Fatalf("expected 8 rows in CTAS table, got %d", rowCount)
	}

	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("ALTER TABLE iceberg.%s.%s ADD COLUMN updated_at TIMESTAMP", schemaName, trinoCustomersTable))
	output = runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DESCRIBE iceberg.%s.%s", schemaName, trinoCustomersTable))
	if !strings.Contains(output, "updated_at") {
		t.Fatalf("expected updated_at column in describe output, got: %s", output)
	}

	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("UPDATE iceberg.%s.%s SET updated_at = current_timestamp", schemaName, trinoCustomersTable))

	// Sleep to ensure timestamps are in the past for time travel queries
	time.Sleep(1 * time.Second)

	snapshotOutput := runTrinoSQL(t, env.trinoContainer, fmt.Sprintf(`SELECT snapshot_id FROM iceberg.%s."%s$snapshots" ORDER BY committed_at DESC LIMIT 1`, schemaName, trinoCustomersTable))
	snapshotID := mustParseCSVInt64(t, snapshotOutput)
	if snapshotID == 0 {
		t.Fatalf("expected snapshot ID from snapshots table, got 0")
	}

	filesOutput := runTrinoSQL(t, env.trinoContainer, fmt.Sprintf(`SELECT file_path FROM iceberg.%s."%s$files" LIMIT 1`, schemaName, trinoCustomersTable))
	if !hasCSVDataRow(filesOutput) {
		t.Fatalf("expected files metadata rows, got: %s", filesOutput)
	}

	historyOutput := runTrinoSQL(t, env.trinoContainer, fmt.Sprintf(`SELECT made_current_at FROM iceberg.%s."%s$history" LIMIT 1`, schemaName, trinoCustomersTable))
	if !hasCSVDataRow(historyOutput) {
		t.Fatalf("expected history metadata rows, got: %s", historyOutput)
	}

	countOutput = runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("SELECT count(*) FROM iceberg.%s.%s FOR VERSION AS OF %d", schemaName, trinoCustomersTable, snapshotID))
	versionCount := mustParseCSVInt64(t, countOutput)
	if versionCount != 8 {
		t.Fatalf("expected 8 rows for version time travel, got %d", versionCount)
	}

	// Use current_timestamp - interval '1 second' to ensure it's in the past (Iceberg requirement)
	countOutput = runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("SELECT count(*) FROM iceberg.%s.%s FOR TIMESTAMP AS OF (current_timestamp - interval '1' second)", schemaName, trinoCustomersTable))
	timestampCount := mustParseCSVInt64(t, countOutput)
	if timestampCount != 8 {
		t.Fatalf("expected 8 rows for timestamp time travel, got %d", timestampCount)
	}

	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DELETE FROM iceberg.%s.%s WHERE customer_sk = 8", schemaName, trinoCustomersTable))
	countOutput = runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("SELECT count(*) FROM iceberg.%s.%s", schemaName, trinoCustomersTable))
	rowCount = mustParseCSVInt64(t, countOutput)
	if rowCount != 7 {
		t.Fatalf("expected 7 rows after delete, got %d", rowCount)
	}

	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("ALTER TABLE iceberg.%s.%s EXECUTE rollback_to_snapshot(%d)", schemaName, trinoCustomersTable, snapshotID))
	countOutput = runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("SELECT count(*) FROM iceberg.%s.%s", schemaName, trinoCustomersTable))
	rowCount = mustParseCSVInt64(t, countOutput)
	if rowCount != 8 {
		t.Fatalf("expected 8 rows after rollback, got %d", rowCount)
	}
}

func runTrinoSQLAllowExists(t *testing.T, containerName, sql string) string {
	t.Helper()

	cmd := exec.Command("docker", "exec", containerName,
		"trino", "--catalog", "iceberg",
		"--output-format", "CSV",
		"--execute", sql,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		outputStr := string(output)
		if strings.Contains(outputStr, "already exists") {
			return sanitizeTrinoOutput(outputStr)
		}
		logs, _ := exec.Command("docker", "logs", containerName).CombinedOutput()
		t.Fatalf("Trino command failed: %v\nSQL: %s\nOutput:\n%s\nTrino logs:\n%s", err, sql, outputStr, string(logs))
	}
	return sanitizeTrinoOutput(string(output))
}

func runTrinoCTAS(t *testing.T, containerName, createSQL, deleteSQL, insertSQL string) {
	t.Helper()

	cmd := exec.Command("docker", "exec", containerName,
		"trino", "--catalog", "iceberg",
		"--output-format", "CSV",
		"--execute", createSQL,
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		outputStr := string(output)
		if strings.Contains(outputStr, "already exists") {
			if deleteSQL != "" {
				runTrinoSQL(t, containerName, deleteSQL)
			}
			runTrinoSQL(t, containerName, insertSQL)
			return
		}
		logs, _ := exec.Command("docker", "logs", containerName).CombinedOutput()
		t.Fatalf("Trino command failed: %v\nSQL: %s\nOutput:\n%s\nTrino logs:\n%s", err, createSQL, outputStr, string(logs))
	}
}

func hasCSVDataRow(output string) bool {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) == 0 {
		return false
	}
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			return true
		}
	}
	return false
}

func mustParseCSVInt64(t *testing.T, output string) int64 {
	t.Helper()
	value := mustFirstCSVValue(t, output)
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		t.Fatalf("failed to parse int from output %q: %v", output, err)
	}
	return parsed
}

func mustFirstCSVValue(t *testing.T, output string) string {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) == 0 {
		t.Fatalf("expected CSV output with data row, got: %q", output)
	}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Split(line, ",")
		return strings.Trim(parts[0], "\"")
	}
	t.Fatalf("no CSV data rows found in output: %q", output)
	return ""
}
