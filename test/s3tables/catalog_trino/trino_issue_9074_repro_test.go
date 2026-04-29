package catalog_trino

import (
	"fmt"
	"strings"
	"testing"
)

// TestTrinoIssue9074CTASFreshTable is a regression test for the follow-up
// report on https://github.com/seaweedfs/seaweedfs/issues/9074.
//
// The original fix purged stale data on DROP so a re-CREATE at the same
// path could succeed. The reporter then observed that even a fresh table
// name fails — they ran a CTAS without an explicit `location =` clause and
// hit the same "Cannot create a table on a non-empty location" error.
//
// Root cause: when (a) the user does not pass `location = ...`, and (b) the
// namespace has no Iceberg `location` property, Trino's REST catalog goes
// through TrinoRestCatalog.newCreateTableTransaction's eager branch
// (`tableBuilder.create().newTransaction()`) which immediately invokes the
// REST POST. Our handleCreateTable persists `<location>/metadata/v1.metadata.json`
// as part of that call. Trino's IcebergMetadata.beginCreateTable then runs
// `fileSystem.listFiles(location).hasNext()` and trips on the metadata file
// we just wrote.
//
// Fix: handleGetNamespace / handleCreateNamespace now advertise a default
// `location` property of `s3://<bucket>/<flattened-namespace>` when the
// namespace doesn't already carry one. Trino's `defaultTableLocation` then
// returns `<namespace-location>/<table>-<UUID>` (UUID added by the standard
// `iceberg.unique-table-location=true` connector setting), and the REST
// catalog takes the deferred `createTransaction` branch that does NOT call
// REST POST until commit-time. The empty-location check sees a genuinely
// empty UUID-suffixed path and passes; on commit, our metadata write lands
// alongside the data files Trino has already produced.
//
// This test exercises the exact pattern from the report: a fresh schema, a
// fresh table name, no `location =` clause, and a CTAS on top.
func TestTrinoIssue9074CTASFreshTable(t *testing.T) {
	env := setupTrinoTest(t)
	defer env.Cleanup(t)

	schemaName := "issue9074_" + randomString(6)
	srcName := "src_" + randomString(6)
	ctasName := "ctas_" + randomString(6)
	srcQualified := fmt.Sprintf("iceberg.%s.%s", schemaName, srcName)
	ctasQualified := fmt.Sprintf("iceberg.%s.%s", schemaName, ctasName)

	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS iceberg.%s", schemaName))
	defer runTrinoSQLAllowNamespaceNotEmpty(t, env.trinoContainer, fmt.Sprintf("DROP SCHEMA IF EXISTS iceberg.%s", schemaName))
	defer runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP TABLE IF EXISTS %s", srcQualified))
	defer runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP TABLE IF EXISTS %s", ctasQualified))

	t.Logf(">>> CREATE source %s (no explicit location)", srcQualified)
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER, name VARCHAR)", srcQualified))

	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf(
		"INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", srcQualified))

	ctasSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s AS (SELECT * FROM %s)", ctasQualified, srcQualified)
	t.Logf(">>> CTAS (no location override): %s", ctasSQL)
	runTrinoSQL(t, env.trinoContainer, ctasSQL)

	countOutput := runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("SELECT count(*) FROM %s", ctasQualified))
	got := mustParseCSVInt64(t, countOutput)
	if got != 3 {
		t.Fatalf("CTAS target: expected 3 rows, got %d (resulting table should not be empty)", got)
	}

	out := runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("SELECT name FROM %s ORDER BY id", ctasQualified))
	for _, want := range []string{"a", "b", "c"} {
		if !strings.Contains(out, want) {
			t.Fatalf("CTAS target missing row %q; got:\n%s", want, out)
		}
	}
}
