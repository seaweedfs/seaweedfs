package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/test/testutil"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/iceberg"
)

const (
	pyicebergImage = "seaweedfs-lifecycle-pyiceberg"
	duckdbImage    = "duckdb/duckdb:latest"

	// Three appends of this many rows, all inside one month so they land in
	// one partition and compaction has something to merge. The two string
	// columns hold few enough distinct values to be dictionary-encoded, which
	// is the encoding the merge destroyed.
	rowsPerBatch = 4000
	batches      = 3
	categories   = 7
	values       = 13
)

// tally is what a client reports about a table. Both halves of this suite
// speak it, and the point of the whole suite is that the one taken before
// maintenance equals the one taken after.
type tally struct {
	Rows       int    `json:"rows"`
	Categories int    `json:"categories"`
	Values     int    `json:"values"`
	Digest     string `json:"digest"`
	// Lance only: a compaction that merged nothing would otherwise let this
	// test pass without having tested anything.
	Fragments int `json:"fragments"`
	Version   int `json:"version"`
}

func TestIcebergTableLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	if shared == nil {
		t.Skip("no cluster")
	}
	if !testutil.HasDocker() {
		t.Skip("Docker not available")
	}

	// DuckDB is the client the bug was reported against, and the only one
	// here that writes the deprecated PLAIN_DICTIONARY encoding a Go writer
	// will not produce. PyIceberg writes the modern one, so between them the
	// merge is checked against both dictionary encodings in the spec.
	t.Run("DuckDB", testIcebergLifecycleWithDuckDB)
	t.Run("PyIceberg", testIcebergLifecycleWithPyIceberg)
}

func testIcebergLifecycleWithDuckDB(t *testing.T) {
	env := shared
	bucket := "lifecycle-duckdb-" + randomSuffix()
	namespace, table := "sales", "events"
	env.createTableBucket(t, bucket, "ICEBERG")

	inserts := make([]string, 0, batches)
	for i := 0; i < batches; i++ {
		inserts = append(inserts, fmt.Sprintf(
			"INSERT INTO cat.%s.%s SELECT g, TIMESTAMPTZ '2026-03-01 00:00:00' + INTERVAL (g %% 600) MINUTE, "+
				"'cat-' || (g %% %d), 'v-' || (g %% %d) FROM generate_series(%d, %d) t(g);",
			namespace, table, categories, values, i*rowsPerBatch+1, (i+1)*rowsPerBatch))
	}

	before := env.duckdb(t, bucket, "write", strings.Join([]string{
		fmt.Sprintf("CREATE SCHEMA cat.%s;", namespace),
		// Partitioned, the way the table in the report was. Compaction bins
		// per partition, so this is also what puts more than one bin in play.
		fmt.Sprintf("CREATE TABLE cat.%s.%s(id int, ts timestamptz, category text, value text) PARTITIONED BY (month(ts));", namespace, table),
		strings.Join(inserts, "\n"),
		duckdbTallySQL(namespace, table),
	}, "\n"))
	assertSeeded(t, before)

	env.maintainIcebergTable(t, bucket, namespace+"/"+table)

	after := env.duckdb(t, bucket, "verify", duckdbTallySQL(namespace, table))
	assertSameData(t, before, after)

	env.duckdb(t, bucket, "drop", fmt.Sprintf("DROP TABLE cat.%s.%s;\nDROP SCHEMA cat.%s;", namespace, table, namespace))
	if env.entryExists(t, fmt.Sprintf("/buckets/%s/%s/%s/", bucket, namespace, table)) {
		t.Fatal("the dropped table's data is still on disk")
	}
}

func testIcebergLifecycleWithPyIceberg(t *testing.T) {
	env := shared
	bucket := "lifecycle-pyiceberg-" + randomSuffix()
	namespace, table := "sales", "events"
	env.createTableBucket(t, bucket, "ICEBERG")
	buildClientImage(t, pyicebergImage, "Dockerfile.pyiceberg")

	before := env.pyiceberg(t, "write", bucket, namespace, table)
	assertSeeded(t, before)

	env.maintainIcebergTable(t, bucket, namespace+"/"+table)

	after := env.pyiceberg(t, "verify", bucket, namespace, table)
	assertSameData(t, before, after)

	env.pyiceberg(t, "drop", bucket, namespace, table)
	if env.entryExists(t, fmt.Sprintf("/buckets/%s/%s/%s/", bucket, namespace, table)) {
		t.Fatal("the dropped table's data is still on disk")
	}
}

// maintainIcebergTable runs the worker's whole maintenance cycle against the
// live filer, in the order a scheduled worker would: merge the small files,
// drop the snapshots that referenced them, sweep what nothing references any
// more, then fold the manifests together.
func (env *environment) maintainIcebergTable(t *testing.T, bucket, tablePath string) {
	t.Helper()

	client := env.filerClient(t)
	handler := iceberg.NewHandler(nil)
	config := iceberg.Config{
		TargetFileSizeBytes:  256 << 20,
		MinInputFiles:        2,
		MaxCommitRetries:     3,
		SnapshotRetentionMs:  1,
		MaxSnapshotsToKeep:   1,
		OrphanOlderThanHours: 1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	operations := []struct {
		name string
		run  func() (string, map[string]int64, error)
	}{
		{"compact", func() (string, map[string]int64, error) {
			return handler.CompactDataFiles(ctx, client, bucket, tablePath, config)
		}},
		{"expire", func() (string, map[string]int64, error) {
			return handler.ExpireSnapshots(ctx, client, bucket, tablePath, config)
		}},
		{"orphans", func() (string, map[string]int64, error) {
			return handler.RemoveOrphans(ctx, client, bucket, tablePath, config)
		}},
		{"manifests", func() (string, map[string]int64, error) {
			return handler.RewriteManifests(ctx, client, bucket, tablePath, config)
		}},
	}
	for _, operation := range operations {
		result, metrics, err := operation.run()
		if err != nil {
			t.Fatalf("%s: %v", operation.name, err)
		}
		t.Logf("%s: %s %v", operation.name, result, metrics)

		// A compaction that merged nothing leaves the read below checking a
		// file the worker never wrote, which proves nothing at all.
		if operation.name == "compact" && metrics[iceberg.MetricFilesMerged] < batches {
			t.Fatalf("compaction merged %d files, want all %d written by the client: %s",
				metrics[iceberg.MetricFilesMerged], batches, result)
		}
	}
}

// duckdb runs a script against the catalog and returns whatever tally it
// printed. The prelude is the reporter's own ATTACH, SigV4 and all.
func (env *environment) duckdb(t *testing.T, bucket, phase, body string) tally {
	t.Helper()

	script := fmt.Sprintf(`INSTALL iceberg;
LOAD iceberg;
CREATE SECRET s3_secret (TYPE S3, KEY_ID '%s', SECRET '%s', ENDPOINT 'host.docker.internal:%d', URL_STYLE 'path', USE_SSL false);
ATTACH 's3://%s' AS cat (TYPE ICEBERG, ENDPOINT '%s', AUTHORIZATION_TYPE SigV4, SECRET 's3_secret', SIGV4_SERVICE 's3', SIGV4_REGION 'us-east-1', ACCESS_DELEGATION_MODE 'none', READ_ONLY false);
%s
`, accessKey, secretKey, env.s3Port, bucket, env.containerURL(env.icebergPort), body)

	name := fmt.Sprintf("duckdb-%s-%s.sql", bucket, phase)
	if err := os.WriteFile(filepath.Join(env.dataDir, name), []byte(script), 0644); err != nil {
		t.Fatalf("write the DuckDB script: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "run", "--rm",
		"-v", fmt.Sprintf("%s:/test", env.dataDir),
		"--add-host", "host.docker.internal:host-gateway",
		"--entrypoint", "duckdb",
		duckdbImage,
		"-init", "/test/"+name,
		"-c", "SELECT 1",
	)
	out, err := cmd.CombinedOutput()
	t.Logf("DuckDB %s:\n%s", phase, out)
	if err != nil {
		// Coverage where the image supports it, rather than a hard
		// requirement on an extension that ships separately.
		if isMissingDuckDBSupport(string(out)) {
			t.Skipf("this DuckDB image cannot write Iceberg through a REST catalog: %v", err)
		}
		t.Fatalf("DuckDB %s: %v", phase, err)
	}
	if !strings.Contains(body, "TALLY") {
		return tally{}
	}
	return parseDuckDBTally(t, string(out))
}

func duckdbTallySQL(namespace, table string) string {
	// The digest covers whole rows, because counting each column on its own
	// passes a merge that keeps every column's cardinality and hands the
	// values to the wrong rows.
	return fmt.Sprintf("SELECT 'TALLY ' || count(*) || ' ' || count(DISTINCT category) || ' ' || count(DISTINCT value)"+
		" || ' ' || md5(string_agg(id || '|' || category || '|' || value, chr(10) ORDER BY id)) AS marker FROM cat.%s.%s;",
		namespace, table)
}

var duckdbTallyPattern = regexp.MustCompile(`TALLY (\d+) (\d+) (\d+) ([0-9a-f]{32})`)

func parseDuckDBTally(t *testing.T, out string) tally {
	t.Helper()

	fields := duckdbTallyPattern.FindStringSubmatch(out)
	if fields == nil {
		t.Fatalf("DuckDB printed no tally:\n%s", out)
	}
	number := func(s string) int {
		value, err := strconv.Atoi(s)
		if err != nil {
			t.Fatalf("parse %q: %v", s, err)
		}
		return value
	}
	return tally{
		Rows:       number(fields[1]),
		Categories: number(fields[2]),
		Values:     number(fields[3]),
		Digest:     fields[4],
	}
}

func isMissingDuckDBSupport(out string) bool {
	for _, marker := range []string{
		"iceberg extension is not available",
		"Failed to load",
		"Unknown extension",
		"syntax error",
		"not implemented",
	} {
		if strings.Contains(out, marker) {
			return true
		}
	}
	return false
}

func (env *environment) pyiceberg(t *testing.T, phase, bucket, namespace, table string) tally {
	t.Helper()

	out := env.runClient(t, pyicebergImage, phase, "/app/iceberg_lifecycle.py",
		"--catalog-url", env.containerURL(env.icebergPort),
		"--s3-endpoint", env.containerURL(env.s3Port),
		"--bucket", bucket,
		"--namespace", namespace,
		"--table", table,
	)
	if phase == "drop" {
		return tally{}
	}
	return decodeTally(t, out)
}

// runClient runs one phase of a python client and returns its stdout.
func (env *environment) runClient(t *testing.T, image, phase, script string, args ...string) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), clientTimeout)
	defer cancel()

	run := []string{"run", "--rm",
		"--add-host", "host.docker.internal:host-gateway",
		"-e", "AWS_ACCESS_KEY_ID=" + accessKey,
		"-e", "AWS_SECRET_ACCESS_KEY=" + secretKey,
		"-e", "AWS_REGION=us-east-1",
		"-e", "AWS_ALLOW_HTTP=true",
		image, "python3", script, "--phase", phase,
		"--access-key", accessKey, "--secret-key", secretKey,
	}
	cmd := exec.CommandContext(ctx, "docker", append(run, args...)...)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if stderr.Len() > 0 {
		t.Logf("%s %s (stderr):\n%s", image, phase, stderr.String())
	}
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", image, phase, err, out)
	}
	t.Logf("%s %s: %s", image, phase, strings.TrimSpace(string(out)))
	return string(out)
}

func decodeTally(t *testing.T, out string) tally {
	t.Helper()

	// The clients print the tally last; anything a library logged before it
	// is not JSON and not ours.
	lines := strings.Split(strings.TrimSpace(out), "\n")
	var decoded tally
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &decoded); err != nil {
		t.Fatalf("decode the tally from %q: %v", out, err)
	}
	return decoded
}

func buildClientImage(t *testing.T, image, dockerfile string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "docker", "build", "-t", image, "-f", dockerfile, ".")
	cmd.Dir = shared.testDir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", image, err, out)
	}
}

// assertSeeded checks the client wrote what this suite assumes it wrote. Every
// comparison below is against these numbers, so a client that quietly seeded
// one row would otherwise make the whole test vacuous.
func assertSeeded(t *testing.T, seeded tally) {
	t.Helper()

	if want := rowsPerBatch * batches; seeded.Rows != want {
		t.Fatalf("seeded %d rows, want %d", seeded.Rows, want)
	}
	if seeded.Categories != categories || seeded.Values != values {
		t.Fatalf("seeded %d categories and %d values, want %d and %d",
			seeded.Categories, seeded.Values, categories, values)
	}
}

// assertSameData is the test. Maintenance rewrites files; it must not change a
// single row, and the cardinalities are called out separately because that is
// the shape the failure took: a dictionary column collapsed onto one entry,
// read back without an error, and answered wrongly.
func assertSameData(t *testing.T, before, after tally) {
	t.Helper()

	if after.Rows != before.Rows {
		t.Errorf("maintenance changed the row count: %d -> %d", before.Rows, after.Rows)
	}
	if after.Categories != before.Categories {
		t.Errorf("maintenance collapsed the category column: %d distinct values -> %d",
			before.Categories, after.Categories)
	}
	if after.Values != before.Values {
		t.Errorf("maintenance collapsed the value column: %d distinct values -> %d",
			before.Values, after.Values)
	}
	if after.Digest != before.Digest {
		t.Errorf("maintenance changed the rows: digest %s -> %s", before.Digest, after.Digest)
	}
}
