package catalog_trino

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestTrinoDeterministicLocationCTAS exercises the Trino REST catalog with
// `iceberg.unique-table-location=false`. With #9246 advertising a default
// namespace `location` property, Trino's defaultTableLocation is non-null,
// so even with the unique-table-location flag flipped off the CREATE flow
// takes the deferred-transaction branch and the listFiles emptiness check
// passes.
//
// The benefit users opt into by flipping the flag is a single dir per
// table on disk (catalog entry and Iceberg <location> share a path, no
// sibling `<table>-<uuid>/` directory). This test exercises a fresh CTAS
// in that mode and asserts the on-disk layout has no UUID-suffixed sibling.
func TestTrinoDeterministicLocationCTAS(t *testing.T) {
	env := setupTrinoTest(t, withDeterministicTableLocation())
	defer env.Cleanup(t)

	schemaName := "detloc_" + randomString(6)
	srcName := "src_" + randomString(6)
	ctasName := "ctas_" + randomString(6)
	srcQualified := fmt.Sprintf("iceberg.%s.%s", schemaName, srcName)
	ctasQualified := fmt.Sprintf("iceberg.%s.%s", schemaName, ctasName)

	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS iceberg.%s", schemaName))
	defer runTrinoSQLAllowNamespaceNotEmpty(t, env.trinoContainer, fmt.Sprintf("DROP SCHEMA IF EXISTS iceberg.%s", schemaName))
	defer runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP TABLE IF EXISTS %s", srcQualified))
	defer runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("DROP TABLE IF EXISTS %s", ctasQualified))

	t.Logf(">>> CREATE source %s (no explicit location, unique-table-location=false)", srcQualified)
	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf(
		"CREATE TABLE %s (id INTEGER, name VARCHAR)", srcQualified))

	runTrinoSQL(t, env.trinoContainer, fmt.Sprintf(
		"INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", srcQualified))

	ctasSQL := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s", ctasQualified, srcQualified)
	t.Logf(">>> CTAS (no location override): %s", ctasSQL)
	runTrinoSQL(t, env.trinoContainer, ctasSQL)

	countOutput := runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("SELECT count(*) FROM %s", ctasQualified))
	if got := mustParseCSVInt64(t, countOutput); got != 3 {
		t.Fatalf("CTAS target: expected 3 rows, got %d", got)
	}

	out := runTrinoSQL(t, env.trinoContainer, fmt.Sprintf("SELECT name FROM %s ORDER BY id", ctasQualified))
	for _, want := range []string{"a", "b", "c"} {
		if !strings.Contains(out, want) {
			t.Fatalf("CTAS target missing row %q; got:\n%s", want, out)
		}
	}

	// Layout assertion: with unique-table-location=false, both tables must
	// land at the deterministic <bucket>/<schema>/<tableName> path without
	// any UUID-suffixed sibling directory under the namespace.
	keys := listBucketKeysUnder(t, env, "iceberg-tables", schemaName+"/")
	expectKeyAtLocation(t, keys, schemaName, srcName)
	expectKeyAtLocation(t, keys, schemaName, ctasName)
	rejectUUIDSuffixedSibling(t, keys, schemaName, srcName)
	rejectUUIDSuffixedSibling(t, keys, schemaName, ctasName)
}

// listBucketKeysUnder lists every object key under the given bucket prefix
// using the test S3 endpoint.
func listBucketKeysUnder(t *testing.T, env *TestEnvironment, bucket, prefix string) []string {
	t.Helper()
	cfg := aws.Config{
		Region:       "us-east-1",
		Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(env.accessKey, env.secretKey, "")),
		BaseEndpoint: aws.String(fmt.Sprintf("http://%s:%d", env.bindIP, env.s3Port)),
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	out, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 prefix=%s: %v", prefix, err)
	}
	keys := make([]string, 0, len(out.Contents))
	for _, obj := range out.Contents {
		keys = append(keys, aws.ToString(obj.Key))
	}
	return keys
}

// expectKeyAtLocation asserts at least one object key sits under
// <schema>/<table>/ (any depth), proving the table has a deterministic path.
func expectKeyAtLocation(t *testing.T, keys []string, schema, table string) {
	t.Helper()
	want := schema + "/" + table + "/"
	for _, k := range keys {
		if strings.HasPrefix(k, want) {
			return
		}
	}
	t.Fatalf("expected at least one object under %q; got keys: %v", want, keys)
}

// rejectUUIDSuffixedSibling asserts no object key sits under
// <schema>/<table>-<hex>/, since a UUID-suffixed sibling would mean Trino
// did add a UUID despite unique-table-location=false.
func rejectUUIDSuffixedSibling(t *testing.T, keys []string, schema, table string) {
	t.Helper()
	prefix := schema + "/" + table + "-"
	for _, k := range keys {
		if strings.HasPrefix(k, prefix) {
			t.Fatalf("found UUID-suffixed sibling key %q for table %q (unique-table-location=false should produce no -<uuid> suffix)", k, table)
		}
	}
}
