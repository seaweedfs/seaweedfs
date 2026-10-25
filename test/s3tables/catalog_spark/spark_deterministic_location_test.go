package catalog_spark

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestSparkDeterministicLocationCTAS exercises Spark's iceberg-spark
// connector against the SeaweedFS REST catalog with no explicit `LOCATION`
// clause and no UUID-suffixing. iceberg-spark does not implement Trino's
// unique-table-location feature, so every CREATE TABLE here produces a
// deterministic <namespace-location>/<tableName> path. The test asserts
// the fresh-CTAS pattern works AND the resulting on-disk layout has no
// UUID-suffixed sibling — i.e. one directory per table.
//
// The fix in #9246 (advertise a default `location` namespace property)
// is what allows iceberg-spark to resolve the table location at all when
// the user did not pass one. Without it, a Spark-side CREATE TABLE on a
// namespace with no `location` property fails to compute a path.
func TestSparkDeterministicLocationCTAS(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env, _, tableBucket := setupSparkTestEnv(t)

	namespace := "detloc_" + randomString(6)
	srcTable := "src_" + randomString(6)
	ctasTable := "ctas_" + randomString(6)

	sql := fmt.Sprintf(`
spark.sql("CREATE NAMESPACE iceberg.%s")
print("namespace created")

spark.sql("""
CREATE TABLE iceberg.%s.%s (
    id INT,
    name STRING
) USING iceberg
""")
print("source table created")

spark.sql("""
INSERT INTO iceberg.%s.%s VALUES
    (1, 'alpha'),
    (2, 'beta'),
    (3, 'gamma')
""")
print("data inserted")

# CTAS without explicit LOCATION — the exact pattern that breaks on Trino
# without #9246, and that iceberg-spark needs the namespace-location
# property to resolve.
spark.sql("""
CREATE TABLE iceberg.%s.%s
USING iceberg
AS SELECT * FROM iceberg.%s.%s
""")
print("ctas created")

count = spark.sql("SELECT COUNT(*) AS c FROM iceberg.%s.%s").collect()[0]['c']
print(f"ctas row count: {count}")
`,
		namespace,
		namespace, srcTable,
		namespace, srcTable,
		namespace, ctasTable, namespace, srcTable,
		namespace, ctasTable,
	)

	output := runSparkPySQL(t, env.sparkContainer, sql, env.icebergRestPort, env.s3Port)

	for _, want := range []string{"namespace created", "source table created", "data inserted", "ctas created", "ctas row count: 3"} {
		if !strings.Contains(output, want) {
			t.Fatalf("expected output to contain %q; full output:\n%s", want, output)
		}
	}

	// Layout assertion: each table's data must land at the deterministic
	// <namespace>/<tableName>/ path, with no UUID-suffixed sibling.
	keys := listSparkBucketKeysUnder(t, env, tableBucket, namespace+"/")
	expectKeyAtSparkLocation(t, keys, namespace, srcTable)
	expectKeyAtSparkLocation(t, keys, namespace, ctasTable)
	rejectUUIDSuffixedSparkSibling(t, keys, namespace, srcTable)
	rejectUUIDSuffixedSparkSibling(t, keys, namespace, ctasTable)
}

func listSparkBucketKeysUnder(t *testing.T, env *TestEnvironment, bucket, prefix string) []string {
	t.Helper()
	cfg := aws.Config{
		Region:       "us-east-1",
		Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(env.accessKey, env.secretKey, "")),
		BaseEndpoint: aws.String(fmt.Sprintf("http://localhost:%d", env.s3Port)),
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

func expectKeyAtSparkLocation(t *testing.T, keys []string, namespace, table string) {
	t.Helper()
	want := namespace + "/" + table + "/"
	for _, k := range keys {
		if strings.HasPrefix(k, want) {
			return
		}
	}
	t.Fatalf("expected at least one object under %q; got keys: %v", want, keys)
}

func rejectUUIDSuffixedSparkSibling(t *testing.T, keys []string, namespace, table string) {
	t.Helper()
	prefix := namespace + "/" + table + "-"
	for _, k := range keys {
		if strings.HasPrefix(k, prefix) {
			t.Fatalf("found UUID-suffixed sibling key %q for table %q (Spark should produce no -<uuid> suffix)", k, table)
		}
	}
}
