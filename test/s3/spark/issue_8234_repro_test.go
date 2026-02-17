package spark

import (
	"strings"
	"testing"
)

func setupSparkIssue8234Env(t *testing.T) *TestEnvironment {
	t.Helper()

	env := NewTestEnvironment()
	if !env.dockerAvailable {
		t.Skip("Docker not available, skipping Spark S3 integration test")
	}

	env.StartSeaweedFS(t)
	t.Cleanup(func() { env.Cleanup(t) })

	createObjectBucket(t, env, "test")
	env.startSparkContainer(t)
	return env
}

func TestSparkS3AppendIssue8234Regression(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Spark integration test in short mode")
	}

	env := setupSparkIssue8234Env(t)

	script := `
import sys
import pyspark.sql.types as T

target = "s3a://test/test_table/"
sc = spark.sparkContext
df = sc.parallelize([1, 2, 3]).toDF(T.IntegerType())

df.write.format("parquet").mode("append").save(target)
print("FIRST_APPEND_OK")

try:
    df.write.format("parquet").mode("append").save(target)
    print("SECOND_APPEND_OK")
    spark.stop()
    sys.exit(0)
except Exception as err:
    message = str(err)
    print("SECOND_APPEND_FAILED")
    print(message)
    if "Copy Source must mention the source bucket and key" in message:
        print("ISSUE_8234_REGRESSION")
        spark.stop()
        sys.exit(4)
    spark.stop()
    sys.exit(2)
`

	code, output := runSparkPyScript(t, env.sparkContainer, script, env.s3Port)
	if code != 0 {
		if strings.Contains(output, "ISSUE_8234_REGRESSION") {
			t.Fatalf("issue #8234 regression detected; output:\n%s", output)
		}
		t.Fatalf("Spark script exited with code %d; output:\n%s", code, output)
	}

	if !strings.Contains(output, "FIRST_APPEND_OK") {
		t.Fatalf("expected first append success marker in output, got:\n%s", output)
	}
	if !strings.Contains(output, "SECOND_APPEND_OK") {
		t.Fatalf("expected second append success marker in output, got:\n%s", output)
	}
	if strings.Contains(output, "Copy Source must mention the source bucket and key") {
		t.Fatalf("unexpected issue #8234 error in output:\n%s", output)
	}
}
