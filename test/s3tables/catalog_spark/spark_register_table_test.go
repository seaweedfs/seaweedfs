package catalog_spark

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestSparkRegisterTableServesItsData registers a table from a copy of another
// table's current metadata, uploaded through the S3 gateway into the new
// table's own directory the way the metadata of a table migrated in arrives,
// which the filer stores in chunks rather than inline, and checks that Spark
// reads the registered rows and can write on top of them. A registered table
// used to load as an empty placeholder: SELECT found no rows and INSERT failed
// its table UUID check. The source's own file is refused under another name,
// since both tables would then own the source's files.
func TestSparkRegisterTableServesItsData(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	env, _, tableBucket := setupSparkTestEnv(t)

	namespace := "register_" + randomString(6)
	source := fmt.Sprintf("iceberg.%s.src_%s", namespace, randomString(6))
	output := runSparkPySQL(t, env.sparkContainer, fmt.Sprintf(`
spark.sql("CREATE NAMESPACE iceberg.%[1]s")
spark.sql("CREATE TABLE %[2]s (id INT, name STRING) USING iceberg")
spark.sql("INSERT INTO %[2]s VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
meta = spark.sql("SELECT file FROM %[2]s.metadata_log_entries ORDER BY timestamp DESC LIMIT 1").collect()[0]['file']
print(f"metadata file: {meta}")
`, namespace, source), env.icebergRestPort, env.s3Port)
	match := regexp.MustCompile(`metadata file: (\S+)`).FindStringSubmatch(output)
	if match == nil {
		t.Fatalf("create/insert of %s failed, output:\n%s", source, output)
	}
	metadataFile := match[1]

	t.Run("the source's own metadata file is refused", func(t *testing.T) {
		output := runSparkPySQL(t, env.sparkContainer, fmt.Sprintf(`
try:
    spark.sql("CALL iceberg.system.register_table('%[1]s.alias_%[2]s', '%[3]s')")
    print("register accepted")
except Exception as e:
    print(f"register refused: {e}")
count = spark.sql("SELECT COUNT(*) AS c FROM %[4]s").collect()[0]['c']
print(f"source row count: {count}")
`, namespace, randomString(6), metadataFile, source), env.icebergRestPort, env.s3Port)
		if !strings.Contains(output, "register refused") || !strings.Contains(output, "location is in use") {
			t.Fatalf("registering %s under another name was not refused, output:\n%s", metadataFile, output)
		}
		if !strings.Contains(output, "source row count: 3") {
			t.Fatalf("the source lost its rows, output:\n%s", output)
		}
	})

	t.Run("a copy uploaded under the new table's location", func(t *testing.T) {
		table := "reg_" + randomString(6)
		ident := fmt.Sprintf("iceberg.%s.%s", namespace, table)
		copied := uploadMetadataCopy(t, env, tableBucket, metadataFile, path.Join(namespace, table))
		output := runSparkPySQL(t, env.sparkContainer, fmt.Sprintf(`
spark.sql("CALL iceberg.system.register_table('%[1]s.%[2]s', '%[3]s')")
registered = spark.sql("SELECT COUNT(*) AS c FROM %[4]s").collect()[0]['c']
print(f"registered row count: {registered}")
spark.sql("INSERT INTO %[4]s VALUES (4, 'delta')")
inserted = spark.sql("SELECT COUNT(*) AS c FROM %[4]s").collect()[0]['c']
print(f"row count after insert: {inserted}")
`, namespace, table, copied, ident), env.icebergRestPort, env.s3Port)
		if !strings.Contains(output, "registered row count: 3") {
			t.Fatalf("the table registered from %s does not serve its rows, output:\n%s", copied, output)
		}
		if !strings.Contains(output, "row count after insert: 4") {
			t.Fatalf("INSERT into the table registered from %s failed, output:\n%s", copied, output)
		}
	})
}

// uploadMetadataCopy puts a copy of metadataFile whose location is tableDir
// under tableDir through the S3 gateway and returns the copy's location.
func uploadMetadataCopy(t *testing.T, env *TestEnvironment, bucket, metadataFile, tableDir string) string {
	t.Helper()
	key, ok := strings.CutPrefix(metadataFile, "s3://"+bucket+"/")
	if !ok {
		t.Fatalf("metadata file %s is not in bucket %s", metadataFile, bucket)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg := aws.Config{
		Region:       "us-east-1",
		Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(env.accessKey, env.secretKey, "")),
		BaseEndpoint: aws.String(fmt.Sprintf("http://localhost:%d", env.s3Port)),
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })

	obj, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("GetObject %s: %v", key, err)
	}
	content, err := io.ReadAll(obj.Body)
	obj.Body.Close()
	if err != nil {
		t.Fatalf("read %s: %v", key, err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(content, &fields); err != nil {
		t.Fatalf("parse %s: %v", key, err)
	}
	fields["location"], _ = json.Marshal("s3://" + bucket + "/" + tableDir)
	if content, err = json.Marshal(fields); err != nil {
		t.Fatalf("encode %s: %v", key, err)
	}

	copyKey := path.Join(tableDir, "metadata", "00003-uploaded.metadata.json")
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(copyKey),
		Body:   bytes.NewReader(content),
	}); err != nil {
		t.Fatalf("PutObject %s: %v", copyKey, err)
	}
	return "s3://" + bucket + "/" + copyKey
}
