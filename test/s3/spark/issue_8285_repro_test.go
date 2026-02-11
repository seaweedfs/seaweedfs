package spark

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestSparkS3TemporaryDirectoryCleanupIssue8285Regression(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Spark integration test in short mode")
	}

	env := setupSparkIssue8234Env(t)

	script := `
import pyspark.sql.functions as F

target = "s3a://test/issue-8285/output"

spark.conf.set("spark.hadoop.fs.s3a.committer.name", "directory")
spark.conf.set("spark.hadoop.fs.s3a.committer.magic.enabled", "false")
spark.conf.set("spark.hadoop.fs.s3a.committer.staging.abort.pending.uploads", "true")
spark.conf.set("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "append")
spark.conf.set("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/tmp")
spark.conf.set("spark.hadoop.fs.s3a.directory.marker.retention", "keep")

df = spark.range(0, 200).repartition(12).withColumn("value", F.col("id") * 2)
df.write.format("parquet").mode("overwrite").save(target)
count = spark.read.parquet(target).count()
print("WRITE_COUNT=" + str(count))
`

	code, output := runSparkPyScript(t, env.sparkContainer, script, env.s3Port)
	if code != 0 {
		t.Fatalf("Spark script exited with code %d; output:\n%s", code, output)
	}
	if !strings.Contains(output, "WRITE_COUNT=200") {
		t.Fatalf("expected write/read success marker in output, got:\n%s", output)
	}

	keys := listObjectKeysByPrefix(t, env, "test", "issue-8285/")
	var temporaryKeys []string
	for _, key := range keys {
		if hasTemporaryPathSegment(key) {
			temporaryKeys = append(temporaryKeys, key)
		}
	}

	if len(temporaryKeys) > 0 {
		t.Fatalf("issue #8285 regression detected: found lingering _temporary artifacts: %v\nall keys: %v", temporaryKeys, keys)
	}

	temporaryCandidates := []string{
		"issue-8285/output/_temporary/",
		"issue-8285/output/_temporary/0/",
		"issue-8285/output/_temporary/0/_temporary/",
	}
	lingering := waitForObjectsToDisappear(t, env, "test", temporaryCandidates, 35*time.Second)
	if len(lingering) > 0 {
		t.Fatalf("issue #8285 regression detected: lingering temporary directories: %v", lingering)
	}
}

func listObjectKeysByPrefix(t *testing.T, env *TestEnvironment, bucketName, prefix string) []string {
	t.Helper()
	client := newS3Client(env)

	pager := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	})

	var keys []string
	for pager.HasMorePages() {
		page, err := pager.NextPage(context.Background())
		if err != nil {
			t.Fatalf("failed listing objects for prefix %q: %v", prefix, err)
		}
		for _, object := range page.Contents {
			keys = append(keys, aws.ToString(object.Key))
		}
	}

	return keys
}

func headObjectInfo(t *testing.T, env *TestEnvironment, bucketName, key string) (bool, string, error) {
	t.Helper()

	client := newS3Client(env)
	output, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err == nil {
		return true, aws.ToString(output.ContentType), nil
	}

	var notFound *s3types.NotFound
	if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "NoSuchKey") || errors.As(err, &notFound) {
		return false, "", nil
	}
	return false, "", err
}

func waitForObjectsToDisappear(t *testing.T, env *TestEnvironment, bucketName string, keys []string, timeout time.Duration) []string {
	t.Helper()

	deadline := time.Now().Add(timeout)
	pending := make(map[string]struct{}, len(keys))
	details := make(map[string]string, len(keys))
	for _, key := range keys {
		pending[key] = struct{}{}
	}

	for len(pending) > 0 && time.Now().Before(deadline) {
		for key := range pending {
			exists, contentType, err := headObjectInfo(t, env, bucketName, key)
			if err != nil {
				details[key] = fmt.Sprintf("%s (head_error=%v)", key, err)
				continue
			}
			if !exists {
				delete(pending, key)
				delete(details, key)
				continue
			}
			details[key] = fmt.Sprintf("%s (exists=true, contentType=%q)", key, contentType)
		}
		if len(pending) > 0 {
			time.Sleep(2 * time.Second)
		}
	}

	if len(pending) == 0 {
		return nil
	}

	var lingering []string
	for _, key := range keys {
		if _, ok := pending[key]; !ok {
			continue
		}
		if detail, hasDetail := details[key]; hasDetail {
			lingering = append(lingering, detail)
		} else {
			lingering = append(lingering, key)
		}
	}
	return lingering
}

func newS3Client(env *TestEnvironment) *s3.Client {
	cfg := aws.Config{
		Region:       "us-east-1",
		Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(env.accessKey, env.secretKey, "")),
		BaseEndpoint: aws.String(fmt.Sprintf("http://localhost:%d", env.s3Port)),
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
}

func hasTemporaryPathSegment(key string) bool {
	for _, segment := range strings.Split(strings.TrimSuffix(key, "/"), "/") {
		if segment == "_temporary" {
			return true
		}
	}
	return false
}
