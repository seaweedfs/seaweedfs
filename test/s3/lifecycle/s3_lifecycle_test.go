// Package lifecycle is the end-to-end test for the event-driven S3
// lifecycle worker, driven by the s3.lifecycle.run-shard shell command.
//
// The S3 API rejects Expiration.Days < 1, so a literal "wait one day"
// integration test is unworkable. Instead, the test backdates the target
// object's mtime via the filer's UpdateEntry RPC: from the engine's
// perspective the object is past its expiration window the moment the
// shell command starts, and the dispatcher fires immediately.
//
// Variables (set by the Makefile):
//
//	WEED_BINARY         - path to the built `weed` binary
//	S3_ENDPOINT         - http://host:port for the S3 API
//	FILER_GRPC_ADDRESS  - host:port of the filer's gRPC listener
package lifecycle

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultS3Endpoint     = "http://localhost:8333"
	defaultS3GrpcEndpoint = "localhost:18333"
	defaultMasterEndpt    = "http://localhost:9333"
	defaultFilerGRPC      = "localhost:18888"
	bucketLifecycleXMLKey = "s3-bucket-lifecycle-configuration-xml"
	bucketsPath           = "/buckets"
	accessKey             = "some_access_key1"
	secretKey           = "some_secret_key1"
	region              = "us-east-1"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func s3Client(t *testing.T) *s3.Client {
	t.Helper()
	endpoint := envOr("S3_ENDPOINT", defaultS3Endpoint)
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: endpoint, SigningRegion: region, HostnameImmutable: true}, nil
			})),
	)
	require.NoError(t, err)
	return s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
}

func filerClient(t *testing.T) (filer_pb.SeaweedFilerClient, func()) {
	t.Helper()
	addr := envOr("FILER_GRPC_ADDRESS", defaultFilerGRPC)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	return filer_pb.NewSeaweedFilerClient(conn), func() { conn.Close() }
}

// uniqueBucket returns a fresh bucket name; the shell command picks up all
// buckets with lifecycle config, so two parallel-running tests would conflict
// on cursor state. Each test gets its own bucket.
func uniqueBucket(prefix string) string {
	return fmt.Sprintf("lc-%s-%d", prefix, time.Now().UnixNano())
}

func mustCreateBucket(t *testing.T, c *s3.Client, name string) {
	t.Helper()
	_, err := c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(name)})
	require.NoError(t, err)
	t.Cleanup(func() {
		// Best effort: empty + delete.
		listOut, _ := c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(name)})
		if listOut != nil {
			for _, o := range listOut.Contents {
				c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(name), Key: o.Key})
			}
		}
		c.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(name)})
	})
}

func putExpirationLifecycle(t *testing.T, c *s3.Client, bucket, prefix string, days int32) {
	t.Helper()
	_, err := c.PutBucketLifecycleConfiguration(context.Background(), &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(bucket),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{
				{
					ID:     aws.String("expire-prefix"),
					Status: types.ExpirationStatusEnabled,
					Filter: &types.LifecycleRuleFilter{Prefix: aws.String(prefix)},
					Expiration: &types.LifecycleExpiration{
						Days: aws.Int32(days),
					},
				},
			},
		},
	})
	require.NoError(t, err)
}

func putObject(t *testing.T, c *s3.Client, bucket, key, body string) {
	t.Helper()
	_, err := c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(body),
	})
	require.NoError(t, err)
}

// backdateMtime rewrites the object's filer entry attributes so its Mtime
// is daysOld days in the past. This sidesteps the AWS-spec minimum of one
// day for Expiration.Days, letting the lifecycle dispatcher fire on demand.
func backdateMtime(t *testing.T, fc filer_pb.SeaweedFilerClient, bucket, key string, daysOld int) {
	t.Helper()
	dir, name := splitBucketKey(bucket, key)
	resp, err := fc.LookupDirectoryEntry(context.Background(), &filer_pb.LookupDirectoryEntryRequest{
		Directory: dir,
		Name:      name,
	})
	require.NoError(t, err, "lookup %s/%s", dir, name)
	require.NotNil(t, resp.Entry)
	require.NotNil(t, resp.Entry.Attributes)

	resp.Entry.Attributes.Mtime = time.Now().Add(-time.Duration(daysOld) * 24 * time.Hour).Unix()
	resp.Entry.Attributes.MtimeNs = 0
	_, err = fc.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{
		Directory: dir,
		Entry:     resp.Entry,
	})
	require.NoError(t, err, "update %s/%s", dir, name)
}

func splitBucketKey(bucket, key string) (dir, name string) {
	full := bucketsPath + "/" + bucket + "/" + key
	if i := strings.LastIndex(full, "/"); i >= 0 {
		return full[:i], full[i+1:]
	}
	return full, ""
}

// runShellCommand invokes `weed shell` with a one-shot s3.lifecycle.run-shard
// command piped via stdin and returns the combined stdout+stderr.
func runShellCommand(t *testing.T, command string) string {
	t.Helper()
	binary := envOr("WEED_BINARY", "")
	require.NotEmpty(t, binary, "WEED_BINARY must be set")
	masterEndpoint := envOr("MASTER_ENDPOINT", defaultMasterEndpt)
	master := strings.TrimPrefix(masterEndpoint, "http://")
	if u, err := url.Parse(masterEndpoint); err == nil && u.Host != "" {
		master = u.Host
	}
	cmd := exec.Command(binary, "shell", "-master="+master)
	cmd.Stdin = strings.NewReader(command + "\nexit\n")
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	output := out.String()
	if err != nil {
		t.Logf("shell command output:\n%s", output)
		t.Fatalf("shell exec failed: %v", err)
	}
	return output
}

// TestLifecycleExpirationFiresOnBackdatedObject is the end-to-end
// validation: a 1-day expiration rule plus an object whose mtime has been
// backdated to 30 days ago must result in deletion when the shell command
// runs the matching shard.
func TestLifecycleExpirationFiresOnBackdatedObject(t *testing.T) {
	c := s3Client(t)
	fc, fcClose := filerClient(t)
	defer fcClose()

	bucket := uniqueBucket("expire")
	mustCreateBucket(t, c, bucket)
	putExpirationLifecycle(t, c, bucket, "expire/", 1)

	const oldKey = "expire/old.txt"
	const freshKey = "keep/fresh.txt"
	putObject(t, c, bucket, oldKey, "old")
	putObject(t, c, bucket, freshKey, "fresh")
	backdateMtime(t, fc, bucket, oldKey, 30)

	// One subscription handles every shard via -shards 0-15; this stays
	// independent of which (bucket, key) hash lands the target on.
	// -runtime caps the run by wall-clock so the subprocess exits even
	// when the in-shard event count never reaches -events.
	out := runShellCommand(t, fmt.Sprintf(
		"s3.lifecycle.run-shard -shards 0-15 -s3 %s -events 0 -dispatch 200ms -checkpoint 5s -runtime 10s",
		envOr("S3_GRPC_ENDPOINT", defaultS3GrpcEndpoint),
	))
	t.Logf("shell command output:\n%s", out)
	require.NotContains(t, out, "FATAL", "shell output:\n%s", out)

	// Allow the dispatched delete to land in the filer + propagate to S3.
	require.Eventuallyf(t, func() bool {
		_, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(oldKey),
		})
		return err != nil
	}, 30*time.Second, 500*time.Millisecond, "expected %s/%s to be deleted", bucket, oldKey)

	// The fresh, in-prefix-but-recent object must remain (1d rule, mtime
	// is now). Best-evidence check after waiting for the same poll window.
	_, err := c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(freshKey),
	})
	require.NoError(t, err, "%s should still exist", freshKey)
}
