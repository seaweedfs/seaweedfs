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
	"sync"
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
	defaultAdminEndpoint  = "http://localhost:23646"
	// Pinned off the FILER_PORT+10000 convention; see Makefile.
	defaultFilerGRPC      = "localhost:18890"
	bucketLifecycleXMLKey = "s3-bucket-lifecycle-configuration-xml"
	bucketsPath           = "/buckets"
	accessKey             = "some_access_key1"
	secretKey             = "some_secret_key1"
	region                = "us-east-1"
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
	client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	ensureClusterWritable(t, client)
	return client
}

var clusterWritableOnce sync.Once

// ensureClusterWritable blocks until the cluster can actually serve a write,
// absorbing the volume-growth warmup window after a fresh start. The Makefile
// only waits for the server process to be up ("server up after N s"); it does
// not wait for a writable volume, so the first PutObject can race volume growth
// and fail with a transient 500 (assign volume: DeadlineExceeded) — the source
// of the lifecycle-test flakes. Probing one throwaway write here, once per
// process, warms growth so every test's first real write is past that window.
// Best-effort: if it never becomes writable, the test's own PutObject surfaces
// the failure normally.
func ensureClusterWritable(t *testing.T, c *s3.Client) {
	t.Helper()
	clusterWritableOnce.Do(func() {
		bucket := uniqueBucket("warmup")
		deadline := time.Now().Add(60 * time.Second)

		// try runs fn under a bounded context so a single hung call can't block.
		try := func(timeout time.Duration, fn func(ctx context.Context) error) error {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			return fn(ctx)
		}
		// probe is a try whose timeout is clamped to the time left before the
		// deadline, so the whole warmup stays within the budget; ok=false means
		// the budget is exhausted and the caller should stop.
		probe := func(fn func(ctx context.Context) error) (err error, ok bool) {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return nil, false
			}
			if remaining > 10*time.Second {
				remaining = 10 * time.Second
			}
			return try(remaining, fn), true
		}
		// backoff sleeps attempt*250ms, never past the deadline.
		backoff := func(attempt int) {
			d := time.Duration(attempt) * 250 * time.Millisecond
			if left := time.Until(deadline); d > left {
				d = left
			}
			if d > 0 {
				time.Sleep(d)
			}
		}

		// CreateBucket is a metadata op, but on a cold cluster the filer itself may
		// not be ready yet, so retry it within the deadline rather than abandoning
		// the whole warmup (and the PutObject probe) on the first error.
		created := false
		for attempt := 1; ; attempt++ {
			err, ok := probe(func(ctx context.Context) error {
				_, e := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
				return e
			})
			if !ok {
				break
			}
			if err == nil {
				created = true
				break
			}
			backoff(attempt)
		}
		if !created {
			t.Logf("warmup: could not create probe bucket within 60s; proceeding")
			return
		}
		// Cleanup gets a fresh timeout (not the warmup budget) so teardown runs
		// even when the probe loop used the full window.
		defer try(10*time.Second, func(ctx context.Context) error {
			_, e := c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
			return e
		})

		for attempt := 1; ; attempt++ {
			err, ok := probe(func(ctx context.Context) error {
				_, e := c.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucket), Key: aws.String("warmup"), Body: strings.NewReader("ok"),
				})
				return e
			})
			if !ok {
				break
			}
			if err == nil {
				try(10*time.Second, func(ctx context.Context) error {
					_, e := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("warmup")})
					return e
				})
				if attempt > 1 {
					t.Logf("cluster became writable after %d probe(s)", attempt)
				}
				return
			}
			backoff(attempt)
		}
		t.Logf("warmup: cluster not confirmed writable within 60s; proceeding")
	})
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
		// Drop the lifecycle configuration first so any subsequent
		// shell-driven shard sweep stops loading rules for this bucket
		// — without this, a later test's run-shard would pick up the
		// dead bucket's config and produce phantom dispatches.
		c.DeleteBucketLifecycle(context.Background(), &s3.DeleteBucketLifecycleInput{Bucket: aws.String(name)})

		// Empty every version + delete marker (versioning-aware buckets
		// hold state that ListObjectsV2 doesn't surface). Best-effort:
		// errors are tolerated because the bucket might already be
		// half-torn-down.
		listOut, _ := c.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String(name)})
		if listOut != nil {
			for _, v := range listOut.Versions {
				c.DeleteObject(context.Background(), &s3.DeleteObjectInput{
					Bucket: aws.String(name), Key: v.Key, VersionId: v.VersionId,
				})
			}
			for _, m := range listOut.DeleteMarkers {
				c.DeleteObject(context.Background(), &s3.DeleteObjectInput{
					Bucket: aws.String(name), Key: m.Key, VersionId: m.VersionId,
				})
			}
		}

		// Catch any non-versioned objects ListObjectVersions missed.
		objs, _ := c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(name)})
		if objs != nil {
			for _, o := range objs.Contents {
				c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(name), Key: o.Key})
			}
		}

		// Abort any in-flight multipart uploads — the AbortMPU lifecycle
		// test leaves these intentionally; without an explicit Abort the
		// bucket DELETE refuses with NotEmpty.
		mpus, _ := c.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{Bucket: aws.String(name)})
		if mpus != nil {
			for _, u := range mpus.Uploads {
				c.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
					Bucket: aws.String(name), Key: u.Key, UploadId: u.UploadId,
				})
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
