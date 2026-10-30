package sqlite_cbs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests exercise SeaweedFS the way SQLite Cloud Backed Storage (CBS)
// drives a cloud bucket. CBS needs put/fetch/delete/list primitives and —
// critically — conditional PUT semantics so two clients cannot clobber each
// other's manifest update: put-if-absent (If-None-Match: *) for new objects
// and compare-and-swap (If-Match: "<etag>") for manifest rewrites. A failed
// precondition must return 412 and must not commit, even when conditional
// writes to the same key race. See sqlite.org/cloudsqlite/doc/trunk/www/module.wiki.

func cbsTestContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}

func cbsBucketName(prefix string) string {
	return fmt.Sprintf("cbs-%s-%d", prefix, time.Now().UnixNano())
}

func createCBSBucket(t *testing.T, client *s3.Client, bucket string) {
	t.Helper()
	ctx, cancel := cbsTestContext()
	defer cancel()
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
}

func putCBSTestObject(t *testing.T, client *s3.Client, bucket, key, body string) *s3.PutObjectOutput {
	t.Helper()
	ctx, cancel := cbsTestContext()
	defer cancel()
	out, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte(body)),
	})
	require.NoError(t, err)
	require.NotNil(t, out.ETag, "PutObject must return the ETag CBS feeds back into If-Match")
	return out
}

func getCBSTestObject(t *testing.T, client *s3.Client, bucket, key string) (etag string, body []byte) {
	t.Helper()
	ctx, cancel := cbsTestContext()
	defer cancel()
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	defer out.Body.Close()
	body, err = io.ReadAll(out.Body)
	require.NoError(t, err)
	require.NotNil(t, out.ETag, "GetObject must return the ETag CBS feeds back into If-Match")
	return aws.ToString(out.ETag), body
}

func isPreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "PreconditionFailed" {
		return true
	}
	var respErr *awshttp.ResponseError
	return errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusPreconditionFailed
}

func isNotModified(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NotModified" {
		return true
	}
	var respErr *awshttp.ResponseError
	return errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusNotModified
}

func isNoSuchKey(err error) bool {
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchKey"
}

// TestCBSContainerAndObjectPrimitives covers the storage primitives a CBS
// cloud module needs: create/destroy a container (bucket), put, fetch, list,
// head and delete named objects.
func TestCBSContainerAndObjectPrimitives(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping sqlite cbs integration test in short mode")
	}
	cluster := startCBSMiniCluster(t)
	client := cluster.newS3Client(t)
	bucket := cbsBucketName("primitives")

	createCBSBucket(t, client, bucket)

	ctx, cancel := cbsTestContext()
	defer cancel()

	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err, "container should be reachable right after creation")

	// xPut: store named block objects under a database prefix.
	blocks := map[string]string{
		"mydb/manifest":  "gen=1 blocks=block-000,block-001",
		"mydb/block-000": "page data for block 0",
		"mydb/block-001": "page data for block 1",
	}
	for key, body := range blocks {
		putCBSTestObject(t, client, bucket, key, body)
	}

	// xFetch: read objects back by name.
	for key, want := range blocks {
		_, got := getCBSTestObject(t, client, bucket, key)
		assert.Equal(t, want, string(got), "fetched body for %s", key)
	}

	// xList: enumerate the container's objects under the database prefix.
	listOut, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String("mydb/"),
	})
	require.NoError(t, err)
	gotKeys := make([]string, 0, len(listOut.Contents))
	for _, obj := range listOut.Contents {
		gotKeys = append(gotKeys, aws.ToString(obj.Key))
	}
	assert.ElementsMatch(t, []string{"mydb/manifest", "mydb/block-000", "mydb/block-001"}, gotKeys)

	// A conditional fetch (xFetch carries an ETag) should answer 304 when the
	// cached copy is current, so a CBS daemon revalidating a cached block does
	// not re-download it.
	manifestETag, _ := getCBSTestObject(t, client, bucket, "mydb/manifest")
	getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String("mydb/manifest"),
		IfNoneMatch: aws.String(manifestETag),
	})
	if getOut != nil && getOut.Body != nil {
		getOut.Body.Close()
	}
	require.Error(t, err, "If-None-Match with the current ETag must not return the object again")
	assert.True(t, isNotModified(err), "expected 304 NotModified, got %v", err)

	// xDelete: drop a named object.
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("mydb/block-001"),
	})
	require.NoError(t, err)
	_, err = client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("mydb/block-001"),
	})
	require.Error(t, err, "deleted block should be gone")

	// xDestroy: remove the remaining objects and the container itself.
	for _, key := range []string{"mydb/manifest", "mydb/block-000"} {
		_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		require.NoError(t, err)
	}
	_, err = client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
}

// TestCBSPutIfAbsent covers the put-if-absent half of CBS's conditional-PUT
// requirement: If-None-Match: * must create the object when absent and fail
// with 412 without committing when the name is taken.
func TestCBSPutIfAbsent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping sqlite cbs integration test in short mode")
	}
	cluster := startCBSMiniCluster(t)
	client := cluster.newS3Client(t)
	bucket := cbsBucketName("if-absent")
	createCBSBucket(t, client, bucket)

	ctx, cancel := cbsTestContext()
	defer cancel()
	key := "mydb/block-007"

	first, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		IfNoneMatch: aws.String("*"),
		Body:        bytes.NewReader([]byte("first")),
	})
	require.NoError(t, err, "put-if-absent on a new block name must succeed")
	require.NotNil(t, first.ETag)

	second, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		IfNoneMatch: aws.String("*"),
		Body:        bytes.NewReader([]byte("second")),
	})
	require.Error(t, err, "put-if-absent on a taken block name must fail")
	assert.True(t, isPreconditionFailed(err), "expected 412 PreconditionFailed, got %v", err)
	assert.Nil(t, second)

	_, body := getCBSTestObject(t, client, bucket, key)
	assert.Equal(t, "first", string(body), "the rejected conditional PUT must not have committed")
}

// TestCBSCompareAndSwap covers the compare-and-swap half: If-Match carries the
// version identifier from a prior PUT/GET and the write only commits when the
// stored object is still exactly that version.
func TestCBSCompareAndSwap(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping sqlite cbs integration test in short mode")
	}
	cluster := startCBSMiniCluster(t)
	client := cluster.newS3Client(t)
	bucket := cbsBucketName("cas")
	createCBSBucket(t, client, bucket)

	ctx, cancel := cbsTestContext()
	defer cancel()
	key := "mydb/manifest"

	v1 := putCBSTestObject(t, client, bucket, key, "generation=1")
	etag1 := aws.ToString(v1.ETag)

	// If-Match with the version identifier just returned commits.
	v2, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(key),
		IfMatch: aws.String(etag1),
		Body:    bytes.NewReader([]byte("generation=2")),
	})
	require.NoError(t, err)
	etag2 := aws.ToString(v2.ETag)
	require.NotEqual(t, etag1, etag2)

	// A retry of the same conditional write — the identifier is now stale —
	// must fail instead of clobbering generation 2.
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(key),
		IfMatch: aws.String(etag1),
		Body:    bytes.NewReader([]byte("generation=1-replayed")),
	})
	require.Error(t, err)
	assert.True(t, isPreconditionFailed(err), "stale If-Match must yield 412, got %v", err)

	// The current identifier still works.
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(key),
		IfMatch: aws.String(etag2),
		Body:    bytes.NewReader([]byte("generation=3")),
	})
	require.NoError(t, err)

	_, body := getCBSTestObject(t, client, bucket, key)
	assert.Equal(t, "generation=3", string(body))

	// If-Match on a name that does not exist can never commit — there is no
	// version it could equal. SeaweedFS deliberately answers 404 NoSuchKey
	// here rather than 412 (see validateConditionalHeaders); CBS only needs
	// the write to fail, so accept either rejection.
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String("mydb/no-such-block"),
		IfMatch: aws.String(etag2),
		Body:    bytes.NewReader([]byte("orphan")),
	})
	require.Error(t, err)
	assert.True(t, isPreconditionFailed(err) || isNoSuchKey(err), "If-Match on an absent key must be rejected, got %v", err)

	// If-Match: * is "any version exists": commits over an existing object,
	// fails on an absent one (404 NoSuchKey here, same as above).
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(key),
		IfMatch: aws.String("*"),
		Body:    bytes.NewReader([]byte("generation=4")),
	})
	require.NoError(t, err)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String("mydb/another-missing-block"),
		IfMatch: aws.String("*"),
		Body:    bytes.NewReader([]byte("orphan")),
	})
	require.Error(t, err)
	assert.True(t, isPreconditionFailed(err) || isNoSuchKey(err), "If-Match:* on an absent key must be rejected, got %v", err)

	// If-None-Match with a concrete identifier fails only when it equals the
	// current version — the form a client uses for "put unless unchanged".
	currentETag, _ := getCBSTestObject(t, client, bucket, key)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		IfNoneMatch: aws.String(currentETag),
		Body:        bytes.NewReader([]byte("unchanged rewrite")),
	})
	require.Error(t, err)
	assert.True(t, isPreconditionFailed(err), "If-None-Match with the current ETag must yield 412, got %v", err)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		IfNoneMatch: aws.String(etag1),
		Body:        bytes.NewReader([]byte("generation=5")),
	})
	require.NoError(t, err, "If-None-Match with an outdated ETag must commit")
}

// TestCBSCompareAndSwapVersionedBucket runs the same CAS sequence on a
// versioned bucket. Conditional writes must validate against the latest
// version rather than a stale entry.
func TestCBSCompareAndSwapVersionedBucket(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping sqlite cbs integration test in short mode")
	}
	cluster := startCBSMiniCluster(t)
	client := cluster.newS3Client(t)
	bucket := cbsBucketName("cas-versioned")
	createCBSBucket(t, client, bucket)

	ctx, cancel := cbsTestContext()
	defer cancel()

	_, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	key := "mydb/manifest"
	v1 := putCBSTestObject(t, client, bucket, key, "generation=1")
	v2 := putCBSTestObject(t, client, bucket, key, "generation=2")

	// The latest version's identifier commits; v1's is stale and must fail.
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(key),
		IfMatch: v1.ETag,
		Body:    bytes.NewReader([]byte("stale rewrite")),
	})
	require.Error(t, err)
	assert.True(t, isPreconditionFailed(err), "If-Match against a non-latest version must yield 412, got %v", err)

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(key),
		IfMatch: v2.ETag,
		Body:    bytes.NewReader([]byte("generation=3")),
	})
	require.NoError(t, err)

	// The CAS also works fed from a GET's ETag, matching how CBS re-reads the
	// manifest before rewriting it.
	etag3, body := getCBSTestObject(t, client, bucket, key)
	assert.Equal(t, "generation=3", string(body))
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(key),
		IfMatch: aws.String(etag3),
		Body:    bytes.NewReader([]byte("generation=4")),
	})
	require.NoError(t, err)
}

// TestCBSConcurrentPutIfAbsent races several CBS writers creating the same new
// block name: exactly one put-if-absent may commit.
func TestCBSConcurrentPutIfAbsent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping sqlite cbs integration test in short mode")
	}
	cluster := startCBSMiniCluster(t)
	bucket := cbsBucketName("race-absent")
	adminClient := cluster.newS3Client(t)
	createCBSBucket(t, adminClient, bucket)

	const writers = 8
	key := "mydb/block-contended"

	start := make(chan struct{})
	results := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		// Separate clients stand in for separate CBS client processes.
		writerClient := cluster.newS3Client(t)
		go func(i int) {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, err := writerClient.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      aws.String(bucket),
				Key:         aws.String(key),
				IfNoneMatch: aws.String("*"),
				Body:        bytes.NewReader([]byte(fmt.Sprintf("writer-%d", i))),
			})
			results <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)

	successes, preconditionFailures := 0, 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case isPreconditionFailed(err):
			preconditionFailures++
		default:
			t.Fatalf("unexpected race error: %v", err)
		}
	}
	require.Equal(t, 1, successes, "exactly one put-if-absent may commit")
	require.Equal(t, writers-1, preconditionFailures, "every losing write must see 412")
}

// cbsManifest mirrors the state a CBS daemon keeps in cloud storage: a single
// object naming the current generation of database blocks, rewritten
// transactionally at each checkpoint.
type cbsManifest struct {
	Updates []string `json:"updates"`
}

// TestCBSConcurrentManifestCAS is the core CBS correctness scenario: many
// clients read-modify-write the same manifest object. Every writer retries its
// read + If-Match PUT on 412, so all commits land and no update is silently
// lost — which is exactly what keeps the database intact when two cloud
// clients checkpoint at the same time.
func TestCBSConcurrentManifestCAS(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping sqlite cbs integration test in short mode")
	}
	cluster := startCBSMiniCluster(t)
	bucket := cbsBucketName("manifest-cas")
	adminClient := cluster.newS3Client(t)
	createCBSBucket(t, adminClient, bucket)

	const writers = 8
	key := "mydb/manifest"

	// Seed the manifest so every writer takes the read-then-CAS path.
	putCBSTestObject(t, adminClient, bucket, key, `{"updates":[]}`)

	start := make(chan struct{})
	errs := make(chan error, writers)
	var wg sync.WaitGroup

	for i := 0; i < writers; i++ {
		wg.Add(1)
		writerTag := fmt.Sprintf("writer-%d", i)
		writerClient := cluster.newS3Client(t)
		go func() {
			defer wg.Done()
			<-start
			errs <- casManifestUpdate(writerClient, bucket, key, writerTag)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	// Every committed update must be present exactly once: a lost CAS would
	// drop a checkpoint's block list and corrupt the database.
	finalETag, body := getCBSTestObject(t, adminClient, bucket, key)
	require.NotEmpty(t, finalETag)
	var manifest cbsManifest
	require.NoError(t, json.Unmarshal(body, &manifest))
	assert.Len(t, manifest.Updates, writers)
	for i := 0; i < writers; i++ {
		assert.Contains(t, manifest.Updates, fmt.Sprintf("writer-%d", i))
	}
}

// casManifestUpdate performs one CBS-style manifest rewrite: fetch the current
// manifest and its version identifier, append the writer's tag, and commit
// with If-Match. A 412 means another client committed first — re-read and
// retry, which is what a CBS client must do on a failed conditional PUT.
func casManifestUpdate(client *s3.Client, bucket, key, tag string) error {
	for attempt := 0; attempt < 64; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		getOut, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			cancel()
			return fmt.Errorf("fetch manifest: %w", err)
		}
		raw, err := io.ReadAll(getOut.Body)
		getOut.Body.Close()
		if err != nil {
			cancel()
			return fmt.Errorf("read manifest: %w", err)
		}
		var manifest cbsManifest
		if err := json.Unmarshal(raw, &manifest); err != nil {
			cancel()
			return fmt.Errorf("parse manifest: %w", err)
		}
		manifest.Updates = append(manifest.Updates, tag)
		updated, err := json.Marshal(manifest)
		if err != nil {
			cancel()
			return err
		}
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:  aws.String(bucket),
			Key:     aws.String(key),
			IfMatch: getOut.ETag,
			Body:    bytes.NewReader(updated),
		})
		cancel()
		switch {
		case err == nil:
			return nil
		case isPreconditionFailed(err):
			continue // another client committed first; re-read and retry
		default:
			return fmt.Errorf("commit manifest: %w", err)
		}
	}
	return fmt.Errorf("writer %s never won the manifest CAS", tag)
}

// TestCBSConditionalMultipartCommit covers conditional headers on
// CompleteMultipartUpload. CBS daemons stage large uploads; the completion
// must apply the same conditions atomically with the publish, or two racing
// completions could both report success.
func TestCBSConditionalMultipartCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping sqlite cbs integration test in short mode")
	}
	cluster := startCBSMiniCluster(t)
	client := cluster.newS3Client(t)
	bucket := cbsBucketName("mp-cond")
	createCBSBucket(t, client, bucket)

	ctx, cancel := cbsTestContext()
	defer cancel()
	key := "mydb/big-block"

	// If-None-Match: * completes when the object is absent.
	uploadID, partETag := startMultipartUpload(t, client, bucket, key, "multipart-generation-1")
	completeOut, err := client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
			{ETag: aws.String(partETag), PartNumber: aws.Int32(1)},
		}},
		IfNoneMatch: aws.String("*"),
	})
	require.NoError(t, err)

	// A second conditional completion on the now-existing key must fail
	// instead of silently replacing the object.
	uploadID2, partETag2 := startMultipartUpload(t, client, bucket, key, "multipart-generation-2")
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID2),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
			{ETag: aws.String(partETag2), PartNumber: aws.Int32(1)},
		}},
		IfNoneMatch: aws.String("*"),
	})
	require.Error(t, err)
	assert.True(t, isPreconditionFailed(err), "expected 412 on conditional multipart completion, got %v", err)
	abortMultipartUpload(client, bucket, key, uploadID2)

	// If-Match commits when fed the stored version identifier, and the stale
	// identifier from before must then fail.
	uploadID3, partETag3 := startMultipartUpload(t, client, bucket, key, "multipart-generation-3")
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID3),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
			{ETag: aws.String(partETag3), PartNumber: aws.Int32(1)},
		}},
		IfMatch: completeOut.ETag,
	})
	require.NoError(t, err)

	uploadID4, partETag4 := startMultipartUpload(t, client, bucket, key, "multipart-generation-4")
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID4),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
			{ETag: aws.String(partETag4), PartNumber: aws.Int32(1)},
		}},
		IfMatch: completeOut.ETag,
	})
	require.Error(t, err)
	assert.True(t, isPreconditionFailed(err), "stale If-Match on multipart completion must yield 412, got %v", err)
	abortMultipartUpload(client, bucket, key, uploadID4)

	_, body := getCBSTestObject(t, client, bucket, key)
	assert.Equal(t, "multipart-generation-3", string(body))
}

// TestCBSConcurrentConditionalMultipartCommit races two completions of the
// same key, each guarded by put-if-absent. The race the post-4.51 work fixed:
// previously both completions could report success.
func TestCBSConcurrentConditionalMultipartCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping sqlite cbs integration test in short mode")
	}
	cluster := startCBSMiniCluster(t)
	bucket := cbsBucketName("mp-race")
	adminClient := cluster.newS3Client(t)
	createCBSBucket(t, adminClient, bucket)

	const racers = 4
	key := "mydb/raced-block"

	uploadIDs := make([]string, racers)
	partETags := make([]string, racers)
	for i := 0; i < racers; i++ {
		uploadIDs[i], partETags[i] = startMultipartUpload(t, adminClient, bucket, key, fmt.Sprintf("racer-%d", i))
	}

	start := make(chan struct{})
	results := make(chan error, racers)
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		racerClient := cluster.newS3Client(t)
		uploadID, partETag := uploadIDs[i], partETags[i]
		go func() {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, err := racerClient.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
				Bucket:   aws.String(bucket),
				Key:      aws.String(key),
				UploadId: aws.String(uploadID),
				MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
					{ETag: aws.String(partETag), PartNumber: aws.Int32(1)},
				}},
				IfNoneMatch: aws.String("*"),
			})
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	successes, preconditionFailures := 0, 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case isPreconditionFailed(err):
			preconditionFailures++
		default:
			t.Fatalf("unexpected race error: %v", err)
		}
	}
	require.Equal(t, 1, successes, "exactly one conditional multipart completion may commit")
	require.Equal(t, racers-1, preconditionFailures, "every losing completion must see 412")
	for i, uploadID := range uploadIDs {
		if i == 0 {
			continue
		}
		abortMultipartUpload(adminClient, bucket, key, uploadID)
	}
}

// TestCBSDatabaseUploadDownloadRoundtrip replays a bcv-style session end to
// end: a client uploads immutable database blocks plus a manifest guarded by
// put-if-absent, a second client lists and fetches the database back, then a
// checkpoint writes new blocks and rewrites the manifest via CAS while a stale
// writer's CAS is rejected.
func TestCBSDatabaseUploadDownloadRoundtrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping sqlite cbs integration test in short mode")
	}
	cluster := startCBSMiniCluster(t)
	client := cluster.newS3Client(t)
	bucket := cbsBucketName("roundtrip")
	createCBSBucket(t, client, bucket)

	ctx, cancel := cbsTestContext()
	defer cancel()
	prefix := "shop.db/"
	manifestKey := prefix + "manifest"

	const blockCount = 6
	blockBody := func(gen, i int) []byte {
		return bytes.Repeat([]byte{byte('a' + i)}, 1024) // deterministic content per block
	}
	blockKey := func(i int) string {
		return fmt.Sprintf("%sblock-%06d", prefix, i)
	}

	// Upload generation 1: immutable blocks with put-if-absent, then the
	// manifest guarded the same way so no existing database is clobbered.
	manifest := cbsManifest{Updates: make([]string, 0, blockCount)}
	for i := 0; i < blockCount; i++ {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(blockKey(i)),
			IfNoneMatch: aws.String("*"),
			Body:        bytes.NewReader(blockBody(1, i)),
		})
		require.NoError(t, err, "upload block %d", i)
		manifest.Updates = append(manifest.Updates, blockKey(i))
	}
	manifestBody, err := json.Marshal(manifest)
	require.NoError(t, err)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(manifestKey),
		IfNoneMatch: aws.String("*"),
		Body:        bytes.NewReader(manifestBody),
	})
	require.NoError(t, err)

	// Re-uploading the same database under the same manifest name must not
	// commit over generation 1.
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(manifestKey),
		IfNoneMatch: aws.String("*"),
		Body:        bytes.NewReader([]byte("different manifest")),
	})
	require.Error(t, err)
	assert.True(t, isPreconditionFailed(err))

	// Download: list the container, fetch the manifest, fetch and verify every
	// block it names.
	listOut, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Contents, blockCount+1, "manifest plus %d blocks", blockCount)

	manifestETag, rawManifest := getCBSTestObject(t, client, bucket, manifestKey)
	var fetched cbsManifest
	require.NoError(t, json.Unmarshal(rawManifest, &fetched))
	require.Len(t, fetched.Updates, blockCount)
	for i, key := range fetched.Updates {
		_, body := getCBSTestObject(t, client, bucket, key)
		assert.Equal(t, sha256.Sum256(blockBody(1, i)), sha256.Sum256(body), "block %s checksum", key)
	}

	// Checkpoint: append a new block, rewrite the manifest by CAS on the
	// fetched ETag. A reader racing us with the old identifier loses.
	newBlock := fmt.Sprintf("%sblock-%06d", prefix, blockCount)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(newBlock),
		IfNoneMatch: aws.String("*"),
		Body:        bytes.NewReader(blockBody(2, blockCount)),
	})
	require.NoError(t, err)

	updated := fetched
	updated.Updates = append(updated.Updates, newBlock)
	updatedBody, err := json.Marshal(updated)
	require.NoError(t, err)

	// A writer holding the generation-1 identifier loses the race...
	stale, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(manifestKey),
		IfMatch: aws.String("\"stale-generation-etag\""),
		Body:    bytes.NewReader(updatedBody),
	})
	require.Error(t, err)
	assert.True(t, isPreconditionFailed(err))
	assert.Nil(t, stale)

	// ...while the writer that re-read the manifest commits.
	gen2, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(manifestKey),
		IfMatch: aws.String(manifestETag),
		Body:    bytes.NewReader(updatedBody),
	})
	require.NoError(t, err)
	require.NotNil(t, gen2.ETag)

	// The committed database is consistent: manifest lists every block and
	// every block fetches intact.
	_, rawManifest = getCBSTestObject(t, client, bucket, manifestKey)
	require.NoError(t, json.Unmarshal(rawManifest, &fetched))
	require.Len(t, fetched.Updates, blockCount+1)
	for i, key := range fetched.Updates[:blockCount] {
		_, body := getCBSTestObject(t, client, bucket, key)
		assert.Equal(t, sha256.Sum256(blockBody(1, i)), sha256.Sum256(body))
	}
	_, body := getCBSTestObject(t, client, bucket, newBlock)
	assert.Equal(t, sha256.Sum256(blockBody(2, blockCount)), sha256.Sum256(body))

	// Tear the database down the way bcv destroy does.
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(manifestKey)})
	require.NoError(t, err)
	deleteOut, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &types.Delete{Objects: []types.ObjectIdentifier{
			{Key: aws.String(blockKey(0))},
			{Key: aws.String(newBlock)},
		}},
	})
	require.NoError(t, err)
	assert.Len(t, deleteOut.Deleted, 2)
}

func startMultipartUpload(t *testing.T, client *s3.Client, bucket, key, body string) (uploadID string, partETag string) {
	t.Helper()
	ctx, cancel := cbsTestContext()
	defer cancel()

	createOut, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.UploadId)

	partOut, err := client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   createOut.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader([]byte(body)),
	})
	require.NoError(t, err)
	require.NotNil(t, partOut.ETag)

	return aws.ToString(createOut.UploadId), aws.ToString(partOut.ETag)
}

func abortMultipartUpload(client *s3.Client, bucket, key, uploadID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
}
