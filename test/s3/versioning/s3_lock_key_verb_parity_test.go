package s3api

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Backup clients arbitrate ownership by writing and retracting small keys under a
// fixed prefix, then re-probing them. Each probe uses a different verb, and the
// client trusts them to agree: a key reported present by one and absent by another
// makes it either spin or conclude the repository is corrupt. The keys are written
// and immediately deleted by version, which is the cycle that leaves versioning
// residue behind, so parity has to hold on both sides of the delete.

var lockArbitrationKeys = []string{
	"Metadata/Lock/create.checkpoint/lock",
	"Metadata/Lock/create.checkpoint/try",
	"Metadata/Lock/create.checkpoint/declare",
	"Metadata/Lock/delete.checkpoint/lock",
}

// existence reports what each verb believes about a key, so a disagreement names
// the verbs rather than just failing.
type existence struct {
	get         bool
	head        bool
	listObjects bool
	listVersion bool
}

func (e existence) agree() bool {
	return e.get == e.head && e.head == e.listObjects && e.listObjects == e.listVersion
}

func notFound(t *testing.T, err error) bool {
	t.Helper()
	if err == nil {
		return false
	}
	var respErr *smithyhttp.ResponseError
	if errors.As(err, &respErr) {
		require.Less(t, respErr.HTTPStatusCode(), 500, "probing a lock key must never fault the server: %v", err)
		return respErr.HTTPStatusCode() == 404
	}
	t.Fatalf("unexpected error probing lock key: %v", err)
	return false
}

func probe(t *testing.T, client *s3.Client, bucket, key string) existence {
	t.Helper()
	var e existence

	getResp, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err == nil {
		// Drain before closing so the connection goes back to the pool; these
		// probes run in tight loops and a discarded body forces a new connection.
		_, _ = io.Copy(io.Discard, getResp.Body)
		getResp.Body.Close()
		e.get = true
	} else {
		require.True(t, notFound(t, err), "GET must answer present or 404, got %v", err)
	}

	if _, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	}); err == nil {
		e.head = true
	} else {
		require.True(t, notFound(t, err), "HEAD must answer present or 404, got %v", err)
	}

	listed, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket), Prefix: aws.String(key),
	})
	require.NoError(t, err)
	for _, obj := range listed.Contents {
		if obj.Key != nil && *obj.Key == key {
			e.listObjects = true
		}
	}

	versions, err := client.ListObjectVersions(context.TODO(), &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket), Prefix: aws.String(key),
	})
	require.NoError(t, err)
	for _, v := range versions.Versions {
		// A version is only "the object exists" if it is the current one; a
		// superseded version coexists with a delete marker on a live key.
		if v.Key != nil && *v.Key == key && v.IsLatest != nil && *v.IsLatest {
			e.listVersion = true
		}
	}

	return e
}

func TestLockKeyVerbParityWhilePresent(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucketWithObjectLock(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	for _, key := range lockArbitrationKeys {
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte("held")),
		})
		require.NoError(t, err)
	}

	for _, key := range lockArbitrationKeys {
		t.Run(strings.ReplaceAll(key, "/", "_"), func(t *testing.T) {
			e := probe(t, client, bucketName, key)
			assert.True(t, e.agree(), "verbs disagree on a present lock key: %+v", e)
			assert.True(t, e.get, "lock key should be present: %+v", e)
		})
	}
}

// The retract half of the cycle: write, then delete by version id — the sequence
// that empties a version container. Every verb must then agree the key is gone.
func TestLockKeyVerbParityAfterVersionDelete(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucketWithObjectLock(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	for _, key := range lockArbitrationKeys {
		put, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte("held")),
		})
		require.NoError(t, err)
		require.NotNil(t, put.VersionId)

		_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket:    aws.String(bucketName),
			Key:       aws.String(key),
			VersionId: put.VersionId,
		})
		require.NoError(t, err)
	}

	for _, key := range lockArbitrationKeys {
		t.Run(strings.ReplaceAll(key, "/", "_"), func(t *testing.T) {
			e := probe(t, client, bucketName, key)
			assert.True(t, e.agree(), "verbs disagree on a retracted lock key: %+v", e)
			assert.False(t, e.get, "retracted lock key should be absent: %+v", e)
		})
	}
}

// Re-acquiring after a retract is the steady-state loop. Residue left by the
// previous cycle must not make the new key invisible to any verb.
func TestLockKeyVerbParityAcrossReacquireCycles(t *testing.T) {
	client := getS3Client(t)
	bucketName := getNewBucketName()

	createBucketWithObjectLock(t, client, bucketName)
	defer deleteBucket(t, client, bucketName)

	for _, key := range lockArbitrationKeys {
		t.Run(strings.ReplaceAll(key, "/", "_"), func(t *testing.T) {
			for cycle := 0; cycle < 3; cycle++ {
				put, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String(key),
					Body:   bytes.NewReader([]byte("held")),
				})
				require.NoError(t, err)
				require.NotNil(t, put.VersionId)

				held := probe(t, client, bucketName, key)
				require.True(t, held.agree(), "cycle %d: verbs disagree while held: %+v", cycle, held)
				require.True(t, held.get, "cycle %d: key should be present while held: %+v", cycle, held)

				_, err = client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
					Bucket:    aws.String(bucketName),
					Key:       aws.String(key),
					VersionId: put.VersionId,
				})
				require.NoError(t, err)

				released := probe(t, client, bucketName, key)
				require.True(t, released.agree(), "cycle %d: verbs disagree after release: %+v", cycle, released)
				require.False(t, released.get, "cycle %d: key should be absent after release: %+v", cycle, released)
			}
		})
	}
}
