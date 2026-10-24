package s3api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

type postPolicyUploadResult struct {
	versionID string
	etag      string
}

func postPolicyUpload(ctx context.Context, client *s3.Client, bucket, key string, body []byte) (postPolicyUploadResult, error) {
	return postPolicyUploadWithFields(ctx, client, bucket, key, body, nil)
}

func postPolicyUploadWithFields(ctx context.Context, client *s3.Client, bucket, key string, body []byte, extraFields map[string]string) (postPolicyUploadResult, error) {
	presigner := s3.NewPresignClient(client, s3.WithPresignClientFromClientOptions(func(options *s3.Options) {
		options.BaseEndpoint = aws.String(defaultConfig.Endpoint)
		options.EndpointResolver = nil
		options.UsePathStyle = true
	}))
	presigned, err := presigner.PresignPostObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}, func(options *s3.PresignPostOptions) {
		options.Expires = time.Hour
		for name, value := range extraFields {
			options.Conditions = append(options.Conditions, map[string]string{name: value})
		}
	})
	if err != nil {
		return postPolicyUploadResult{}, err
	}

	var requestBody bytes.Buffer
	writer := multipart.NewWriter(&requestBody)
	for name, value := range presigned.Values {
		if err := writer.WriteField(name, value); err != nil {
			return postPolicyUploadResult{}, err
		}
	}
	for name, value := range extraFields {
		if err := writer.WriteField(name, value); err != nil {
			return postPolicyUploadResult{}, err
		}
	}
	file, err := writer.CreateFormFile("file", "payload.bin")
	if err != nil {
		return postPolicyUploadResult{}, err
	}
	if _, err := file.Write(body); err != nil {
		return postPolicyUploadResult{}, err
	}
	if err := writer.Close(); err != nil {
		return postPolicyUploadResult{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, presigned.URL, &requestBody)
	if err != nil {
		return postPolicyUploadResult{}, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return postPolicyUploadResult{}, err
	}
	defer resp.Body.Close()
	responseBody, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return postPolicyUploadResult{}, readErr
	}
	if resp.StatusCode != http.StatusNoContent {
		return postPolicyUploadResult{}, fmt.Errorf("POST Object returned %s: %s", resp.Status, responseBody)
	}
	return postPolicyUploadResult{
		versionID: resp.Header.Get("x-amz-version-id"),
		etag:      resp.Header.Get("ETag"),
	}, nil
}

func TestPostPolicyPreservesVersionHistoryAcrossPutAndDelete(t *testing.T) {
	ctx := context.Background()
	client := getS3Client(t)
	bucket := getNewBucketName()
	key := "post-policy-history.bin"
	createBucket(t, client, bucket)
	defer deleteBucket(t, client, bucket)

	putObject(t, client, bucket, key, "legacy-null")
	enableVersioning(t, client, bucket)

	postOne, err := postPolicyUpload(ctx, client, bucket, key, []byte("post-one"))
	require.NoError(t, err)
	require.NotEmpty(t, postOne.versionID)
	require.NotEmpty(t, postOne.etag)

	putTwo := putObject(t, client, bucket, key, "put-two")
	require.NotNil(t, putTwo.VersionId)
	require.NotEmpty(t, *putTwo.VersionId)

	postThree, err := postPolicyUpload(ctx, client, bucket, key, []byte("post-three"))
	require.NoError(t, err)
	require.NotEmpty(t, postThree.versionID)

	deleted, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	require.True(t, aws.ToBool(deleted.DeleteMarker))
	require.NotEmpty(t, aws.ToString(deleted.VersionId))

	postFour, err := postPolicyUpload(ctx, client, bucket, key, []byte("post-four"))
	require.NoError(t, err)
	require.NotEmpty(t, postFour.versionID)

	requireVersionBody(t, client, bucket, key, "null", []byte("legacy-null"), "legacy null version")
	requireVersionBody(t, client, bucket, key, postOne.versionID, []byte("post-one"), "first POST version")
	requireVersionBody(t, client, bucket, key, aws.ToString(putTwo.VersionId), []byte("put-two"), "interleaved PUT version")
	requireVersionBody(t, client, bucket, key, postThree.versionID, []byte("post-three"), "second POST version")
	requireVersionBody(t, client, bucket, key, postFour.versionID, []byte("post-four"), "POST after delete marker")

	listed, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key),
	})
	require.NoError(t, err)
	require.Len(t, listed.Versions, 5)
	require.Len(t, listed.DeleteMarkers, 1)
	latest := 0
	latestVersionID := ""
	for _, version := range listed.Versions {
		if aws.ToBool(version.IsLatest) {
			latest++
			latestVersionID = aws.ToString(version.VersionId)
		}
	}
	require.Equal(t, 1, latest)
	require.Equal(t, postFour.versionID, latestVersionID)
	require.False(t, aws.ToBool(listed.DeleteMarkers[0].IsLatest))
}

func TestPostPolicyRejectsIncompleteObjectLockHeaders(t *testing.T) {
	ctx := context.Background()
	client := getS3Client(t)
	bucket := getNewBucketName()
	key := "post-policy-invalid-object-lock.bin"
	createBucketWithObjectLock(t, client, bucket)
	defer deleteBucket(t, client, bucket)

	_, err := postPolicyUploadWithFields(ctx, client, bucket, key, []byte("rejected"), map[string]string{
		"x-amz-object-lock-mode": "GOVERNANCE",
	})
	require.ErrorContains(t, err, "400 Bad Request")
	require.ErrorContains(t, err, "<Code>InvalidRequest</Code>")

	listed, listErr := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key),
	})
	require.NoError(t, listErr)
	require.Empty(t, listed.Versions)
}

func TestPostPolicyVersioningStateCompatibility(t *testing.T) {
	ctx := context.Background()
	client := getS3Client(t)
	bucket := getNewBucketName()
	key := "post-policy-state.bin"
	createBucket(t, client, bucket)
	defer deleteBucket(t, client, bucket)

	unconfigured, err := postPolicyUpload(ctx, client, bucket, key, []byte("unconfigured"))
	require.NoError(t, err)
	require.Empty(t, unconfigured.versionID)
	unconfigured, err = postPolicyUpload(ctx, client, bucket, key, []byte("unconfigured-two"))
	require.NoError(t, err)
	require.Empty(t, unconfigured.versionID)
	requireVersionBody(t, client, bucket, key, "null", []byte("unconfigured-two"), "unconfigured POST replaces null version")

	enableVersioning(t, client, bucket)
	versioned := putObject(t, client, bucket, key, "numbered")
	require.NotEmpty(t, aws.ToString(versioned.VersionId))
	suspendVersioning(t, client, bucket)

	suspended, err := postPolicyUpload(ctx, client, bucket, key, []byte("suspended-one"))
	require.NoError(t, err)
	require.Empty(t, suspended.versionID)
	suspended, err = postPolicyUpload(ctx, client, bucket, key, []byte("suspended-two"))
	require.NoError(t, err)
	require.Empty(t, suspended.versionID)
	requireVersionBody(t, client, bucket, key, "null", []byte("suspended-two"), "suspended POST replaces null version")
	requireVersionBody(t, client, bucket, key, aws.ToString(versioned.VersionId), []byte("numbered"), "suspended POST preserves numbered version")

	listed, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key),
	})
	require.NoError(t, err)
	require.Len(t, listed.Versions, 2)
}

func TestPostPolicyConcurrentWritesKeepEveryVersion(t *testing.T) {
	const writeCount = 8

	ctx := context.Background()
	client := getS3Client(t)
	bucket := getNewBucketName()
	key := "post-policy-concurrent.bin"
	createBucket(t, client, bucket)
	defer deleteBucket(t, client, bucket)
	enableVersioning(t, client, bucket)

	type writeResult struct {
		versionID string
		body      []byte
		err       error
	}
	start := make(chan struct{})
	results := make(chan writeResult, writeCount)
	var wg sync.WaitGroup
	for i := 0; i < writeCount; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			body := []byte(fmt.Sprintf("concurrent-%d", i))
			if i%2 == 0 {
				post, err := postPolicyUpload(ctx, client, bucket, key, body)
				results <- writeResult{versionID: post.versionID, body: body, err: err}
				return
			}
			put, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   bytes.NewReader(body),
			})
			versionID := ""
			if put != nil {
				versionID = aws.ToString(put.VersionId)
			}
			results <- writeResult{versionID: versionID, body: body, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	seen := make(map[string]struct{}, writeCount)
	for result := range results {
		require.NoError(t, result.err)
		require.NotEmpty(t, result.versionID)
		_, duplicate := seen[result.versionID]
		require.False(t, duplicate, "each successful write must receive a unique version ID")
		seen[result.versionID] = struct{}{}
		requireVersionBody(t, client, bucket, key, result.versionID, result.body, "concurrent version body")
	}

	listed, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key),
	})
	require.NoError(t, err)
	require.Len(t, listed.Versions, writeCount)
	latest := 0
	for _, version := range listed.Versions {
		if aws.ToBool(version.IsLatest) {
			latest++
		}
	}
	require.Equal(t, 1, latest)
}
