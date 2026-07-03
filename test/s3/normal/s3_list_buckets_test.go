package example

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v2config "github.com/aws/aws-sdk-go-v2/config"
	v2creds "github.com/aws/aws-sdk-go-v2/credentials"
	s3v2 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3v2types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListBucketsPaginationAndOwnerIndex exercises ListBuckets pagination
// (max-buckets, continuation-token, prefix) and the owner-index path that
// serves non-admin identities without scanning all buckets.
func TestListBucketsPaginationAndOwnerIndex(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	s3Config := `{
	  "identities": [
	    {"name": "admin", "credentials": [{"accessKey": "admin", "secretKey": "admin"}], "actions": ["Admin"]},
	    {"name": "alice", "credentials": [{"accessKey": "alice", "secretKey": "alice_secret"}],
	     "actions": ["Admin:alice-b1", "Admin:alice-b2", "Admin:alice-b3"]},
	    {"name": "carol", "credentials": [{"accessKey": "carol", "secretKey": "carol_secret"}],
	     "actions": ["List:alice-b1"]},
	    {"name": "bob", "credentials": [{"accessKey": "bob", "secretKey": "bob_secret"}],
	     "actions": ["Read:alice-b1"]}
	  ]
	}`
	configPath := filepath.Join(t.TempDir(), "s3.json")
	require.NoError(t, os.WriteFile(configPath, []byte(s3Config), 0644))

	cluster, err := startMiniCluster(t, "-s3.config="+configPath)
	require.NoError(t, err)
	defer cluster.Stop()

	ctx := context.Background()
	newClient := func(key, secret string) *s3v2.Client {
		cfg, err := v2config.LoadDefaultConfig(ctx,
			v2config.WithRegion("us-east-1"),
			v2config.WithCredentialsProvider(v2creds.NewStaticCredentialsProvider(key, secret, "")),
		)
		require.NoError(t, err)
		return s3v2.NewFromConfig(cfg, func(o *s3v2.Options) {
			o.BaseEndpoint = aws.String(cluster.s3Endpoint)
			o.UsePathStyle = true
		})
	}
	admin := newClient("admin", "admin")
	alice := newClient("alice", "alice_secret")
	carol := newClient("carol", "carol_secret")
	bob := newClient("bob", "bob_secret")

	// Non-admin listings use the owner index once the backfill marker exists.
	markerURL := fmt.Sprintf("http://127.0.0.1:%d/buckets/.system/owners/.complete", cluster.filerPort)
	require.Eventually(t, func() bool {
		resp, err := http.Get(markerURL)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 15*time.Second, 200*time.Millisecond, "owner index backfill marker")

	aliceBuckets := []string{"alice-b1", "alice-b2", "alice-b3"}
	for _, b := range aliceBuckets {
		_, err := alice.CreateBucket(ctx, &s3v2.CreateBucketInput{Bucket: aws.String(b)})
		require.NoError(t, err, "alice create %s", b)
	}
	var adminBuckets []string
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("admin-b%d", i)
		adminBuckets = append(adminBuckets, name)
		_, err := admin.CreateBucket(ctx, &s3v2.CreateBucketInput{Bucket: aws.String(name)})
		require.NoError(t, err, "admin create %s", name)
	}

	t.Run("OwnerSeesOwnBuckets", func(t *testing.T) {
		out, err := alice.ListBuckets(ctx, &s3v2.ListBucketsInput{})
		require.NoError(t, err)
		assert.Equal(t, aliceBuckets, bucketNames(out.Buckets))
		assert.Nil(t, out.ContinuationToken)
	})

	t.Run("OwnerPagination", func(t *testing.T) {
		page1, err := alice.ListBuckets(ctx, &s3v2.ListBucketsInput{MaxBuckets: aws.Int32(2)})
		require.NoError(t, err)
		assert.Equal(t, aliceBuckets[:2], bucketNames(page1.Buckets))
		require.NotNil(t, page1.ContinuationToken)

		page2, err := alice.ListBuckets(ctx, &s3v2.ListBucketsInput{
			MaxBuckets:        aws.Int32(2),
			ContinuationToken: page1.ContinuationToken,
		})
		require.NoError(t, err)
		assert.Equal(t, aliceBuckets[2:], bucketNames(page2.Buckets))
		assert.Nil(t, page2.ContinuationToken)
	})

	t.Run("OwnerPrefix", func(t *testing.T) {
		out, err := alice.ListBuckets(ctx, &s3v2.ListBucketsInput{Prefix: aws.String("alice-b2")})
		require.NoError(t, err)
		assert.Equal(t, []string{"alice-b2"}, bucketNames(out.Buckets))
	})

	t.Run("GrantedListVisible", func(t *testing.T) {
		out, err := carol.ListBuckets(ctx, &s3v2.ListBucketsInput{})
		require.NoError(t, err)
		assert.Equal(t, []string{"alice-b1"}, bucketNames(out.Buckets))
	})

	t.Run("ReadGrantNotVisible", func(t *testing.T) {
		out, err := bob.ListBuckets(ctx, &s3v2.ListBucketsInput{})
		require.NoError(t, err)
		assert.Empty(t, bucketNames(out.Buckets))
	})

	t.Run("AdminPaginatesAll", func(t *testing.T) {
		var all []string
		var token *string
		for pages := 0; ; pages++ {
			require.Less(t, pages, 10, "runaway pagination")
			out, err := admin.ListBuckets(ctx, &s3v2.ListBucketsInput{MaxBuckets: aws.Int32(3), ContinuationToken: token})
			require.NoError(t, err)
			require.LessOrEqual(t, len(out.Buckets), 3)
			all = append(all, bucketNames(out.Buckets)...)
			if out.ContinuationToken == nil {
				break
			}
			token = out.ContinuationToken
		}
		assert.Equal(t, append(append([]string{}, adminBuckets...), aliceBuckets...), all)
	})

	t.Run("AdminPrefix", func(t *testing.T) {
		out, err := admin.ListBuckets(ctx, &s3v2.ListBucketsInput{Prefix: aws.String("admin-")})
		require.NoError(t, err)
		assert.Equal(t, adminBuckets, bucketNames(out.Buckets))
	})

	t.Run("InvalidMaxBuckets", func(t *testing.T) {
		_, err := admin.ListBuckets(ctx, &s3v2.ListBucketsInput{MaxBuckets: aws.Int32(0)})
		require.Error(t, err)
	})

	t.Run("IndexEntriesOnFiler", func(t *testing.T) {
		indexURL := fmt.Sprintf("http://127.0.0.1:%d/buckets/.system/owners/alice/?limit=100", cluster.filerPort)
		req, err := http.NewRequest(http.MethodGet, indexURL, nil)
		require.NoError(t, err)
		req.Header.Set("Accept", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		for _, b := range aliceBuckets {
			assert.Contains(t, string(body), b)
		}
	})

	t.Run("DeleteRemovesFromListing", func(t *testing.T) {
		_, err := alice.DeleteBucket(ctx, &s3v2.DeleteBucketInput{Bucket: aws.String("alice-b2")})
		require.NoError(t, err)
		out, err := alice.ListBuckets(ctx, &s3v2.ListBucketsInput{})
		require.NoError(t, err)
		assert.Equal(t, []string{"alice-b1", "alice-b3"}, bucketNames(out.Buckets))
	})

	t.Run("HiddenSystemBucket", func(t *testing.T) {
		_, err := admin.HeadBucket(ctx, &s3v2.HeadBucketInput{Bucket: aws.String(".system")})
		require.Error(t, err)
	})
}

func bucketNames(buckets []s3v2types.Bucket) (names []string) {
	for _, b := range buckets {
		names = append(names, aws.ToString(b.Name))
	}
	return names
}
