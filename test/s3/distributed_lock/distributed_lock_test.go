package distributed_lock

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConditionalPutIfNoneMatchDistributedLockAcrossS3Gateways(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping distributed lock integration test in short mode")
	}

	cluster := startDistributedLockCluster(t)
	clientA := cluster.newS3Client(t, cluster.s3Endpoint(0))
	clientB := cluster.newS3Client(t, cluster.s3Endpoint(1))

	bucket := fmt.Sprintf("distributed-lock-%d", time.Now().UnixNano())
	_, err := clientA.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, err := clientB.HeadBucket(context.Background(), &s3.HeadBucketInput{
			Bucket: aws.String(bucket),
		})
		return err == nil
	}, 30*time.Second, 200*time.Millisecond, "bucket should replicate to the second filer-backed gateway")

	keysByOwner := cluster.findLockOwnerKeys(bucket, "conditional-put")
	require.Len(t, keysByOwner, len(cluster.filerPorts), "should exercise both filer lock owners")

	for owner, key := range keysByOwner {
		owner := owner
		key := key
		t.Run(lockOwnerLabel(owner), func(t *testing.T) {
			runConditionalPutRace(t, []s3RaceClient{
				{name: "s3-a", client: clientA},
				{name: "s3-b", client: clientB},
			}, bucket, key)
		})
	}
}

type s3RaceClient struct {
	name   string
	client *s3.Client
}

type putAttemptResult struct {
	clientName string
	body       string
	err        error
}

func runConditionalPutRace(t *testing.T, clients []s3RaceClient, bucket, key string) {
	t.Helper()

	start := make(chan struct{})
	results := make(chan putAttemptResult, len(clients))
	var wg sync.WaitGroup

	for _, client := range clients {
		wg.Add(1)
		body := fmt.Sprintf("%s-race", client.name)
		go func(client s3RaceClient, body string) {
			defer wg.Done()
			<-start

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			_, err := client.client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      aws.String(bucket),
				Key:         aws.String(key),
				IfNoneMatch: aws.String("*"),
				Body:        bytes.NewReader([]byte(body)),
			})
			results <- putAttemptResult{
				clientName: client.name,
				body:       body,
				err:        err,
			}
		}(client, body)
	}

	close(start)
	wg.Wait()
	close(results)

	successes := 0
	preconditionFailures := 0
	winnerBody := ""
	unexpectedErrors := make([]string, 0)

	for result := range results {
		if result.err == nil {
			successes++
			winnerBody = result.body
			continue
		}
		if isPreconditionFailed(result.err) {
			preconditionFailures++
			continue
		}
		unexpectedErrors = append(unexpectedErrors, fmt.Sprintf("%s: %v", result.clientName, result.err))
	}

	require.Empty(t, unexpectedErrors, "unexpected race errors")
	require.Equal(t, 1, successes, "exactly one write should win")
	require.Equal(t, len(clients)-1, preconditionFailures, "all losing writes should fail with 412")

	object, err := clients[0].client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	defer object.Body.Close()

	data, err := io.ReadAll(object.Body)
	require.NoError(t, err)
	assert.Equal(t, winnerBody, string(data), "stored object body should match the successful request")
}

func isPreconditionFailed(err error) bool {
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && apiErr.ErrorCode() == "PreconditionFailed"
}

func (c *distributedLockCluster) findLockOwnerKeys(bucket, prefix string) map[pb.ServerAddress]string {
	owners := make([]pb.ServerAddress, 0, len(c.filerPorts))
	for i := range c.filerPorts {
		owners = append(owners, c.filerServerAddress(i))
	}
	sort.Slice(owners, func(i, j int) bool {
		return owners[i] < owners[j]
	})

	keysByOwner := make(map[pb.ServerAddress]string, len(owners))
	for i := 0; i < 1024 && len(keysByOwner) < len(owners); i++ {
		key := fmt.Sprintf("%s-%03d.txt", prefix, i)
		lockOwner := ownerForObjectLock(bucket, key, owners)
		if _, exists := keysByOwner[lockOwner]; !exists {
			keysByOwner[lockOwner] = key
		}
	}
	return keysByOwner
}

func ownerForObjectLock(bucket, object string, owners []pb.ServerAddress) pb.ServerAddress {
	lockKey := fmt.Sprintf("s3.object.write:/buckets/%s/%s", bucket, s3_constants.NormalizeObjectKey(object))
	hash := util.HashStringToLong(lockKey)
	if hash < 0 {
		hash = -hash
	}
	return owners[hash%int64(len(owners))]
}

func lockOwnerLabel(owner pb.ServerAddress) string {
	replacer := strings.NewReplacer(":", "_", ".", "_")
	return "owner_" + replacer.Replace(string(owner))
}
