package s3tables

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tag operations bound to a table bucket answer not found once its name
// holds another bucket, for the bucket and for a table in it, and leave the
// tags as they were.
func TestTagOperationsKeepToTheBoundBucket(t *testing.T) {
	fs, m := startRenameManager(t)
	client := NewManagerClient(fs.Client)
	tableARN, err := BuildTableARN(DefaultRegion, DefaultAccountID, renameTestBucket, "ns", "t")
	require.NoError(t, err)
	var bucket GetTableBucketResponse
	require.NoError(t, m.Execute(context.Background(), client, "GetTableBucket",
		&GetTableBucketRequest{TableBucketARN: mustBucketARN(t)}, &bucket, ""))
	bound := WithTableBucketCreatedAt(context.Background(), bucket.CreatedAt)
	stale := WithTableBucketCreatedAt(context.Background(), bucket.CreatedAt.Add(-time.Hour))

	for _, arn := range []string{mustBucketARN(t), tableARN} {
		require.NoError(t, m.Execute(bound, client, "TagResource",
			&TagResourceRequest{ResourceARN: arn, Tags: map[string]string{"k": "v"}}, nil, ""), arn)
		for operation, req := range map[string]interface{}{
			"TagResource":         &TagResourceRequest{ResourceARN: arn, Tags: map[string]string{"k": "other"}},
			"UntagResource":       &UntagResourceRequest{ResourceARN: arn, TagKeys: []string{"k"}},
			"ListTagsForResource": &ListTagsForResourceRequest{ResourceARN: arn},
		} {
			err := m.Execute(stale, client, operation, req, nil, "")
			var apiErr *S3TablesError
			require.ErrorAs(t, err, &apiErr, "%s on %s", operation, arn)
			assert.Contains(t, []string{ErrCodeNoSuchBucket, ErrCodeNoSuchTable}, apiErr.Type, "%s on %s", operation, arn)
		}
		var tags ListTagsForResourceResponse
		require.NoError(t, m.Execute(bound, client, "ListTagsForResource",
			&ListTagsForResourceRequest{ResourceARN: arn}, &tags, ""), arn)
		assert.Equal(t, map[string]string{"k": "v"}, tags.Tags, arn)
	}
}
