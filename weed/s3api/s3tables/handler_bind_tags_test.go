package s3tables

import (
	"context"
	"encoding/json"
	"net/http"
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

// A tag write merges into the tags the record holds when it is written, so a
// tag write that lands between its read and its write is kept.
func TestTagWritesKeepAConcurrentTagWrite(t *testing.T) {
	for _, tc := range []struct {
		operation string
		input     func(t *testing.T) interface{}
		want      map[string]string
	}{
		{"TagResource", func(t *testing.T) interface{} {
			return &TagResourceRequest{ResourceARN: mustBucketARN(t), Tags: map[string]string{"k": "v"}}
		}, map[string]string{"keep": "1", "other": "1", "k": "v"}},
		{"UntagResource", func(t *testing.T) interface{} {
			return &UntagResourceRequest{ResourceARN: mustBucketARN(t), TagKeys: []string{"keep"}}
		}, map[string]string{"other": "1"}},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			fs, m := startEmptyBucket(t)
			extended := extendedOf(t, fs, TablesPath, renameTestBucket)
			extended[ExtendedKeyTags] = []byte(`{"keep":"1"}`)
			fs.Put(TablesPath, renameTestBucket, extended)
			fs.BeforeUpdate = func() {
				current := extendedOf(t, fs, TablesPath, renameTestBucket)
				current[ExtendedKeyTags] = []byte(`{"keep":"1","other":"1"}`)
				fs.Put(TablesPath, renameTestBucket, current)
			}
			code, resp := runHandler(t, m, fs.Client, tc.operation, tc.input(t))
			require.Equal(t, http.StatusOK, code, resp.Message)
			var got map[string]string
			require.NoError(t, json.Unmarshal(extendedOf(t, fs, TablesPath, renameTestBucket)[ExtendedKeyTags], &got))
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestTagResourceKeepsToItsLimit(t *testing.T) {
	fs, m := startEmptyBucket(t)
	client := NewManagerClient(fs.Client)
	ctx := WithMaxTags(context.Background(), 2)
	tag := func(tags map[string]string) error {
		return m.Execute(ctx, client, "TagResource", &TagResourceRequest{ResourceARN: mustBucketARN(t), Tags: tags}, nil, "")
	}
	require.NoError(t, tag(map[string]string{"a": "1", "b": "1"}))
	require.NoError(t, tag(map[string]string{"a": "2"}))
	err := tag(map[string]string{"c": "1"})
	var apiErr *S3TablesError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, ErrCodeInvalidRequest, apiErr.Type)
	assert.JSONEq(t, `{"a":"2","b":"1"}`, string(extendedOf(t, fs, TablesPath, renameTestBucket)[ExtendedKeyTags]))
}
