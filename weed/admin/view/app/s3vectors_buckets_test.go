package app

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3vectors"
	"github.com/stretchr/testify/require"
)

func TestS3VectorsBucketsRendersBuckets(t *testing.T) {
	data := dash.S3VectorsBucketsData{TotalBuckets: 2, Buckets: []dash.S3VectorsBucketSummary{
		{Name: "media", ARN: "arn:aws:s3vectors:us-east-1:000000000000:bucket/media", CreatedAt: time.Unix(0, 0)},
		{Name: "sealed", ARN: "arn:aws:s3vectors:us-east-1:000000000000:bucket/sealed", Encryption: "AES256", CreatedAt: time.Unix(0, 0)},
	}}
	var out bytes.Buffer
	require.NoError(t, S3VectorsBuckets(data).Render(dash.WithURLPrefix(t.Context(), "/admin"), &out))
	page := out.String()
	require.Contains(t, page, `href="/admin/object-store/vector-buckets/media"`)
	require.Contains(t, page, `data-bucket-name="sealed"`)
	require.Contains(t, page, `<span class="badge bg-success">AES256</span>`)
	require.Contains(t, page, "2 vector buckets")
	require.NotContains(t, page, "No vector buckets found")
	require.NotContains(t, page, "showing the first")

	data.Truncated = true
	out.Reset()
	require.NoError(t, S3VectorsBuckets(data).Render(t.Context(), &out))
	require.Contains(t, out.String(), "showing the first 2; list the rest with an S3 Vectors client")
}

func TestS3VectorsBucketDetailsRendersIndexesAndPolicy(t *testing.T) {
	data := dash.S3VectorsBucketData{
		Bucket:       dash.S3VectorsBucketSummary{Name: "media", ARN: "arn:aws:s3vectors:us-east-1:000000000000:bucket/media"},
		Policy:       `{"Version":"2012-10-17","Statement":[]}`,
		TotalIndexes: 1,
		Indexes: []*s3vectors.Index{{
			IndexName: "movies", IndexArn: "arn:aws:s3vectors:us-east-1:000000000000:bucket/media/index/movies",
			DataType: "float32", Dimension: 384, DistanceMetric: "cosine",
			MetadataConfiguration: &s3vectors.MetadataConfiguration{NonFilterableMetadataKeys: []string{"summary", "body"}},
		}},
	}
	var out bytes.Buffer
	require.NoError(t, S3VectorsBucketDetails(data).Render(t.Context(), &out))
	page := out.String()
	for _, want := range []string{"movies", ">384<", ">cosine<", ">float32<", "summary, body", `data-index-name="movies"`, "{\n  &#34;Version&#34;: &#34;2012-10-17&#34;,\n  &#34;Statement&#34;: []\n}"} {
		require.Contains(t, page, want)
	}
	require.False(t, strings.Contains(page, "No indexes."), "an index is listed")

	data.Policy = "not json"
	out.Reset()
	require.NoError(t, S3VectorsBucketDetails(data).Render(t.Context(), &out))
	require.Contains(t, out.String(), ">not json</pre>")

	data.Policy = `{"Version":"2012-10-17","Extra":` + strings.Repeat("[", 5000) + strings.Repeat("]", 5000) + "}"
	out.Reset()
	require.NoError(t, S3VectorsBucketDetails(data).Render(t.Context(), &out))
	require.Contains(t, out.String(), `&#34;Extra&#34;:[[[[`, "a deeply nested policy is shown as stored")
}

func TestJSONDepthSkipsStrings(t *testing.T) {
	require.Equal(t, 3, jsonDepth(`{"a":["[{\"[",{}]}`))
	require.Equal(t, 0, jsonDepth(`"[[["`))
}

func TestS3TablesBucketsLinksVectorBuckets(t *testing.T) {
	data := dash.S3TablesBucketsData{Buckets: []dash.S3TablesBucketSummary{
		{Name: "vectors", ARN: "arn:aws:s3tables:us-east-1:000000000000:bucket/vectors", Format: "LANCE"},
		{Name: "warehouse", ARN: "arn:aws:s3tables:us-east-1:000000000000:bucket/warehouse", Format: "ICEBERG"},
	}}
	var out bytes.Buffer
	require.NoError(t, S3TablesBuckets(data).Render(t.Context(), &out))
	page := out.String()
	require.Contains(t, page, `href="/object-store/vector-buckets/vectors"`)
	require.NotContains(t, page, `href="/object-store/vector-buckets/warehouse"`)
}

func TestVectorOperationalStates(t *testing.T) {
	for _, data := range []dash.VectorOperationsData{
		{}, {DiscoveryError: "master unavailable"},
		{Engines: []dash.VectorNode{{Address: "engine:8090", Status: "Ready"}}, MaintenanceWorkers: 1},
	} {
		var out bytes.Buffer
		require.NoError(t, VectorStatus(data).Render(t.Context(), &out))
		if data.DiscoveryError != "" {
			require.Contains(t, out.String(), "Engine discovery failed")
		} else if len(data.Engines) == 0 {
			require.Contains(t, out.String(), "No vector engine is registered")
		} else {
			require.Contains(t, out.String(), "engine:8090")
			require.Contains(t, out.String(), "Ready")
		}
	}
}
