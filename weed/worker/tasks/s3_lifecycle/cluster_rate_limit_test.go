package s3_lifecycle

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildLimiterFromClusterContext_NilCC(t *testing.T) {
	l, desc := buildLimiterFromClusterContext(nil)
	assert.Nil(t, l)
	assert.Equal(t, "unlimited", desc)
}

func TestBuildLimiterFromClusterContext_NoMetadata(t *testing.T) {
	l, desc := buildLimiterFromClusterContext(&plugin_pb.ClusterContext{})
	assert.Nil(t, l)
	assert.Equal(t, "unlimited", desc)
}

func TestBuildLimiterFromClusterContext_MissingRateKey(t *testing.T) {
	l, desc := buildLimiterFromClusterContext(&plugin_pb.ClusterContext{
		Metadata: map[string]string{"unrelated": "x"},
	})
	assert.Nil(t, l)
	assert.Equal(t, "unlimited", desc)
}

func TestBuildLimiterFromClusterContext_NonPositiveRate(t *testing.T) {
	// 0 or negative rate means the admin didn't allocate; the worker
	// must NOT construct a limiter that throttles every request to
	// zero-throughput.
	for _, raw := range []string{"0", "-1", "0.0", "not-a-number", ""} {
		l, desc := buildLimiterFromClusterContext(&plugin_pb.ClusterContext{
			Metadata: map[string]string{MetadataKeyDeletesPerSecond: raw},
		})
		assert.Nil(t, l, "rate=%q must yield nil limiter", raw)
		assert.Equal(t, "unlimited", desc)
	}
}

func TestBuildLimiterFromClusterContext_PositiveRateBuildsLimiter(t *testing.T) {
	l, desc := buildLimiterFromClusterContext(&plugin_pb.ClusterContext{
		Metadata: map[string]string{
			MetadataKeyDeletesPerSecond: "12.5",
			MetadataKeyDeletesBurst:     "25",
		},
	})
	require.NotNil(t, l)
	assert.Equal(t, 12.5, float64(l.Limit()))
	assert.Equal(t, 25, l.Burst())
	assert.Contains(t, desc, "12.5/s")
	assert.Contains(t, desc, "burst=25")
}

func TestBuildLimiterFromClusterContext_BurstMissingDefaultsTo2xRate(t *testing.T) {
	// burst omitted (admin allocator wrote nothing) → worker computes
	// 2 × rps so a single tick has headroom for two refills.
	l, _ := buildLimiterFromClusterContext(&plugin_pb.ClusterContext{
		Metadata: map[string]string{MetadataKeyDeletesPerSecond: "10"},
	})
	require.NotNil(t, l)
	assert.Equal(t, 10.0, float64(l.Limit()))
	assert.Equal(t, 20, l.Burst(), "burst default must be 2× rps")
}

func TestBuildLimiterFromClusterContext_TinyRateClampsBurstToOne(t *testing.T) {
	// A sub-1-rps allocation (e.g. 0.5/s across few workers) would
	// compute 2 × 0.5 = 1, but if the int truncation produced 0 the
	// limiter would never refill. Clamp to at least 1.
	l, _ := buildLimiterFromClusterContext(&plugin_pb.ClusterContext{
		Metadata: map[string]string{MetadataKeyDeletesPerSecond: "0.1"},
	})
	require.NotNil(t, l)
	assert.GreaterOrEqual(t, l.Burst(), 1, "burst must never round down to 0")
}
