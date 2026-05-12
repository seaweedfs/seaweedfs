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
	// rate<=0 must yield nil, not a zero-throughput limiter.
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
	l, _ := buildLimiterFromClusterContext(&plugin_pb.ClusterContext{
		Metadata: map[string]string{MetadataKeyDeletesPerSecond: "10"},
	})
	require.NotNil(t, l)
	assert.Equal(t, 10.0, float64(l.Limit()))
	assert.Equal(t, 20, l.Burst(), "burst default must be 2× rps")
}

func TestBuildLimiterFromClusterContext_TinyRateClampsBurstToOne(t *testing.T) {
	// rate.Limiter with burst<1 never refills; floor.
	l, _ := buildLimiterFromClusterContext(&plugin_pb.ClusterContext{
		Metadata: map[string]string{MetadataKeyDeletesPerSecond: "0.1"},
	})
	require.NotNil(t, l)
	assert.GreaterOrEqual(t, l.Burst(), 1, "burst must never round down to 0")
}
