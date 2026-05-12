package plugin

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pluginWithExecutors builds a Plugin with n execute-capable workers
// for jobType, bypassing UpsertFromHello.
func pluginWithExecutors(t *testing.T, jobType string, n int) *Plugin {
	t.Helper()
	reg := NewRegistry()
	now := time.Now()
	for i := 0; i < n; i++ {
		id := "worker-" + string(rune('a'+i))
		reg.sessions[id] = &WorkerSession{
			WorkerID:    id,
			LastSeenAt:  now,
			ConnectedAt: now,
			Capabilities: map[string]*plugin_pb.JobTypeCapability{
				jobType: {CanExecute: true},
			},
		}
	}
	return &Plugin{registry: reg}
}

func adminConfig(pairs ...interface{}) map[string]*plugin_pb.ConfigValue {
	if len(pairs)%2 != 0 {
		panic("adminConfig expects key/value pairs")
	}
	out := map[string]*plugin_pb.ConfigValue{}
	for i := 0; i < len(pairs); i += 2 {
		key := pairs[i].(string)
		switch v := pairs[i+1].(type) {
		case int:
			out[key] = &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(v)}}
		case int64:
			out[key] = &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: v}}
		case float64:
			out[key] = &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: v}}
		default:
			panic("adminConfig: unsupported value type")
		}
	}
	return out
}

func TestDecorateClusterContext_NonS3LifecycleIsPassThrough(t *testing.T) {
	r := pluginWithExecutors(t, s3LifecycleJobType, 4)
	in := &plugin_pb.ClusterContext{Metadata: map[string]string{"unrelated": "v"}}
	out := r.decorateClusterContextForJob(in, "some_other_job", adminConfig(s3LifecycleClusterDeletesPerSecondKey, 100), 1)
	assert.Same(t, in, out, "non-allocator job type must return the same pointer")
}

func TestDecorateClusterContext_RpsZeroSkipsAllocation(t *testing.T) {
	// rps=0 must NOT write "0" — worker would read it as zero-throughput.
	r := pluginWithExecutors(t, s3LifecycleJobType, 4)
	in := &plugin_pb.ClusterContext{}
	out := r.decorateClusterContextForJob(in, s3LifecycleJobType, adminConfig(s3LifecycleClusterDeletesPerSecondKey, 0), 1)
	if out.Metadata != nil {
		_, hasRps := out.Metadata[s3LifecycleMetadataDeletesPerSecond]
		assert.False(t, hasRps, "rps=0 must not write a deletes_per_second key")
	}
}

func TestDecorateClusterContext_NoExecutorsSkipsAllocation(t *testing.T) {
	r := pluginWithExecutors(t, s3LifecycleJobType, 0)
	in := &plugin_pb.ClusterContext{}
	out := r.decorateClusterContextForJob(in, s3LifecycleJobType, adminConfig(s3LifecycleClusterDeletesPerSecondKey, 100), 1)
	if out.Metadata != nil {
		_, hasRps := out.Metadata[s3LifecycleMetadataDeletesPerSecond]
		assert.False(t, hasRps, "0 executors must not write share metadata (would divide by zero)")
	}
}

func TestDecorateClusterContext_SingletonJobGetsFullBudget(t *testing.T) {
	// maxJobs=1: budget must go undivided to the single active worker.
	r := pluginWithExecutors(t, s3LifecycleJobType, 4)
	in := &plugin_pb.ClusterContext{}
	out := r.decorateClusterContextForJob(in, s3LifecycleJobType,
		adminConfig(s3LifecycleClusterDeletesPerSecondKey, 100), 1)
	require.NotNil(t, out.Metadata)
	assert.Equal(t, "100", out.Metadata[s3LifecycleMetadataDeletesPerSecond])
}

func TestDecorateClusterContext_SharedEvenlyWhenParallelLimited(t *testing.T) {
	r := pluginWithExecutors(t, s3LifecycleJobType, 4)
	in := &plugin_pb.ClusterContext{}
	out := r.decorateClusterContextForJob(in, s3LifecycleJobType,
		adminConfig(s3LifecycleClusterDeletesPerSecondKey, 100), 4)
	require.NotNil(t, out.Metadata)
	assert.Equal(t, "25", out.Metadata[s3LifecycleMetadataDeletesPerSecond])
}

func TestDecorateClusterContext_MaxJobsExceedsExecutors(t *testing.T) {
	// divisor = min(executors=4, maxJobs=10).
	r := pluginWithExecutors(t, s3LifecycleJobType, 4)
	in := &plugin_pb.ClusterContext{}
	out := r.decorateClusterContextForJob(in, s3LifecycleJobType,
		adminConfig(s3LifecycleClusterDeletesPerSecondKey, 100), 10)
	require.NotNil(t, out.Metadata)
	assert.Equal(t, "25", out.Metadata[s3LifecycleMetadataDeletesPerSecond])
}

func TestDecorateClusterContext_BurstSharedWhenParallel(t *testing.T) {
	r := pluginWithExecutors(t, s3LifecycleJobType, 2)
	in := &plugin_pb.ClusterContext{}
	out := r.decorateClusterContextForJob(in, s3LifecycleJobType,
		adminConfig(s3LifecycleClusterDeletesPerSecondKey, 100, s3LifecycleClusterDeletesBurstKey, 20), 2)
	require.NotNil(t, out.Metadata)
	assert.Equal(t, "50", out.Metadata[s3LifecycleMetadataDeletesPerSecond])
	assert.Equal(t, "10", out.Metadata[s3LifecycleMetadataDeletesBurst])
}

func TestDecorateClusterContext_BurstZeroOmitsKey(t *testing.T) {
	// burst=0 means "let the worker default to 2*rps" — omit the key.
	r := pluginWithExecutors(t, s3LifecycleJobType, 4)
	in := &plugin_pb.ClusterContext{}
	out := r.decorateClusterContextForJob(in, s3LifecycleJobType,
		adminConfig(s3LifecycleClusterDeletesPerSecondKey, 100, s3LifecycleClusterDeletesBurstKey, 0), 4)
	_, hasBurst := out.Metadata[s3LifecycleMetadataDeletesBurst]
	assert.False(t, hasBurst, "burst=0 must NOT write the burst key (worker default kicks in)")
}

func TestDecorateClusterContext_BurstFloorIsOneWhenDividesBelowOne(t *testing.T) {
	// rate.Limiter with burst<1 never refills.
	r := pluginWithExecutors(t, s3LifecycleJobType, 4)
	in := &plugin_pb.ClusterContext{}
	out := r.decorateClusterContextForJob(in, s3LifecycleJobType,
		adminConfig(s3LifecycleClusterDeletesPerSecondKey, 100, s3LifecycleClusterDeletesBurstKey, 1), 4)
	assert.Equal(t, "1", out.Metadata[s3LifecycleMetadataDeletesBurst])
}

func TestDecorateClusterContext_DoesNotMutateInput(t *testing.T) {
	// Base ClusterContext is shared across parallel ExecuteJob calls.
	r := pluginWithExecutors(t, s3LifecycleJobType, 4)
	baseMeta := map[string]string{"existing": "value"}
	in := &plugin_pb.ClusterContext{Metadata: baseMeta}
	_ = r.decorateClusterContextForJob(in, s3LifecycleJobType,
		adminConfig(s3LifecycleClusterDeletesPerSecondKey, 100), 1)
	_, leaked := baseMeta[s3LifecycleMetadataDeletesPerSecond]
	assert.False(t, leaked, "decorator must not mutate the input metadata map")
	assert.Equal(t, "value", baseMeta["existing"])
}

func TestDecorateClusterContext_NilInputPassesThrough(t *testing.T) {
	r := pluginWithExecutors(t, s3LifecycleJobType, 4)
	out := r.decorateClusterContextForJob(nil, s3LifecycleJobType, adminConfig(s3LifecycleClusterDeletesPerSecondKey, 100), 1)
	assert.Nil(t, out)
}
