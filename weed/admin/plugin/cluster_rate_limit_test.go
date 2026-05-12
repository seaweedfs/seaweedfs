package plugin

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pluginWithExecutors returns a Plugin whose registry contains n
// non-stale execute-capable workers for jobType. Helper for the
// allocator tests. Bypasses UpsertFromHello so tests don't have to
// build a full Hello message.
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

// adminConfig builds an int64 admin config map for the given fields.
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
	// Any job type other than s3_lifecycle gets the input cc back
	// unchanged. Future allocators add their own branch; the default
	// is pass-through.
	r := pluginWithExecutors(t, s3LifecycleJobType, 4)
	in := &plugin_pb.ClusterContext{Metadata: map[string]string{"unrelated": "v"}}
	out := r.decorateClusterContextForJob(in, "some_other_job", adminConfig(s3LifecycleClusterDeletesPerSecondKey, 100))
	assert.Same(t, in, out, "non-allocator job type must return the same pointer")
}

func TestDecorateClusterContext_RpsZeroSkipsAllocation(t *testing.T) {
	// rps=0 means "operator hasn't configured a cap"; the worker
	// treats missing keys as unlimited. We must NOT inject any
	// metadata (in particular, not "0") because that would force the
	// worker into a no-throughput state on a misconfigured cluster.
	r := pluginWithExecutors(t, s3LifecycleJobType, 4)
	in := &plugin_pb.ClusterContext{}
	out := r.decorateClusterContextForJob(in, s3LifecycleJobType, adminConfig(s3LifecycleClusterDeletesPerSecondKey, 0))
	if out.Metadata != nil {
		_, hasRps := out.Metadata[s3LifecycleMetadataDeletesPerSecond]
		assert.False(t, hasRps, "rps=0 must not write a deletes_per_second key")
	}
}

func TestDecorateClusterContext_NoExecutorsSkipsAllocation(t *testing.T) {
	r := pluginWithExecutors(t, s3LifecycleJobType, 0)
	in := &plugin_pb.ClusterContext{}
	out := r.decorateClusterContextForJob(in, s3LifecycleJobType, adminConfig(s3LifecycleClusterDeletesPerSecondKey, 100))
	if out.Metadata != nil {
		_, hasRps := out.Metadata[s3LifecycleMetadataDeletesPerSecond]
		assert.False(t, hasRps, "0 executors must not write share metadata (would divide by zero)")
	}
}

func TestDecorateClusterContext_SharedEvenly(t *testing.T) {
	r := pluginWithExecutors(t, s3LifecycleJobType, 4)
	in := &plugin_pb.ClusterContext{}
	out := r.decorateClusterContextForJob(in, s3LifecycleJobType, adminConfig(s3LifecycleClusterDeletesPerSecondKey, 100))
	require.NotNil(t, out.Metadata)
	assert.Equal(t, "25", out.Metadata[s3LifecycleMetadataDeletesPerSecond], "100 / 4 executors = 25/worker")
}

func TestDecorateClusterContext_BurstShared(t *testing.T) {
	r := pluginWithExecutors(t, s3LifecycleJobType, 2)
	in := &plugin_pb.ClusterContext{}
	out := r.decorateClusterContextForJob(in, s3LifecycleJobType,
		adminConfig(s3LifecycleClusterDeletesPerSecondKey, 100, s3LifecycleClusterDeletesBurstKey, 20))
	require.NotNil(t, out.Metadata)
	assert.Equal(t, "50", out.Metadata[s3LifecycleMetadataDeletesPerSecond])
	assert.Equal(t, "10", out.Metadata[s3LifecycleMetadataDeletesBurst])
}

func TestDecorateClusterContext_BurstZeroOmitsKey(t *testing.T) {
	// burst=0 means "let the worker default it." Don't write the key —
	// the worker's parsePositiveInt would then take the unset path
	// and compute 2 × rps automatically.
	r := pluginWithExecutors(t, s3LifecycleJobType, 4)
	in := &plugin_pb.ClusterContext{}
	out := r.decorateClusterContextForJob(in, s3LifecycleJobType,
		adminConfig(s3LifecycleClusterDeletesPerSecondKey, 100, s3LifecycleClusterDeletesBurstKey, 0))
	_, hasBurst := out.Metadata[s3LifecycleMetadataDeletesBurst]
	assert.False(t, hasBurst, "burst=0 must NOT write the burst key (worker default kicks in)")
}

func TestDecorateClusterContext_BurstFloorIsOneWhenDividesBelowOne(t *testing.T) {
	// burst=1 across 4 workers would round to 0; clamp to 1 so the
	// limiter doesn't become "single-token bucket that never refills."
	r := pluginWithExecutors(t, s3LifecycleJobType, 4)
	in := &plugin_pb.ClusterContext{}
	out := r.decorateClusterContextForJob(in, s3LifecycleJobType,
		adminConfig(s3LifecycleClusterDeletesPerSecondKey, 100, s3LifecycleClusterDeletesBurstKey, 1))
	assert.Equal(t, "1", out.Metadata[s3LifecycleMetadataDeletesBurst])
}

func TestDecorateClusterContext_DoesNotMutateInput(t *testing.T) {
	// The same base ClusterContext is shared across many parallel
	// ExecuteJob calls. The decorator must produce a fresh map so it
	// can't race / leak per-job metadata into the base.
	r := pluginWithExecutors(t, s3LifecycleJobType, 4)
	baseMeta := map[string]string{"existing": "value"}
	in := &plugin_pb.ClusterContext{Metadata: baseMeta}
	_ = r.decorateClusterContextForJob(in, s3LifecycleJobType,
		adminConfig(s3LifecycleClusterDeletesPerSecondKey, 100))
	_, leaked := baseMeta[s3LifecycleMetadataDeletesPerSecond]
	assert.False(t, leaked, "decorator must not mutate the input metadata map")
	assert.Equal(t, "value", baseMeta["existing"])
}

func TestDecorateClusterContext_NilInputPassesThrough(t *testing.T) {
	r := pluginWithExecutors(t, s3LifecycleJobType, 4)
	out := r.decorateClusterContextForJob(nil, s3LifecycleJobType, adminConfig(s3LifecycleClusterDeletesPerSecondKey, 100))
	assert.Nil(t, out)
}
