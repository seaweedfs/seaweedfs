package plugin

import (
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// Job types whose ExecuteJobRequest needs per-worker rate-allocation
// metadata injected. Keyed by the job-type string so plugin.go's
// generic dispatch path stays job-agnostic.
//
// To add a new job type to the share-allocation pipeline: register an
// entry here that knows how to read its admin-config field(s) and
// produce the metadata keys/values the worker reads.

// s3LifecycleClusterDeletesPerSecondKey, s3LifecycleClusterDeletesBurstKey,
// s3LifecycleMetadataDeletesPerSecond, and s3LifecycleMetadataDeletesBurst
// are the contract between admin and worker. The values must match the
// constants exported from weed/worker/tasks/s3_lifecycle/cluster_rate_limit.go
// — duplicated here as plain strings rather than imported so the admin
// plugin package doesn't pull a dependency on the worker handler
// package. A mismatch on either side would silently disable rate
// limiting; tests pin the constants in both packages against the same
// values.
const (
	s3LifecycleJobType                    = "s3_lifecycle"
	s3LifecycleClusterDeletesPerSecondKey = "cluster_deletes_per_second"
	s3LifecycleClusterDeletesBurstKey     = "cluster_deletes_burst"
	s3LifecycleMetadataDeletesPerSecond   = "s3_lifecycle.deletes_per_second"
	s3LifecycleMetadataDeletesBurst       = "s3_lifecycle.deletes_burst"
)

// decorateClusterContextForJob returns a new ClusterContext with any
// per-job-type metadata the admin needs to inject before the
// ExecuteJobRequest is sent. Returns the input cc unchanged when no
// decoration applies.
//
// Today only s3_lifecycle decorates; the function exists so a future
// job type's plumbing slots in alongside without touching
// executeJobWithExecutor.
func (r *Plugin) decorateClusterContextForJob(cc *plugin_pb.ClusterContext, jobType string, adminConfigValues map[string]*plugin_pb.ConfigValue) *plugin_pb.ClusterContext {
	if cc == nil {
		return cc
	}
	if jobType != s3LifecycleJobType {
		return cc
	}
	rps := readNonNegativeInt(adminConfigValues, s3LifecycleClusterDeletesPerSecondKey)
	burst := readNonNegativeInt(adminConfigValues, s3LifecycleClusterDeletesBurstKey)
	if rps <= 0 {
		// Operator hasn't configured a cluster cap; nothing to allocate.
		// The worker treats missing metadata keys as "unlimited," which
		// is the legacy behavior.
		return cc
	}
	executors := r.registry.CountCapableExecutors(jobType)
	if executors <= 0 {
		// No executors means the job won't dispatch at all; metadata
		// would be discarded. Log so the case is visible in ops.
		glog.V(2).Infof("decorateClusterContext: %s rps=%d but no execute-capable workers; skipping allocation", jobType, rps)
		return cc
	}
	perWorkerRps := float64(rps) / float64(executors)
	perWorkerBurst := 0
	if burst > 0 {
		perWorkerBurst = burst / executors
		if perWorkerBurst < 1 {
			perWorkerBurst = 1
		}
	}

	// Clone so we don't mutate the shared base context. The metadata
	// map is small; a fresh allocation per ExecuteJob is fine.
	out := cloneClusterContext(cc)
	if out.Metadata == nil {
		out.Metadata = map[string]string{}
	}
	out.Metadata[s3LifecycleMetadataDeletesPerSecond] = strconv.FormatFloat(perWorkerRps, 'f', -1, 64)
	if perWorkerBurst > 0 {
		out.Metadata[s3LifecycleMetadataDeletesBurst] = strconv.Itoa(perWorkerBurst)
	}
	glog.V(3).Infof("decorateClusterContext: %s rps=%d burst=%d executors=%d -> per-worker rps=%g burst=%d",
		jobType, rps, burst, executors, perWorkerRps, perWorkerBurst)
	return out
}

// cloneClusterContext returns a shallow-but-safe copy: the top-level
// fields are reassigned, and the Metadata map is duplicated so the
// caller can mutate it without racing other consumers of the input.
// Slices of strings (master/filer/volume/s3 addresses) are copied by
// reference — those are treated as immutable elsewhere in the codebase.
func cloneClusterContext(in *plugin_pb.ClusterContext) *plugin_pb.ClusterContext {
	if in == nil {
		return nil
	}
	out := &plugin_pb.ClusterContext{
		MasterGrpcAddresses: in.MasterGrpcAddresses,
		FilerGrpcAddresses:  in.FilerGrpcAddresses,
		VolumeGrpcAddresses: in.VolumeGrpcAddresses,
		S3GrpcAddresses:     in.S3GrpcAddresses,
	}
	if in.Metadata != nil {
		out.Metadata = make(map[string]string, len(in.Metadata))
		for k, v := range in.Metadata {
			out.Metadata[k] = v
		}
	}
	return out
}

// readNonNegativeInt reads an int64 admin config value, treating
// missing fields and non-int kinds as 0. Negative values are clamped
// to 0 since the AdminConfigForm declares MinValue=0 on both fields.
func readNonNegativeInt(values map[string]*plugin_pb.ConfigValue, field string) int {
	v, ok := values[field]
	if !ok || v == nil {
		return 0
	}
	switch k := v.Kind.(type) {
	case *plugin_pb.ConfigValue_Int64Value:
		if k.Int64Value < 0 {
			return 0
		}
		return int(k.Int64Value)
	case *plugin_pb.ConfigValue_DoubleValue:
		if k.DoubleValue < 0 {
			return 0
		}
		return int(k.DoubleValue)
	}
	return 0
}
