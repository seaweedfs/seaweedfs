package plugin

import (
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// String constants must match weed/worker/tasks/s3_lifecycle/cluster_rate_limit.go.
// Duplicated rather than imported so the admin package doesn't depend
// on the worker handler package; tests pin both sides to the same values.
const (
	s3LifecycleJobType                    = "s3_lifecycle"
	s3LifecycleClusterDeletesPerSecondKey = "cluster_deletes_per_second"
	s3LifecycleClusterDeletesBurstKey     = "cluster_deletes_burst"
	s3LifecycleMetadataDeletesPerSecond   = "s3_lifecycle.deletes_per_second"
	s3LifecycleMetadataDeletesBurst       = "s3_lifecycle.deletes_burst"
)

// decorateClusterContextForJob injects per-job rate-allocation metadata
// when the job type opts in. Divisor is min(executors, maxJobsPerDetection)
// so singleton jobs (maxJobs=1) get the full cluster budget on the
// single active worker.
func (r *Plugin) decorateClusterContextForJob(cc *plugin_pb.ClusterContext, jobType string, adminConfigValues map[string]*plugin_pb.ConfigValue, maxJobsPerDetection int) *plugin_pb.ClusterContext {
	if cc == nil {
		return cc
	}
	if jobType != s3LifecycleJobType {
		return cc
	}
	rps := readNonNegativeInt(adminConfigValues, s3LifecycleClusterDeletesPerSecondKey)
	burst := readNonNegativeInt(adminConfigValues, s3LifecycleClusterDeletesBurstKey)
	if rps <= 0 {
		return cc
	}
	executors := r.registry.CountCapableExecutors(jobType)
	if executors <= 0 {
		glog.V(2).Infof("decorateClusterContext: %s rps=%d but no execute-capable workers; skipping allocation", jobType, rps)
		return cc
	}
	activeWorkers := executors
	if maxJobsPerDetection > 0 && maxJobsPerDetection < activeWorkers {
		activeWorkers = maxJobsPerDetection
	}
	perWorkerRps := float64(rps) / float64(activeWorkers)
	perWorkerBurst := 0
	if burst > 0 {
		perWorkerBurst = burst / activeWorkers
		if perWorkerBurst < 1 {
			// rate.Limiter with burst<1 never refills; floor.
			perWorkerBurst = 1
		}
	}

	out := cloneClusterContext(cc)
	if out.Metadata == nil {
		out.Metadata = map[string]string{}
	}
	out.Metadata[s3LifecycleMetadataDeletesPerSecond] = strconv.FormatFloat(perWorkerRps, 'f', -1, 64)
	if perWorkerBurst > 0 {
		out.Metadata[s3LifecycleMetadataDeletesBurst] = strconv.Itoa(perWorkerBurst)
	}
	glog.V(3).Infof("decorateClusterContext: %s rps=%d burst=%d executors=%d maxJobs=%d active=%d -> per-worker rps=%g burst=%d",
		jobType, rps, burst, executors, maxJobsPerDetection, activeWorkers, perWorkerRps, perWorkerBurst)
	return out
}

// cloneClusterContext duplicates the Metadata map so callers can mutate
// it without racing the shared base context. Address slices are aliased.
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
