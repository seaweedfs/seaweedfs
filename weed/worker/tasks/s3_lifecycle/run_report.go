package s3_lifecycle

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/dailyrun"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/scheduler"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	runStatusComplete = "complete"
	runStatusPartial  = "partial"
	runStatusFailed   = "failed"
	runStatusCanceled = "canceled"
)

const maxReportedInvalidPolicies = 10

// runReport is one pass's outcome as the job record shows it.
type runReport struct {
	err             error
	canceled        bool
	elapsed         time.Duration
	bucketsTotal    int
	invalidPolicies []scheduler.ParseError
	summary         dailyrun.RunSummary
	lastFullPass    time.Time
	fullPassSaveErr error
}

// Bucket walk and shard failures always surface as err. A pass without
// one is still partial when a policy could not be loaded or a shard's
// replay halted on an unresolved delete.
func (r *runReport) status() string {
	switch {
	case r.canceled:
		return runStatusCanceled
	case r.err != nil:
		return runStatusFailed
	case len(r.invalidPolicies) > 0, r.summary.ShardsHalted > 0:
		return runStatusPartial
	}
	return runStatusComplete
}

// fullPass also needs every walk-only rule evaluated: a walk deferred by
// WalkerInterval is by design, but it leaves those rules unchecked.
func (r *runReport) fullPass() bool {
	return r.status() == runStatusComplete && r.summary.WalksDeferred == 0
}

func (r *runReport) summaryLine() string {
	verb := r.status()
	if verb == runStatusComplete {
		verb = "completed"
	}
	s := r.summary
	return fmt.Sprintf("s3 lifecycle daily replay %s: buckets=%d walked=%d failed=%d skipped=%d invalid_policies=%d entries=%d events=%d rpc_done=%d rpc_errors=%d elapsed=%s",
		verb, r.bucketsTotal, s.BucketsWalked, s.BucketsFailed, s.BucketsSkipped, len(r.invalidPolicies),
		s.EntriesVisited, s.EventsScanned, s.Outcomes["DONE"], s.Outcomes["RPC_ERROR"], r.elapsed.Round(time.Second))
}

func (r *runReport) errorMessage() string {
	if r.err == nil {
		return ""
	}
	if n := r.summary.BucketsFailed; n > 1 {
		return fmt.Sprintf("%v (%d of %d buckets failed)", r.err, n, r.bucketsTotal)
	}
	return r.err.Error()
}

func (r *runReport) outputValues() map[string]*plugin_pb.ConfigValue {
	s := r.summary
	out := map[string]*plugin_pb.ConfigValue{
		"status":           stringValue(r.status()),
		"buckets_total":    int64Value(int64(r.bucketsTotal)),
		"buckets_walked":   int64Value(int64(s.BucketsWalked)),
		"buckets_failed":   int64Value(int64(s.BucketsFailed)),
		"buckets_skipped":  int64Value(int64(s.BucketsSkipped)),
		"invalid_policies": int64Value(int64(len(r.invalidPolicies))),
		"entries_visited":  int64Value(s.EntriesVisited),
		"objects_visited":  int64Value(s.ObjectsVisited),
		"events_scanned":   int64Value(s.EventsScanned),
		"shard_errors":     int64Value(int64(s.ShardErrors)),
		"shards_halted":    int64Value(int64(s.ShardsHalted)),
		"walks_deferred":   int64Value(int64(s.WalksDeferred)),
		"full_pass":        {Kind: &plugin_pb.ConfigValue_BoolValue{BoolValue: r.fullPass()}},
		"elapsed":          {Kind: &plugin_pb.ConfigValue_DurationValue{DurationValue: durationpb.New(r.elapsed.Round(time.Millisecond))}},
	}
	for outcome, n := range s.Outcomes {
		out["rpc_"+strings.ToLower(strings.TrimPrefix(outcome, "RPC_"))] = int64Value(n)
	}
	if len(s.BucketFailures) > 0 {
		failures := make([]string, 0, len(s.BucketFailures))
		for _, f := range s.BucketFailures {
			failures = append(failures, f.Bucket+": "+f.Error)
		}
		out["bucket_failures"] = stringListValue(failures)
	}
	if len(r.invalidPolicies) > 0 {
		invalid := make([]string, 0, maxReportedInvalidPolicies)
		for _, pe := range r.invalidPolicies {
			if len(invalid) == maxReportedInvalidPolicies {
				break
			}
			invalid = append(invalid, fmt.Sprintf("%s: %v", pe.Bucket, pe.Err))
		}
		sort.Strings(invalid)
		out["invalid_policy_buckets"] = stringListValue(invalid)
	}
	if s.Phase != "" {
		out["last_phase"] = stringValue(s.Phase)
	}
	if s.Bucket != "" {
		out["last_bucket"] = stringValue(s.Bucket)
	}
	if r.fullPassSaveErr != nil {
		out["full_pass_save_error"] = stringValue(r.fullPassSaveErr.Error())
	}
	if !r.lastFullPass.IsZero() {
		out["last_full_pass_at"] = stringValue(r.lastFullPass.UTC().Format(time.RFC3339))
	}
	return out
}

func stringValue(v string) *plugin_pb.ConfigValue {
	return &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_StringValue{StringValue: v}}
}

func int64Value(v int64) *plugin_pb.ConfigValue {
	return &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: v}}
}

func stringListValue(v []string) *plugin_pb.ConfigValue {
	return &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_StringList{StringList: &plugin_pb.StringList{Values: v}}}
}
