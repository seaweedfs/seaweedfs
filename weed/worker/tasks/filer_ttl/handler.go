package filer_ttl

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"google.golang.org/grpc"
)

const (
	jobType                 = "filer_ttl"
	filerAddressParam       = "filer_address"
	locationPrefixesParam   = "location_prefixes"
	defaultDetectionMinutes = 24 * 60
)

func init() {
	pluginworker.RegisterHandler(pluginworker.HandlerFactory{
		JobType:  jobType,
		Category: pluginworker.CategoryDefault,
		Aliases:  []string{"filer-ttl", "filer.ttl"},
		Build: func(opts pluginworker.HandlerBuildOptions) (pluginworker.JobHandler, error) {
			return NewHandler(opts.GrpcDialOption), nil
		},
	})
}

// Handler removes the filer rows of entries whose filer.conf path TTL has
// passed. A store without native TTL keeps such rows until something lists
// their directory, long after the TTL volumes holding the data are gone.
type Handler struct {
	grpcDialOption grpc.DialOption
}

func NewHandler(grpcDialOption grpc.DialOption) *Handler {
	return &Handler{grpcDialOption: grpcDialOption}
}

func (h *Handler) Capability() *plugin_pb.JobTypeCapability {
	return &plugin_pb.JobTypeCapability{
		JobType:                 jobType,
		CanDetect:               true,
		CanExecute:              true,
		MaxDetectionConcurrency: 1,
		MaxExecutionConcurrency: 1,
		DisplayName:             "Filer TTL",
		Description:             "Walk filer.conf TTL paths so the filer removes the rows of expired entries.",
		Weight:                  20,
	}
}

func (h *Handler) Descriptor() *plugin_pb.JobTypeDescriptor {
	return &plugin_pb.JobTypeDescriptor{
		JobType:           jobType,
		DisplayName:       "Filer TTL",
		Description:       "Remove the filer rows of entries past their filer.conf path TTL, once per filer store.",
		Icon:              "bi bi-hourglass-split",
		DescriptorVersion: 1,
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			Enabled:                  true,
			DetectionIntervalMinutes: defaultDetectionMinutes,
			DetectionTimeoutSeconds:  60,
			ExecutionTimeoutSeconds:  math.MaxInt32,
			JobTypeMaxRuntimeSeconds: math.MaxInt32,
		},
		WorkerDefaultValues: map[string]*plugin_pb.ConfigValue{},
	}
}

func (h *Handler) Detect(ctx context.Context, request *plugin_pb.RunDetectionRequest, sender pluginworker.DetectionSender) error {
	if request == nil || sender == nil {
		return fmt.Errorf("detect: nil request or sender")
	}
	if request.JobType != "" && request.JobType != jobType {
		return fmt.Errorf("job type %q is not handled by filer ttl handler", request.JobType)
	}
	var filers []pb.ServerAddress
	for _, address := range request.GetClusterContext().GetFilerAddresses() {
		if address != "" {
			filers = append(filers, pb.ServerAddress(address))
		}
	}
	if len(filers) == 0 {
		_ = sender.SendActivity(pluginworker.BuildDetectorActivity("skipped", "no filer addresses in cluster context", nil))
		return sender.SendComplete(&plugin_pb.DetectionComplete{JobType: jobType, Success: true})
	}
	fc, err := filer.ReadFilerConfFromFilers(filers, h.grpcDialOption, nil, security.FilerAdminToken)
	if err != nil {
		return err
	}
	prefixes := ttlLocationPrefixes(fc)
	if len(prefixes) == 0 {
		_ = sender.SendActivity(pluginworker.BuildDetectorActivity("skipped", "no TTL path rules in filer.conf", nil))
		return sender.SendComplete(&plugin_pb.DetectionComplete{JobType: jobType, Success: true})
	}

	var proposals []*plugin_pb.JobProposal
	for _, address := range h.oneFilerPerStore(ctx, filers, sender) {
		proposals = append(proposals, &plugin_pb.JobProposal{
			JobType:    jobType,
			ProposalId: fmt.Sprintf("filer-ttl-%s-%d", address, time.Now().UnixNano()),
			DedupeKey:  fmt.Sprintf("%s:%s", jobType, address),
			Priority:   plugin_pb.JobPriority_JOB_PRIORITY_LOW,
			Summary:    fmt.Sprintf("Walk %d TTL path(s) on the store of filer %s", len(prefixes), address),
			Parameters: map[string]*plugin_pb.ConfigValue{
				filerAddressParam:     {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: string(address)}},
				locationPrefixesParam: {Kind: &plugin_pb.ConfigValue_StringList{StringList: &plugin_pb.StringList{Values: prefixes}}},
			},
		})
	}
	if len(proposals) > 0 {
		if err := sender.SendProposals(&plugin_pb.DetectionProposals{JobType: jobType, Proposals: proposals}); err != nil {
			return err
		}
	}
	return sender.SendComplete(&plugin_pb.DetectionComplete{
		JobType:        jobType,
		Success:        true,
		TotalProposals: int32(len(proposals)),
	})
}

// oneFilerPerStore keeps the first reachable filer of each store signature:
// filers sharing a store share its rows, and a filer with its own store has
// rows no other filer's walk removes.
func (h *Handler) oneFilerPerStore(ctx context.Context, filers []pb.ServerAddress, sender pluginworker.DetectionSender) []pb.ServerAddress {
	seen := make(map[int32]bool)
	var picked []pb.ServerAddress
	for _, address := range filers {
		var signature int32
		err := pb.WithGrpcFilerClient(false, 0, address, h.grpcDialOption, security.FilerAdminToken, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.GetFilerConfiguration(ctx, &filer_pb.GetFilerConfigurationRequest{})
			if err != nil {
				return err
			}
			signature = resp.Signature
			return nil
		})
		if err != nil {
			_ = sender.SendActivity(pluginworker.BuildDetectorActivity("filer_unreachable", fmt.Sprintf("filer %s: %v", address, err), nil))
			continue
		}
		if seen[signature] {
			continue
		}
		seen[signature] = true
		picked = append(picked, address)
	}
	return picked
}

func (h *Handler) Execute(ctx context.Context, request *plugin_pb.ExecuteJobRequest, sender pluginworker.ExecutionSender) error {
	if request == nil || request.Job == nil || sender == nil {
		return fmt.Errorf("execute: nil request/job/sender")
	}
	if request.Job.JobType != "" && request.Job.JobType != jobType {
		return fmt.Errorf("job type %q is not handled by filer ttl handler", request.Job.JobType)
	}
	address := pluginworker.ReadStringConfig(request.Job.Parameters, filerAddressParam, "")
	if address == "" {
		return fmt.Errorf("execute: missing %s in job parameters", filerAddressParam)
	}
	prefixes := pluginworker.ReadStringListConfig(request.Job.Parameters, locationPrefixesParam)

	_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId: request.Job.JobId, JobType: jobType,
		State: plugin_pb.JobState_JOB_STATE_RUNNING, Stage: "walking",
		Message: fmt.Sprintf("filer %s: %s", address, strings.Join(prefixes, ", ")),
	})

	stats := &walkStats{}
	var failed []string
	err := pb.WithGrpcFilerClient(true, 0, pb.ServerAddress(address), h.grpcDialOption, security.FilerAdminToken, func(client filer_pb.SeaweedFilerClient) error {
		for _, prefix := range prefixes {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := walkLocationPrefix(ctx, client, prefix, stats); err != nil {
				failed = append(failed, fmt.Sprintf("%s: %v", prefix, err))
			}
		}
		return nil
	})
	if err == nil && len(failed) > 0 {
		err = fmt.Errorf("walk %s", strings.Join(failed, "; "))
	}

	summary := fmt.Sprintf("filer %s: walked %d TTL path(s), %d directories, %d live entries", address, len(prefixes), stats.directories, stats.entries)
	completed := &plugin_pb.JobCompleted{
		JobId:   request.Job.JobId,
		JobType: jobType,
		Success: err == nil,
		Result: &plugin_pb.JobResult{
			Summary: summary,
			OutputValues: map[string]*plugin_pb.ConfigValue{
				"directories": {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: stats.directories}},
				"entries":     {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: stats.entries}},
			},
		},
	}
	if err != nil {
		completed.ErrorMessage = err.Error()
	}
	return sender.SendCompleted(completed)
}
