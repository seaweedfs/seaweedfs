package pluginworker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAdminScriptDescriptorDefaults(t *testing.T) {
	descriptor := NewAdminScriptHandler(nil).Descriptor()
	if descriptor == nil {
		t.Fatalf("expected descriptor")
	}
	if descriptor.AdminRuntimeDefaults == nil {
		t.Fatalf("expected admin runtime defaults")
	}
	if descriptor.AdminRuntimeDefaults.DetectionIntervalSeconds != adminScriptDetectTickSecs {
		t.Fatalf("unexpected detection interval seconds: got=%d want=%d",
			descriptor.AdminRuntimeDefaults.DetectionIntervalSeconds, adminScriptDetectTickSecs)
	}
	if descriptor.AdminConfigForm == nil {
		t.Fatalf("expected admin config form")
	}
	runInterval := ReadInt64Config(descriptor.AdminConfigForm.DefaultValues, "run_interval_minutes", 0)
	if runInterval != defaultAdminScriptRunMins {
		t.Fatalf("unexpected run_interval_minutes default: got=%d want=%d", runInterval, defaultAdminScriptRunMins)
	}
	script := ReadStringConfig(descriptor.AdminConfigForm.DefaultValues, "script", "")
	if strings.TrimSpace(script) == "" {
		t.Fatalf("expected non-empty default script")
	}
}

func TestAdminScriptDetectSkipsByRunInterval(t *testing.T) {
	handler := NewAdminScriptHandler(nil)
	sender := &recordingDetectionSender{}
	err := handler.Detect(context.Background(), &plugin_pb.RunDetectionRequest{
		JobType:           adminScriptJobType,
		LastSuccessfulRun: timestamppb.New(time.Now().Add(-2 * time.Minute)),
		AdminConfigValues: map[string]*plugin_pb.ConfigValue{
			"script": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: defaultAdminScript},
			},
			"run_interval_minutes": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 17},
			},
		},
	}, sender)
	if err != nil {
		t.Fatalf("detect returned err = %v", err)
	}
	if sender.proposals == nil {
		t.Fatalf("expected proposals message")
	}
	if len(sender.proposals.Proposals) != 0 {
		t.Fatalf("expected zero proposals, got %d", len(sender.proposals.Proposals))
	}
	if sender.complete == nil || !sender.complete.Success {
		t.Fatalf("expected successful completion message")
	}
	if len(sender.events) == 0 {
		t.Fatalf("expected detector activity events")
	}
	if !strings.Contains(sender.events[0].Message, "run interval") {
		t.Fatalf("unexpected skip message: %q", sender.events[0].Message)
	}
}

func TestAdminScriptDetectCreatesProposalWhenIntervalElapsed(t *testing.T) {
	handler := NewAdminScriptHandler(nil)
	sender := &recordingDetectionSender{}
	err := handler.Detect(context.Background(), &plugin_pb.RunDetectionRequest{
		JobType:           adminScriptJobType,
		LastSuccessfulRun: timestamppb.New(time.Now().Add(-20 * time.Minute)),
		AdminConfigValues: map[string]*plugin_pb.ConfigValue{
			"script": {
				Kind: &plugin_pb.ConfigValue_StringValue{StringValue: defaultAdminScript},
			},
			"run_interval_minutes": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 17},
			},
		},
	}, sender)
	if err != nil {
		t.Fatalf("detect returned err = %v", err)
	}
	if sender.proposals == nil {
		t.Fatalf("expected proposals message")
	}
	if len(sender.proposals.Proposals) != 1 {
		t.Fatalf("expected one proposal, got %d", len(sender.proposals.Proposals))
	}
	if sender.complete == nil || !sender.complete.Success || sender.complete.TotalProposals != 1 {
		t.Fatalf("unexpected completion message: %+v", sender.complete)
	}
}
