package pluginworker

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// VacuumHandler is the plugin job handler for vacuum job type.
type VacuumHandler struct{}

func NewVacuumHandler() *VacuumHandler {
	return &VacuumHandler{}
}

func (h *VacuumHandler) Capability() *plugin_pb.JobTypeCapability {
	return &plugin_pb.JobTypeCapability{
		JobType:                 "vacuum",
		CanDetect:               true,
		CanExecute:              true,
		MaxDetectionConcurrency: 1,
		MaxExecutionConcurrency: 2,
		DisplayName:             "Volume Vacuum",
		Description:             "Reclaims disk space by removing deleted files from volumes",
	}
}

func (h *VacuumHandler) Descriptor() *plugin_pb.JobTypeDescriptor {
	return &plugin_pb.JobTypeDescriptor{
		JobType:           "vacuum",
		DisplayName:       "Volume Vacuum",
		Description:       "Detect and vacuum volumes with high garbage ratio",
		Icon:              "fas fa-broom",
		DescriptorVersion: 1,
		AdminConfigForm: &plugin_pb.ConfigForm{
			FormId:      "vacuum-admin",
			Title:       "Vacuum Admin Config",
			Description: "Admin-side controls for vacuum detection scope.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "scope",
					Title:       "Scope",
					Description: "Optional filter to restrict detection.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "collection_filter",
							Label:       "Collection Filter",
							Description: "Only scan this collection when set.",
							Placeholder: "all collections",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_STRING,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_TEXT,
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"collection_filter": {
					Kind: &plugin_pb.ConfigValue_StringValue{StringValue: ""},
				},
			},
		},
		WorkerConfigForm: &plugin_pb.ConfigForm{
			FormId:      "vacuum-worker",
			Title:       "Vacuum Worker Config",
			Description: "Worker-side vacuum thresholds.",
			Sections: []*plugin_pb.ConfigSection{
				{
					SectionId:   "thresholds",
					Title:       "Thresholds",
					Description: "Detection thresholds and timing constraints.",
					Fields: []*plugin_pb.ConfigField{
						{
							Name:        "garbage_threshold",
							Label:       "Garbage Threshold",
							Description: "Detect volumes with garbage ratio >= threshold.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_DOUBLE,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0}},
							MaxValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 1}},
						},
						{
							Name:        "min_volume_age_seconds",
							Label:       "Min Volume Age (s)",
							Description: "Only detect volumes older than this age.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
						},
						{
							Name:        "min_interval_seconds",
							Label:       "Min Interval (s)",
							Description: "Minimum interval between vacuum on the same volume.",
							FieldType:   plugin_pb.ConfigFieldType_CONFIG_FIELD_TYPE_INT64,
							Widget:      plugin_pb.ConfigWidget_CONFIG_WIDGET_NUMBER,
							Required:    true,
							MinValue:    &plugin_pb.ConfigValue{Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 0}},
						},
					},
				},
			},
			DefaultValues: map[string]*plugin_pb.ConfigValue{
				"garbage_threshold": {
					Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0.3},
				},
				"min_volume_age_seconds": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 24 * 60 * 60},
				},
				"min_interval_seconds": {
					Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 7 * 24 * 60 * 60},
				},
			},
		},
		AdminRuntimeDefaults: &plugin_pb.AdminRuntimeDefaults{
			Enabled:                       true,
			DetectionIntervalSeconds:      2 * 60 * 60,
			DetectionTimeoutSeconds:       120,
			MaxJobsPerDetection:           200,
			GlobalExecutionConcurrency:    2,
			PerWorkerExecutionConcurrency: 2,
			RetryLimit:                    1,
			RetryBackoffSeconds:           10,
		},
		WorkerDefaultValues: map[string]*plugin_pb.ConfigValue{
			"garbage_threshold": {
				Kind: &plugin_pb.ConfigValue_DoubleValue{DoubleValue: 0.3},
			},
			"min_volume_age_seconds": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 24 * 60 * 60},
			},
			"min_interval_seconds": {
				Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 7 * 24 * 60 * 60},
			},
		},
	}
}

func (h *VacuumHandler) Detect(_ context.Context, _ *plugin_pb.RunDetectionRequest, _ DetectionSender) error {
	return fmt.Errorf("vacuum detection not implemented yet")
}

func (h *VacuumHandler) Execute(_ context.Context, _ *plugin_pb.ExecuteJobRequest, _ ExecutionSender) error {
	return fmt.Errorf("vacuum execution not implemented yet")
}
