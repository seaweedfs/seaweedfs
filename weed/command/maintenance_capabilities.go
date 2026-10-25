package command

import (
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
)

// parseCapabilities converts comma-separated legacy maintenance capabilities to task types.
// This remains for mini-mode maintenance worker wiring.
func parseCapabilities(capabilityStr string) []types.TaskType {
	if capabilityStr == "" {
		return nil
	}

	capabilityMap := map[string]types.TaskType{}

	typesRegistry := tasks.GetGlobalTypesRegistry()
	for taskType := range typesRegistry.GetAllDetectors() {
		capabilityMap[strings.ToLower(string(taskType))] = taskType
	}

	if taskType, exists := capabilityMap["erasure_coding"]; exists {
		capabilityMap["ec"] = taskType
	}
	if taskType, exists := capabilityMap["remote_upload"]; exists {
		capabilityMap["remote"] = taskType
	}
	if taskType, exists := capabilityMap["fix_replication"]; exists {
		capabilityMap["replication"] = taskType
	}

	var capabilities []types.TaskType
	parts := strings.Split(capabilityStr, ",")

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if taskType, exists := capabilityMap[part]; exists {
			capabilities = append(capabilities, taskType)
		} else {
			glog.Warningf("Unknown capability: %s", part)
		}
	}

	return capabilities
}
