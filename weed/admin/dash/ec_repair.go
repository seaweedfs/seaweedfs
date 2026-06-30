package dash

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	ecRepairJobType = "admin_script"
	ecRepairTimeout = 30 * time.Minute
)

// StartEcVolumeRepair starts an admin-script plugin job that runs ec.rebuild for
// one EC volume. The job runs asynchronously so the UI request does not block on
// a potentially long rebuild.
func (s *AdminServer) StartEcVolumeRepair(volumeID uint32, collection string) (string, error) {
	if volumeID == 0 {
		return "", fmt.Errorf("volume ID is required")
	}
	if s.plugin == nil {
		return "", fmt.Errorf("plugin is not enabled")
	}
	collection = strings.TrimSpace(collection)
	if err := validateEcRepairCollection(collection); err != nil {
		return "", err
	}
	if !s.plugin.HasCapableWorker(ecRepairJobType) {
		return "", fmt.Errorf("no admin_script plugin worker is connected")
	}

	job := buildEcVolumeRepairJob(volumeID, collection)
	clusterContext := s.buildDefaultPluginClusterContext()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), ecRepairTimeout)
		defer cancel()
		if _, err := s.ExecutePluginJob(ctx, job, clusterContext, 1); err != nil {
			glog.Warningf("EC volume repair job %s failed: %v", job.JobId, err)
		}
	}()

	return job.JobId, nil
}

func buildEcVolumeRepairJob(volumeID uint32, collection string) *plugin_pb.JobSpec {
	now := timestamppb.Now()
	collection = strings.TrimSpace(collection)
	script := buildEcVolumeRepairScript(volumeID, collection)
	scriptHash := hashEcRepairScript(script)
	jobID := fmt.Sprintf("ec-repair-%d-%d", volumeID, now.AsTime().UnixNano())
	summary := fmt.Sprintf("Repair EC volume %d", volumeID)
	if collection != "" {
		summary = fmt.Sprintf("Repair EC volume %d in collection %s", volumeID, collection)
	}

	parameters := map[string]*plugin_pb.ConfigValue{
		"script": {
			Kind: &plugin_pb.ConfigValue_StringValue{StringValue: script},
		},
		"script_name": {
			Kind: &plugin_pb.ConfigValue_StringValue{StringValue: summary},
		},
		"script_hash": {
			Kind: &plugin_pb.ConfigValue_StringValue{StringValue: scriptHash},
		},
		"command_count": {
			Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: 1},
		},
	}

	labels := map[string]string{
		"operation":   "ec_repair",
		"volume_id":   strconv.FormatUint(uint64(volumeID), 10),
		"script_hash": scriptHash,
	}
	if collection != "" {
		labels["collection"] = collection
	}

	return &plugin_pb.JobSpec{
		JobId:       jobID,
		JobType:     ecRepairJobType,
		Priority:    plugin_pb.JobPriority_JOB_PRIORITY_HIGH,
		Summary:     summary,
		Detail:      "Run ec.rebuild for missing EC shards",
		DedupeKey:   "ec-repair:" + scriptHash,
		Parameters:  parameters,
		Labels:      labels,
		CreatedAt:   now,
		ScheduledAt: now,
	}
}

func buildEcVolumeRepairScript(volumeID uint32, collection string) string {
	collection = strings.TrimSpace(collection)
	args := []string{"ec.rebuild"}
	if collection != "" {
		args = append(args, "-collection="+collection)
	}
	args = append(args, "-volumeIds="+strconv.FormatUint(uint64(volumeID), 10), "-apply")
	return strings.Join(args, " ")
}

func validateEcRepairCollection(collection string) error {
	if strings.ContainsAny(collection, " \t\r\n;'\"") {
		return fmt.Errorf("collection contains unsupported characters")
	}
	return nil
}

func hashEcRepairScript(script string) string {
	sum := sha256.Sum256([]byte(script))
	return hex.EncodeToString(sum[:])
}
