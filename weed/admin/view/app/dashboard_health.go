package app

import (
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
)

const diskFullWarnPercent = 90

func clusterHealthIssues(data dash.AdminData) []string {
	var issues []string
	if len(data.MasterNodes) == 0 {
		issues = append(issues, "no master reachable")
	} else if !hasMasterLeader(data.MasterNodes) {
		issues = append(issues, "no master leader")
	}
	if len(data.VolumeServers) == 0 {
		issues = append(issues, "no volume servers")
	}
	if data.DiskCapacity > 0 {
		if p := calculatePercent64(data.DiskUsage, data.DiskCapacity); p >= diskFullWarnPercent {
			issues = append(issues, fmt.Sprintf("disks %d%% full", p))
		}
	}
	if data.VersionSummary.Mixed() {
		issues = append(issues, "mixed server versions")
	}
	return issues
}

func clusterHealthSummary(data dash.AdminData) string {
	nodes := fmt.Sprintf("%s and %s reporting, %s configured.",
		countNoun(len(data.VolumeServers), "volume server"),
		countNoun(len(data.FilerNodes), "filer"),
		countNoun(len(data.MasterNodes), "master"))
	issues := clusterHealthIssues(data)
	if len(issues) == 0 {
		return "Healthy. " + nodes
	}
	return "Needs attention: " + strings.Join(issues, ", ") + ". " + nodes
}

func hasMasterLeader(masters []dash.MasterNode) bool {
	for _, m := range masters {
		if m.IsLeader {
			return true
		}
	}
	return false
}

func countNoun(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}
