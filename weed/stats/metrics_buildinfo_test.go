package stats_test

import (
	"runtime"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	_ "github.com/seaweedfs/seaweedfs/weed/util/version" // Import to trigger version init
)

func TestBuildInfo(t *testing.T) {
	// Verify that BuildInfo metric is registered and has the expected value
	count := testutil.CollectAndCount(stats.BuildInfo)
	if count != 1 {
		t.Errorf("Expected 1 BuildInfo metric, got %d", count)
	}

	// Verify the metric can be gathered
	metrics, err := stats.Gather.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics: %v", err)
	}

	// Find the build_info metric
	found := false
	for _, mf := range metrics {
		if mf.GetName() == "SeaweedFS_build_info" {
			found = true
			metric := mf.GetMetric()[0]

			// Verify the metric value is 1
			if metric.GetGauge().GetValue() != 1 {
				t.Errorf("Expected BuildInfo value to be 1, got %f", metric.GetGauge().GetValue())
			}

			// Verify labels exist
			labels := metric.GetLabel()
			labelMap := make(map[string]string)
			for _, label := range labels {
				labelMap[label.GetName()] = label.GetValue()
			}

			// Check required labels
			if _, ok := labelMap["version"]; !ok {
				t.Error("Missing 'version' label")
			}
			if _, ok := labelMap["commit"]; !ok {
				t.Error("Missing 'commit' label")
			}
			if _, ok := labelMap["sizelimit"]; !ok {
				t.Error("Missing 'sizelimit' label")
			}
			if labelMap["goos"] != runtime.GOOS {
				t.Errorf("Expected goos='%s', got '%s'", runtime.GOOS, labelMap["goos"])
			}
			if labelMap["goarch"] != runtime.GOARCH {
				t.Errorf("Expected goarch='%s', got '%s'", runtime.GOARCH, labelMap["goarch"])
			}

			// Verify version format
			if !strings.Contains(labelMap["version"], ".") {
				t.Errorf("Version should contain a dot: %s", labelMap["version"])
			}

			// Verify sizelimit is either 30GB or 8000GB
			if labelMap["sizelimit"] != "30GB" && labelMap["sizelimit"] != "8000GB" {
				t.Errorf("Expected sizelimit to be '30GB' or '8000GB', got '%s'", labelMap["sizelimit"])
			}

			t.Logf("BuildInfo metric: version=%s, commit=%s, sizelimit=%s, goos=%s, goarch=%s",
				labelMap["version"], labelMap["commit"], labelMap["sizelimit"],
				labelMap["goos"], labelMap["goarch"])
		}
	}

	if !found {
		t.Error("BuildInfo metric not found in gathered metrics")
	}
}
