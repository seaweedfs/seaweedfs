package app

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/admin/dash"
)

// The size card reports the bytes in use on the volume servers' disks, which
// is what fills them, rather than the size of the data they hold.
func TestAdminDashboardShowsDiskUsage(t *testing.T) {
	const tib = int64(1) << 40
	data := dash.AdminData{
		TotalSize:    276 * tib,
		DiskUsage:    348 * tib,
		DiskCapacity: 1000 * tib,
	}
	var buf bytes.Buffer
	if err := Admin(data).Render(context.Background(), &buf); err != nil {
		t.Fatalf("render: %v", err)
	}
	got := buf.String()
	for _, want := range []string{"Disk usage", "348.0 TB", "of 1000.0 TB (34%)"} {
		if !strings.Contains(got, want) {
			t.Errorf("dashboard is missing %q", want)
		}
	}
	if strings.Contains(got, "276.0 TB") {
		t.Error("dashboard still shows the logical data size")
	}
}

func TestCalculatePercent64PetabyteScale(t *testing.T) {
	const pib = int64(1) << 50
	if got := calculatePercent64(100*pib, 200*pib); got != 50 {
		t.Errorf("100 of 200 PiB = %d%%, want 50%%", got)
	}
}

func TestClusterHealthSummary(t *testing.T) {
	healthy := dash.AdminData{
		MasterNodes:   []dash.MasterNode{{Address: "m1:9333", IsLeader: true}},
		VolumeServers: []dash.VolumeServer{{ID: "v1"}, {ID: "v2"}},
		FilerNodes:    []dash.FilerNode{{Address: "f1:8888"}},
		DiskUsage:     50,
		DiskCapacity:  100,
	}
	if got, want := clusterHealthSummary(healthy), "Healthy. 2 volume servers and 1 filer reporting, 1 master configured."; got != want {
		t.Errorf("healthy summary = %q, want %q", got, want)
	}

	degraded := healthy
	degraded.MasterNodes = []dash.MasterNode{{Address: "m1:9333"}}
	degraded.DiskUsage = 95
	want := "Needs attention: no master leader, disks 95% full. 2 volume servers and 1 filer reporting, 1 master configured."
	if got := clusterHealthSummary(degraded); got != want {
		t.Errorf("degraded summary = %q, want %q", got, want)
	}

	if got := clusterHealthIssues(dash.AdminData{}); strings.Join(got, ", ") != "no master reachable, no volume servers" {
		t.Errorf("empty cluster issues = %q", got)
	}
}
