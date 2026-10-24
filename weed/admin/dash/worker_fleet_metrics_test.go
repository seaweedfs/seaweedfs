package dash

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/admin/maintenance"
	adminplugin "github.com/seaweedfs/seaweedfs/weed/admin/plugin"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// TestMergeWorkerFleetTotals covers the accounting behind
// SeaweedFS_admin_workers_connected / SeaweedFS_admin_worker_slots and the
// dashboard's Workers card. Both used to read the legacy maintenance-worker
// registry only, so a cluster whose admin and workers run as separate
// components reported 0 workers (issue #10525).
func TestMergeWorkerFleetTotals(t *testing.T) {
	pluginWorker := func(id string, detectUsed, detectTotal, executeUsed, executeTotal int32) *adminplugin.WorkerSession {
		return &adminplugin.WorkerSession{
			WorkerID: id,
			Heartbeat: &plugin_pb.WorkerHeartbeat{
				DetectionSlotsUsed:  detectUsed,
				DetectionSlotsTotal: detectTotal,
				ExecutionSlotsUsed:  executeUsed,
				ExecutionSlotsTotal: executeTotal,
			},
		}
	}

	testCases := []struct {
		name          string
		legacySlots   map[string]maintenance.WorkerSlots
		pluginWorkers []*adminplugin.WorkerSession
		wantWorkers   int
		wantUsedSlots int
		wantMaxSlots  int
	}{
		{
			name: "no workers at all",
		},
		{
			name: "legacy workers only, unchanged accounting",
			legacySlots: map[string]maintenance.WorkerSlots{
				"w-legacy-a": {Used: 1, Max: 2},
				"w-legacy-b": {Used: 0, Max: 4},
			},
			wantWorkers:   2,
			wantUsedSlots: 1,
			wantMaxSlots:  6,
		},
		{
			// The reported bug: admin and workers deployed separately, so
			// nothing ever lands in the legacy registry.
			name: "plugin workers only are counted",
			pluginWorkers: []*adminplugin.WorkerSession{
				pluginWorker("w-plugin-a", 0, 1, 2, 4),
				pluginWorker("w-plugin-b", 1, 1, 0, 4),
			},
			wantWorkers:   2,
			wantUsedSlots: 3,
			wantMaxSlots:  10,
		},
		{
			name: "distinct workers in both registries are summed",
			legacySlots: map[string]maintenance.WorkerSlots{
				"w-legacy-a": {Used: 1, Max: 2},
			},
			pluginWorkers: []*adminplugin.WorkerSession{
				pluginWorker("w-plugin-a", 0, 1, 1, 4),
			},
			wantWorkers:   2,
			wantUsedSlots: 2,
			wantMaxSlots:  7,
		},
		{
			// `weed mini` runs both worker runtimes out of one working
			// directory, so they share the persisted worker ID and must not be
			// counted twice. Legacy slots win for such a worker.
			name: "same worker ID in both registries counts once",
			legacySlots: map[string]maintenance.WorkerSlots{
				"w-host-abcd": {Used: 1, Max: 2},
			},
			pluginWorkers: []*adminplugin.WorkerSession{
				pluginWorker("w-host-abcd", 1, 1, 3, 4),
			},
			wantWorkers:   1,
			wantUsedSlots: 1,
			wantMaxSlots:  2,
		},
		{
			name: "plugin worker without a heartbeat still counts as connected",
			pluginWorkers: []*adminplugin.WorkerSession{
				{WorkerID: "w-plugin-a"},
			},
			wantWorkers: 1,
		},
		{
			name:          "nil session is skipped",
			pluginWorkers: []*adminplugin.WorkerSession{nil, pluginWorker("w-plugin-a", 0, 1, 0, 2)},
			wantWorkers:   1,
			wantMaxSlots:  3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			workers, usedSlots, maxSlots := mergeWorkerFleetTotals(tc.legacySlots, tc.pluginWorkers)
			if workers != tc.wantWorkers {
				t.Errorf("workers = %d, want %d", workers, tc.wantWorkers)
			}
			if usedSlots != tc.wantUsedSlots {
				t.Errorf("usedSlots = %d, want %d", usedSlots, tc.wantUsedSlots)
			}
			if maxSlots != tc.wantMaxSlots {
				t.Errorf("maxSlots = %d, want %d", maxSlots, tc.wantMaxSlots)
			}
		})
	}
}

// TestWorkerFleetTotalsWithoutMaintenanceManager guards the nil-manager path:
// an admin server with no maintenance manager must still report its plugin
// workers rather than returning early.
func TestWorkerFleetTotalsWithoutMaintenanceManager(t *testing.T) {
	server := &AdminServer{}

	workers, usedSlots, maxSlots := server.workerFleetTotals()
	if workers != 0 || usedSlots != 0 || maxSlots != 0 {
		t.Fatalf("empty admin server reported workers=%d used=%d max=%d, want all 0", workers, usedSlots, maxSlots)
	}
}
