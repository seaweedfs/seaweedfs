// Package block is the SeaweedFS block storage product pack for sw-test-runner.
// It registers block-specific actions (iSCSI, NVMe, target lifecycle, devops,
// snapshots, database workloads, metrics, and Kubernetes) on top of the
// product-agnostic runner core.
//
// Action implementations live in testrunner/actions/ for now (shared package).
// This registration boundary is the structural split point — the physical file
// move into this package happens when the standalone module is created (Step 3).
package block

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner/actions"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
)

// RegisterPack registers all block-specific actions on the registry.
// Core actions (exec, sleep, assert_*, bench) are NOT registered here —
// they are registered by actions.RegisterCore().
func RegisterPack(r *tr.Registry) {
	actions.RegisterBlockActions(r)
	actions.RegisterISCSIActions(r)
	actions.RegisterNVMeActions(r)
	actions.RegisterIOActions(r)
	actions.RegisterDevOpsActions(r)
	actions.RegisterSnapshotActions(r)
	actions.RegisterDatabaseActions(r)
	actions.RegisterMetricsActions(r)
	actions.RegisterK8sActions(r)
}
