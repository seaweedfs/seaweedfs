package actions

import tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"

// RegisterAll registers all action handlers on the given registry.
func RegisterAll(r *tr.Registry) {
	RegisterBlockActions(r)
	RegisterISCSIActions(r)
	RegisterNVMeActions(r)
	RegisterIOActions(r)
	RegisterFaultActions(r)
	RegisterSystemActions(r)
	RegisterMetricsActions(r)
	RegisterBenchActions(r)
	RegisterDevOpsActions(r)
	RegisterSnapshotActions(r)
	RegisterDatabaseActions(r)
	RegisterK8sActions(r)
}
