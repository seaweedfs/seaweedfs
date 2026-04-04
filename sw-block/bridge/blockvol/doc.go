// Package blockvol defines the weed-free bridge contracts that connect the V2
// engine to blockvol-backed control and execution mechanics.
//
// This package owns:
//   - stable control translation helpers
//   - storage/execution port contracts
//   - thin adapters that consume those contracts without importing weed/
//
// Real blockvol-backed implementations live outside this package (today under
// weed/storage/blockvol/v2bridge/). This package must remain reusable from
// sw-block without directly depending on weed/.
//
// Hard rules (Phase 07):
//   - ReplicaID = <volume-name>/<replica-server-id> (not address-derived)
//   - blockvol executes recovery I/O but does NOT own recovery policy
//   - Engine decides zero-gap vs catch-up vs rebuild
//   - Bridge translates engine decisions into blockvol actions
//
// Adapter replacement order:
//
//	P0: control_adapter (assignment → engine intent)
//	P0: storage_adapter (blockvol state → RetainedHistory)
//	P1: executor_bridge (engine executor → blockvol I/O)
//	P1: observe_adapter (engine status → service diagnostics)
package blockvol
