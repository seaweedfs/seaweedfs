// Package blockvol bridges the V2 engine to real blockvol storage and
// control-plane state.
//
// This package implements the adapter interfaces defined in
// sw-block/engine/replication/ using real blockvol internals as the
// source of truth.
//
// Hard rules (Phase 07):
//   - ReplicaID = <volume-name>/<replica-server-id> (not address-derived)
//   - blockvol executes recovery I/O but does NOT own recovery policy
//   - Engine decides zero-gap vs catch-up vs rebuild
//   - Bridge translates engine decisions into blockvol actions
//
// Adapter replacement order:
//   P0: control_adapter (assignment → engine intent)
//   P0: storage_adapter (blockvol state → RetainedHistory)
//   P1: executor_bridge (engine executor → blockvol I/O)
//   P1: observe_adapter (engine status → service diagnostics)
package blockvol
