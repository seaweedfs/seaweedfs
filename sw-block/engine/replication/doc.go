// Package replication implements V2 per-replica sender/session ownership.
//
// This is the real V2 engine core, promoted from the prototype at
// sw-block/prototype/enginev2/. It preserves all accepted invariants:
//
//   - One stable Sender per replica, identified by ReplicaID
//   - One active Session per replica per epoch
//   - Session identity fencing: stale sessionID rejected at every execution API
//   - Endpoint change invalidates active session
//   - Epoch bump invalidates all stale-epoch sessions
//   - Catch-up is bounded (frozen target, budget enforcement)
//   - Rebuild is a separate, exclusive sender-owned execution path
//   - Completion requires convergence (catch-up) or ReadyToComplete (rebuild)
//
// File layout:
//
// Slice 1 core (ownership/fencing):
//   types.go     — Endpoint, ReplicaState, SessionKind, SessionPhase
//   sender.go    — Sender: per-replica owner with execution APIs
//   session.go   — Session: recovery lifecycle with FSM phases
//   registry.go  — Registry: sender group with reconcile + assignment intent
//
// Carried forward from prototype (accepted in Phase 4.5):
//   budget.go    — CatchUpBudget: bounded catch-up enforcement
//   rebuild.go   — RebuildState: rebuild execution FSM
//   outcome.go   — HandshakeResult, RecoveryOutcome classification
package replication
