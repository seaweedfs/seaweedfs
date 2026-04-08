# V2 Design

This directory currently contains both the active V2 design canon and a large
set of working notes, migration packs, and historical comparison material.

Use this README as the navigation layer. If a document is not listed under
`Core Canon`, treat it as supporting or historical context rather than the
current source of truth.

## Core Canon

These are the documents that define the current V2 model and should be read
first.

- `v2-protocol-truths.md` — the stable semantic rules
- `v2-sync-recovery-protocol.md` — sync, keepup, catchup, and rebuild protocol meaning
- `v2-rebuild-mvp-session-protocol.md` — rebuild session contract and data/control lanes
- `v2-automata-ownership-map.md` — assignment, session, and projection ownership
- `v2-protocol-claim-and-evidence.md` — claims and current proof posture
- `v2-validation-matrix.md` — `Rebuild Ready`, `Restore Ready`, and `V2 Ready` gates
- `v2-capability-map.md` — capability-to-proof-tier mapping
- `v2-proof-and-retest-pyramid.md` — proof layering and retest strategy

## Implementation Guides

These help maintainers understand how the current model maps into code.

- `v2-engine-maintainer-tutorial.md`
- `v2-protocol-aware-execution.md`
- `v2-session-protocol-shape.md`
- `v2-two-loop-protocol.md`
- `v2-assignment-translation-unification.md`
- `v2-reuse-replacement-boundary.md`

## Validation And Rollout

These define how the active design is validated, staged, or operationalized.

- `v2-validation-matrix.md`
- `v2-acceptance-criteria.md`
- `v2-product-completion-overview.md`
- `v2-first-launch-supported-matrix.md`
- `v2-legacy-runtime-exit-criteria.md`
- `v2-controlled-rollout-review.md`
- `v2-bounded-internal-pilot-pack.md`
- `v2-pilot-preflight-checklist.md`
- `v2-pilot-stop-conditions.md`

## Working Reference

These are still useful, but they are not the shortest route to the current
truth.

- `v2-open-questions.md`
- `v2-phase-development-plan.md`
- `v2-execution-muscles-inventory.md`
- `v2-scenario-sources-from-v1.md`
- `v2_scenarios.md`
- `v1-v15-v2-comparison.md`
- `v2-algorithm-overview.md`
- `v2-algorithm-overview.zh.md`
- `v2-detailed-algorithm.zh.md`
- `v2-semantic-methodology.zh.md`
- `v2-protocol-closure-map.zh.md`

## Migration And Historical Working Set

These files are mostly valuable for reconstruction of design history, migration
intent, or earlier prototype shapes. They should usually not be the first docs
opened during current development.

- `v2-first-migration-batch.md`
- `v2-first-migration-task-pack.md`
- `v2-second-migration-batch.md`
- `v2-second-migration-task-pack.md`
- `v2-third-migration-batch.md`
- `v2-third-migration-task-pack.md`
- `v2-phase14plus-semantic-framework.md`
- `v2-pure-runtime-rf1-bootstrap.md`
- `v2-volumev2-single-node-mvp.md`
- `v2-loop1-surface-draft.md`
- `v2-rf2-runtime-bounded-envelope.md`
- `v2-rf2-runtime-bounded-envelope-review.md`
- `v2-separation-port-layer-audit.md`
- `v2_mini_core_design.md`
- `wal-replication-v2.md`
- `wal-replication-v2-state-machine.md`
- `wal-replication-v2-orchestrator.md`
- `wal-v2-tiny-prototype.md`
- `wal-v1-to-v2-mapping.md`
- `v2-dist-fsm.md`
- `v1-v15-v2-simulator-goals.md`
- `protocol-version-simulation.md`

## Process

- `protocol-development-process.md`
- `agent_dev_process.md`

## Cleanup Rule

When a document is superseded, prefer:

1. keeping one canonical file in `Core Canon`
2. leaving older reasoning in `Migration And Historical Working Set`
3. avoiding duplicate "read first" lists across many files

Future cleanup should physically move or archive files only after their inbound
references are reviewed.

## Execution Note

- active development tracking lives under `../.private/phase/`
- current phase contract and slice packages live there rather than in this directory

The original project-level copies under `learn/projects/sw-block/design/`
remain as shared references for now.
