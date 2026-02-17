# Admin Worker Plugin System V2 (Design)

This document proposes a new plugin system for admin-managed workers, implemented in parallel with the current maintenance/worker mechanism.

## Scope

- Add a new plugin protocol and runtime model for multi-language workers.
- Keep all current admin + worker code paths untouched.
- Use gRPC for all admin-worker communication.
- Let workers describe job configuration UI declaratively via protobuf.
- Persist all job type configuration under admin server data directory.
- Support detector workers and executor workers per job type.
- Add end-to-end workflow observability (activities, active jobs, progress).

## New Contract

- Proto file: `weed/pb/plugin.proto`
- gRPC service: `PluginControlService.WorkerStream`
- Connection model: worker-initiated long-lived bidirectional stream.

Why this model:

- Works for workers in any language with gRPC support.
- Avoids admin dialing constraints in NAT/private networks.
- Allows command/response, progress streaming, and heartbeat over one channel.

## Core Runtime Components (Admin Side)

1. `PluginRegistry`
- Tracks connected workers and their per-job-type capabilities.
- Maintains liveness via heartbeat timeout.

2. `SchemaCoordinator`
- For each job type, asks one capable worker for `JobTypeDescriptor`.
- Caches descriptor version and refresh timestamp.

3. `ConfigStore`
- Persists descriptor + saved config values in `dataDir`.
- Stores both:
  - Admin-owned runtime config (detection interval, dispatch concurrency, retry).
  - Worker-owned config values (plugin-specific detection/execution knobs).

4. `DetectorScheduler`
- Per job type, chooses one detector worker (`can_detect=true`).
- Sends `RunDetectionRequest` with saved configs + cluster context.
- Accepts `DetectionProposals`, dedupes by `dedupe_key`, inserts jobs.

5. `JobDispatcher`
- Chooses executor worker (`can_execute=true`) for each pending job.
- Sends `ExecuteJobRequest`.
- Consumes `JobProgressUpdate` and `JobCompleted`.

6. `WorkflowMonitor`
- Builds live counters and timeline from events:
  - activities per job type,
  - active jobs,
  - per-job progress/state,
  - worker health/load.

## Worker Responsibilities

1. Register capabilities on connect (`WorkerHello`).
2. Expose job type descriptor (`ConfigSchemaResponse`) including UI schemas:
- admin config form,
- worker config form,
- defaults.
3. Run detection on demand (`RunDetectionRequest`) and return proposals.
4. Execute assigned jobs (`ExecuteJobRequest`) and stream progress.
5. Heartbeat regularly with slot usage and running work.

## Declarative UI Model

UI is fully derived from protobuf schema:

- `ConfigForm`
- `ConfigSection`
- `ConfigField`
- `ConfigOption`
- `ValidationRule`
- `ConfigValue` (typed scalar/list/map/object value container)

Result:

- Admin can render forms without hardcoded task structs.
- New job types can ship UI schema from worker binary alone.
- Worker language is irrelevant as long as it can emit protobuf messages.

## Detection and Dispatch Flow

1. Worker connects and registers capabilities.
2. Admin requests descriptor per job type.
3. Admin persists descriptor and editable config values.
4. On detection interval (admin-owned setting):
- Admin chooses one detector worker for that job type.
- Sends `RunDetectionRequest` with:
  - `AdminRuntimeConfig`,
  - `admin_config_values`,
  - `worker_config_values`,
  - `ClusterContext` (master/filer/volume grpc locations, metadata).
5. Detector emits `DetectionProposals` and `DetectionComplete`.
6. Admin dedupes and enqueues jobs.
7. Dispatcher assigns jobs to any eligible executor worker.
8. Executor emits `JobProgressUpdate` and `JobCompleted`.
9. Monitor updates workflow UI in near-real-time.

## Persistence Layout (Admin Data Dir)

Proposed layout under `<admin-data-dir>/plugin/`:

- `job_types/<job_type>/descriptor.pb`
- `job_types/<job_type>/descriptor.json`
- `job_types/<job_type>/config.pb`
- `job_types/<job_type>/config.json`
- `jobs/<job_id>.pb`
- `jobs/index.json`
- `activities/<yyyy-mm-dd>.log` (append-only JSON lines)

`config.pb` should use `PersistedJobTypeConfig` from `plugin.proto`.

## Admin UI (V2)

1. Job Types page
- job type status, detector assignment, schema version, last detection run.

2. Job Type Config page
- render both forms from descriptor:
  - admin config form,
  - worker config form.
- save to data dir.

3. Jobs page
- filter by job type/state/worker.
- show pending, active, failed, completed counters.

4. Job Detail page
- timeline of activities,
- stage + progress,
- worker assignment history,
- structured result payload.

5. Workers page
- connected workers, capabilities, slot usage, heartbeat age.

6. Activity page
- global event stream grouped by job type and severity.

## Scheduling Policy (Initial)

Detector selection per job type:
- only workers with `can_detect=true`.
- prefer healthy worker with highest free detection slots.
- lease ends when heartbeat timeout or stream drop.

Execution dispatch:
- only workers with `can_execute=true`.
- select by available execution slots and least active jobs.
- retry on failure using admin runtime retry config.

## Safety and Reliability

- Idempotency: dedupe proposals by (`job_type`, `dedupe_key`).
- Backpressure: enforce max jobs per detection run.
- Timeouts: detection and execution timeout from admin runtime config.
- Replay-safe persistence: write job state changes before emitting UI events.
- Heartbeat-based failover for detector/executor reassignment.

## Backward Compatibility

- Existing `worker.proto` + current maintenance manager remain unchanged.
- V2 introduced as parallel path (`plugin.proto`, new runtime package).
- No migration cut-over in this step.
- Runtime is opt-in via admin flags:
  - `weed admin -plugin.enabled=true`
  - `weed server -admin.plugin.enabled=true` (mini/all-in-one mode)

## Incremental Rollout Plan

Phase 1
- Introduce protocol and storage models only.

Phase 2
- Build admin V2 registry/scheduler/dispatcher behind feature flag.

Phase 3
- Add V2 UI pages and metrics.

Phase 4
- Port one existing job type (e.g. vacuum) as external worker plugin.

Phase 5
- Migrate remaining job types and deprecate old mechanism.

## Agreed Defaults

1. Detector multiplicity
- Exactly one detector worker per job type at a time. Admin selects one worker and runs detection there.

2. Secret handling
- No encryption at rest required for plugin config in this phase.

3. Schema compatibility
- No migration policy required yet; this is a new system.

4. Execution ownership
- Same worker is allowed to do both detection and execution.

5. Retention
- Keep last 10 successful runs and last 10 error runs per job type.
