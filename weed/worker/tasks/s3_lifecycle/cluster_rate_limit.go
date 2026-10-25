package s3_lifecycle

// Cluster-wide rate-limit configuration plumbing for the daily-replay
// worker. The admin holds a single "cluster delete budget" knob, divides
// it by the number of execute-capable s3_lifecycle workers at job-dispatch
// time, and ships the per-worker share to the worker via
// ExecuteJobRequest.ClusterContext.Metadata. The worker reads the share,
// constructs a rate.Limiter, and passes it to dailyrun.Run.
//
// These constants are the contract between admin (weed/admin/plugin/plugin.go
// computes the share and writes the keys) and worker (this package's
// handler.go reads them). Changing a name on one side without the other
// would silently disable rate limiting — both sides must read these
// exact values.

const (
	// ClusterDeletesPerSecondAdminKey is the admin-config field that
	// holds the cluster-wide budget in delete RPCs per second. 0 means
	// unlimited (legacy behavior). Set via the AdminConfigForm in
	// handler.go's "Scope" section.
	ClusterDeletesPerSecondAdminKey = "cluster_deletes_per_second"
	// ClusterDeletesBurstAdminKey holds the token-bucket burst. 0 means
	// "2 × rps" (computed by the admin allocator).
	ClusterDeletesBurstAdminKey = "cluster_deletes_burst"
	// MetaLogRetentionDaysAdminKey holds the operator's declaration of
	// how far back the filer's meta-log subscription can reliably reach.
	// Rules whose effective TTL exceeds this window can't be serviced by
	// replay alone and get partitioned into engine.PromotedHash's walk
	// set; a partition flip (operator shrinks retention) then trips the
	// recovery branch on the next run. 0 = unbounded (current behavior,
	// falls back to maxTTL in runShard so PromotedHash stays empty).
	MetaLogRetentionDaysAdminKey = "meta_log_retention_days"
	// WalkerIntervalMinutesAdminKey throttles the per-shard steady-state
	// and empty-replay walker fires. dailyrun.runShard checks the time
	// since the persisted Cursor.LastWalkedNs and skips the walk when
	// less than this interval has elapsed; cold-start and recovery walker
	// fires (RecoveryView) stay unconditional. 0 means "fire on every
	// run" (the prior behavior — appropriate when the worker is driven
	// at the operator's intended walk cadence, e.g. once per hour).
	// Production deployments running the worker at multi-second cadence
	// (CI ticks, sub-minute admin schedules) should set this to roughly
	// the per-shard walk budget — typically 60 (1h) for small clusters,
	// 360+ (6h+) for large ones.
	WalkerIntervalMinutesAdminKey = "walker_interval_minutes"

	// MetadataKeyDeletesPerSecond is the per-worker share value the
	// admin writes into ClusterContext.Metadata at ExecuteJob time.
	// Stored as a string of a non-negative float64; empty/missing/zero
	// means "no rate limit on this run" (cfg.Limiter stays nil).
	MetadataKeyDeletesPerSecond = "s3_lifecycle.deletes_per_second"
	// MetadataKeyDeletesBurst is the per-worker burst share. Stored as
	// a string of a non-negative integer.
	MetadataKeyDeletesBurst = "s3_lifecycle.deletes_burst"
)
