package s3_lifecycle

// Contract with weed/admin/plugin/cluster_rate_limit.go: admin computes
// the per-worker share from the *AdminKey fields and writes it to
// ExecuteJobRequest.ClusterContext.Metadata under the MetadataKey* keys.
// Changing a name on one side without the other silently disables rate
// limiting.

const (
	// 0 = unlimited.
	ClusterDeletesPerSecondAdminKey = "cluster_deletes_per_second"
	// 0 = 2 * rps.
	ClusterDeletesBurstAdminKey = "cluster_deletes_burst"

	// Per-worker share. Missing/empty/zero -> cfg.Limiter stays nil.
	MetadataKeyDeletesPerSecond = "s3_lifecycle.deletes_per_second"
	MetadataKeyDeletesBurst     = "s3_lifecycle.deletes_burst"
)
