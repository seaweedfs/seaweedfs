package s3_constants

const (
	ExtAmzOwnerKey              = "Seaweed-X-Amz-Owner"
	ExtAmzAclKey                = "Seaweed-X-Amz-Acl"
	ExtOwnershipKey             = "Seaweed-X-Amz-Ownership"
	ExtVersioningKey            = "Seaweed-X-Amz-Versioning"
	ExtVersionIdKey             = "Seaweed-X-Amz-Version-Id"
	ExtDeleteMarkerKey          = "Seaweed-X-Amz-Delete-Marker"
	ExtIsLatestKey              = "Seaweed-X-Amz-Is-Latest"
	ExtETagKey                  = "Seaweed-X-Amz-ETag"
	ExtLatestVersionIdKey       = "Seaweed-X-Amz-Latest-Version-Id"
	ExtLatestVersionFileNameKey = "Seaweed-X-Amz-Latest-Version-File-Name"
	ExtAllowEmptyFolders        = "Seaweed-X-Amz-Allow-Empty-Folders"
	// Cached list metadata in .versions directory for single-scan efficiency
	ExtLatestVersionSizeKey        = "Seaweed-X-Amz-Latest-Version-Size"
	ExtLatestVersionETagKey        = "Seaweed-X-Amz-Latest-Version-ETag"
	ExtLatestVersionMtimeKey       = "Seaweed-X-Amz-Latest-Version-Mtime"
	ExtLatestVersionOwnerKey       = "Seaweed-X-Amz-Latest-Version-Owner"
	ExtLatestVersionIsDeleteMarker = "Seaweed-X-Amz-Latest-Version-Is-Delete-Marker"
	ExtMultipartObjectKey          = "key"
	// Wall-clock nanoseconds (int64 as decimal string) captured at the
	// moment a versioned entry was demoted from current to noncurrent
	// by a later PUT or delete marker. Read by the s3 lifecycle engine
	// to compute NoncurrentDays due time; zero/missing falls back to
	// the entry's own mtime so legacy data still expires.
	ExtNoncurrentSinceNsKey = "Seaweed-X-Amz-Noncurrent-Since-Ns"

	// Per-bucket opt-in for the PutObject lifecycle TTL fast path ("true"
	// to enable). When on, an Expiration.Days rule is stamped as a volume
	// TTL at write time instead of being expired by the worker. Off by
	// default: a baked-in TTL can't honor a later policy change (rule
	// removed or lengthened) the way worker-driven expiration does.
	ExtLifecycleTtlFastPathKey = "Seaweed-X-Amz-Lifecycle-Ttl-Fast-Path"

	// S3 checksum storage keys (use x-seaweedfs- prefix to avoid leaking in generic header loop)
	ExtChecksumAlgorithm = "x-seaweedfs-checksum-algorithm"
	ExtChecksumValue     = "x-seaweedfs-checksum-value"

	// Bucket Policy
	ExtBucketPolicyKey = "Seaweed-X-Amz-Bucket-Policy"

	// Object Retention and Legal Hold
	ExtObjectLockModeKey     = "Seaweed-X-Amz-Object-Lock-Mode"
	ExtRetentionUntilDateKey = "Seaweed-X-Amz-Retention-Until-Date"
	ExtLegalHoldKey          = "Seaweed-X-Amz-Legal-Hold"
	ExtObjectLockEnabledKey  = "Seaweed-X-Amz-Object-Lock-Enabled"

	// Object Lock Bucket Configuration (individual components, not XML)
	ExtObjectLockDefaultModeKey  = "Lock-Default-Mode"
	ExtObjectLockDefaultDaysKey  = "Lock-Default-Days"
	ExtObjectLockDefaultYearsKey = "Lock-Default-Years"
)

// Object Lock and Retention Constants
const (
	// Retention modes
	RetentionModeGovernance = "GOVERNANCE"
	RetentionModeCompliance = "COMPLIANCE"

	// Legal hold status
	LegalHoldOn  = "ON"
	LegalHoldOff = "OFF"

	// Object lock enabled status
	ObjectLockEnabled = "Enabled"

	// Bucket versioning status
	VersioningEnabled   = "Enabled"
	VersioningSuspended = "Suspended"
)
