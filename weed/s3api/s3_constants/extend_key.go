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
	ExtS3ImplicitDir            = "Seaweed-X-Amz-Implicit-Dir"
	// Cached list metadata in .versions directory for single-scan efficiency
	ExtLatestVersionSizeKey        = "Seaweed-X-Amz-Latest-Version-Size"
	ExtLatestVersionETagKey        = "Seaweed-X-Amz-Latest-Version-ETag"
	ExtLatestVersionMtimeKey       = "Seaweed-X-Amz-Latest-Version-Mtime"
	ExtLatestVersionOwnerKey       = "Seaweed-X-Amz-Latest-Version-Owner"
	ExtLatestVersionIsDeleteMarker = "Seaweed-X-Amz-Latest-Version-Is-Delete-Marker"
	ExtMultipartObjectKey          = "key"

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
