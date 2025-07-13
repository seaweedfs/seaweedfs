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

	// Bucket Policy
	ExtBucketPolicyKey = "Seaweed-X-Amz-Bucket-Policy"

	// Object Retention and Legal Hold
	ExtObjectLockModeKey     = "Seaweed-X-Amz-Object-Lock-Mode"
	ExtRetentionUntilDateKey = "Seaweed-X-Amz-Retention-Until-Date"
	ExtLegalHoldKey          = "Seaweed-X-Amz-Legal-Hold"
	ExtObjectLockEnabledKey  = "Seaweed-X-Amz-Object-Lock-Enabled"
	ExtObjectLockConfigKey   = "Seaweed-X-Amz-Object-Lock-Config"
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
)
