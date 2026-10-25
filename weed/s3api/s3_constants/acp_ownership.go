package s3_constants

var (
	OwnershipBucketOwnerPreferred = "BucketOwnerPreferred"
	OwnershipObjectWriter         = "ObjectWriter"
	OwnershipBucketOwnerEnforced  = "BucketOwnerEnforced"

	DefaultOwnershipForCreate = OwnershipObjectWriter
	DefaultOwnershipForExists = OwnershipBucketOwnerEnforced
)

func ValidateOwnership(ownership string) bool {
	if ownership == "" || (ownership != OwnershipBucketOwnerPreferred && ownership != OwnershipObjectWriter && ownership != OwnershipBucketOwnerEnforced) {
		return false
	} else {
		return true
	}
}

// EffectiveOwnership resolves a stored ownership setting to the one that governs
// the bucket: absent or invalid behaves as BucketOwnerEnforced.
func EffectiveOwnership(ownership string) string {
	if !ValidateOwnership(ownership) {
		return DefaultOwnershipForExists
	}
	return ownership
}
