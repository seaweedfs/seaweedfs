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
