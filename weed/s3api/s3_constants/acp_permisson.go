package s3_constants

var (
	PermissionFullControl = "FULL_CONTROL"
	PermissionRead        = "READ"
	PermissionWrite       = "WRITE"
	PermissionReadAcp     = "READ_ACP"
	PermissionWriteAcp    = "WRITE_ACP"
)

// ValidatePermission reports whether permission is one of the values the S3
// Grant type allows. The comparison is exact: S3 does not accept other casing
// or surrounding whitespace.
func ValidatePermission(permission string) bool {
	switch permission {
	case PermissionFullControl, PermissionRead, PermissionWrite, PermissionReadAcp, PermissionWriteAcp:
		return true
	}
	return false
}
