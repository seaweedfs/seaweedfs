package s3_constants

// Amazon S3 predefined groups
var (
	GranteeGroupAllUsers           = "http://acs.amazonaws.com/groups/global/AllUsers"
	GranteeGroupAuthenticatedUsers = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
	GranteeGroupLogDelivery        = "http://acs.amazonaws.com/groups/s3/LogDelivery"
)

func ValidateGroup(group string) bool {
	valid := true
	switch group {
	case GranteeGroupAllUsers:
	case GranteeGroupLogDelivery:
	case GranteeGroupAuthenticatedUsers:
	default:
		valid = false
	}
	return valid
}
