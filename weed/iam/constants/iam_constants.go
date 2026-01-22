package constants

const (
	IamAPIDomainName   = "iam.amazonaws.com"
	IamAPIVersion      = "2010-05-08"
	
	ActionRead                        = "Read"
	ActionReadAcp                     = "ReadAcp"
	ActionWrite                       = "Write"
	ActionWriteAcp                    = "WriteAcp"
	ActionAdmin                       = "Admin"
	ActionTagging                     = "Tagging"
	ActionList                        = "List"
	ActionDeleteBucket                = "DeleteBucket"
	ActionBypassGovernanceRetention   = "BypassGovernanceRetention"
	ActionGetObjectRetention          = "GetObjectRetention"
	ActionPutObjectRetention          = "PutObjectRetention"
	ActionGetObjectLegalHold          = "GetObjectLegalHold"
	ActionPutObjectLegalHold          = "PutObjectLegalHold"
	ActionGetBucketObjectLockConfig   = "GetBucketObjectLockConfiguration"
	ActionPutBucketObjectLockConfig   = "PutBucketObjectLockConfiguration"
	
	// Permissions (Legacy/Alias)
	PermissionRead     = ActionRead
	PermissionWrite    = ActionWrite
	PermissionAdmin    = ActionAdmin
)
