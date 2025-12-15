package iam

// Character sets for credential generation
const (
	CharsetUpper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	Charset      = CharsetUpper + "abcdefghijklmnopqrstuvwxyz/"
)

// Policy document version
const PolicyDocumentVersion = "2012-10-17"

// Error message templates
const UserDoesNotExist = "the user with name %s cannot be found."

// Statement action constants - these map to IAM policy actions
const (
	StatementActionAdmin    = "*"
	StatementActionWrite    = "Put*"
	StatementActionWriteAcp = "PutBucketAcl"
	StatementActionRead     = "Get*"
	StatementActionReadAcp  = "GetBucketAcl"
	StatementActionList     = "List*"
	StatementActionTagging  = "Tagging*"
	StatementActionDelete   = "DeleteBucket*"
)

// Access key lengths
const (
	AccessKeyIdLength     = 21
	SecretAccessKeyLength = 42
)

// Access key status values (AWS IAM compatible)
const (
	AccessKeyStatusActive   = "Active"
	AccessKeyStatusInactive = "Inactive"
)
