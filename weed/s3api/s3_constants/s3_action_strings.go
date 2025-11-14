package s3_constants

// S3 action strings for bucket policy evaluation
// These match the official AWS S3 action format used in IAM and bucket policies
const (
	// Object operations
	S3_ACTION_GET_OBJECT            = "s3:GetObject"
	S3_ACTION_PUT_OBJECT            = "s3:PutObject"
	S3_ACTION_DELETE_OBJECT         = "s3:DeleteObject"
	S3_ACTION_DELETE_OBJECT_VERSION = "s3:DeleteObjectVersion"
	S3_ACTION_GET_OBJECT_VERSION    = "s3:GetObjectVersion"

	// Object ACL operations
	S3_ACTION_GET_OBJECT_ACL = "s3:GetObjectAcl"
	S3_ACTION_PUT_OBJECT_ACL = "s3:PutObjectAcl"

	// Object tagging operations
	S3_ACTION_GET_OBJECT_TAGGING    = "s3:GetObjectTagging"
	S3_ACTION_PUT_OBJECT_TAGGING    = "s3:PutObjectTagging"
	S3_ACTION_DELETE_OBJECT_TAGGING = "s3:DeleteObjectTagging"

	// Object retention and legal hold
	S3_ACTION_GET_OBJECT_RETENTION  = "s3:GetObjectRetention"
	S3_ACTION_PUT_OBJECT_RETENTION  = "s3:PutObjectRetention"
	S3_ACTION_GET_OBJECT_LEGAL_HOLD = "s3:GetObjectLegalHold"
	S3_ACTION_PUT_OBJECT_LEGAL_HOLD = "s3:PutObjectLegalHold"
	S3_ACTION_BYPASS_GOVERNANCE     = "s3:BypassGovernanceRetention"

	// Multipart upload operations
	S3_ACTION_CREATE_MULTIPART   = "s3:CreateMultipartUpload"
	S3_ACTION_UPLOAD_PART        = "s3:UploadPart"
	S3_ACTION_COMPLETE_MULTIPART = "s3:CompleteMultipartUpload"
	S3_ACTION_ABORT_MULTIPART    = "s3:AbortMultipartUpload"
	S3_ACTION_LIST_PARTS         = "s3:ListMultipartUploadParts"

	// Bucket operations
	S3_ACTION_CREATE_BUCKET          = "s3:CreateBucket"
	S3_ACTION_DELETE_BUCKET          = "s3:DeleteBucket"
	S3_ACTION_LIST_BUCKET            = "s3:ListBucket"
	S3_ACTION_LIST_BUCKET_VERSIONS   = "s3:ListBucketVersions"
	S3_ACTION_LIST_MULTIPART_UPLOADS = "s3:ListBucketMultipartUploads"

	// Bucket ACL operations
	S3_ACTION_GET_BUCKET_ACL = "s3:GetBucketAcl"
	S3_ACTION_PUT_BUCKET_ACL = "s3:PutBucketAcl"

	// Bucket policy operations
	S3_ACTION_GET_BUCKET_POLICY    = "s3:GetBucketPolicy"
	S3_ACTION_PUT_BUCKET_POLICY    = "s3:PutBucketPolicy"
	S3_ACTION_DELETE_BUCKET_POLICY = "s3:DeleteBucketPolicy"

	// Bucket tagging operations
	S3_ACTION_GET_BUCKET_TAGGING    = "s3:GetBucketTagging"
	S3_ACTION_PUT_BUCKET_TAGGING    = "s3:PutBucketTagging"
	S3_ACTION_DELETE_BUCKET_TAGGING = "s3:DeleteBucketTagging"

	// Bucket CORS operations
	S3_ACTION_GET_BUCKET_CORS    = "s3:GetBucketCors"
	S3_ACTION_PUT_BUCKET_CORS    = "s3:PutBucketCors"
	S3_ACTION_DELETE_BUCKET_CORS = "s3:DeleteBucketCors"

	// Bucket lifecycle operations
	// Note: Both PUT and DELETE lifecycle operations use s3:PutLifecycleConfiguration
	S3_ACTION_GET_BUCKET_LIFECYCLE = "s3:GetLifecycleConfiguration"
	S3_ACTION_PUT_BUCKET_LIFECYCLE = "s3:PutLifecycleConfiguration"

	// Bucket versioning operations
	S3_ACTION_GET_BUCKET_VERSIONING = "s3:GetBucketVersioning"
	S3_ACTION_PUT_BUCKET_VERSIONING = "s3:PutBucketVersioning"

	// Bucket location
	S3_ACTION_GET_BUCKET_LOCATION = "s3:GetBucketLocation"

	// Bucket notification
	S3_ACTION_GET_BUCKET_NOTIFICATION = "s3:GetBucketNotification"
	S3_ACTION_PUT_BUCKET_NOTIFICATION = "s3:PutBucketNotification"

	// Bucket object lock operations
	S3_ACTION_GET_BUCKET_OBJECT_LOCK = "s3:GetBucketObjectLockConfiguration"
	S3_ACTION_PUT_BUCKET_OBJECT_LOCK = "s3:PutBucketObjectLockConfiguration"

	// Wildcard for all S3 actions
	S3_ACTION_ALL = "s3:*"
)
