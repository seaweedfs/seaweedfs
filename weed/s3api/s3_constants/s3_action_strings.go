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
	S3_ACTION_GET_OBJECT_ATTRIBUTES = "s3:GetObjectAttributes"

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
	S3_ACTION_CREATE_MULTIPART       = "s3:CreateMultipartUpload"
	S3_ACTION_UPLOAD_PART            = "s3:UploadPart"
	S3_ACTION_COMPLETE_MULTIPART     = "s3:CompleteMultipartUpload"
	S3_ACTION_ABORT_MULTIPART        = "s3:AbortMultipartUpload"
	S3_ACTION_UPLOAD_PART_COPY       = "s3:UploadPartCopy"
	S3_ACTION_LIST_PARTS             = "s3:ListMultipartUploadParts"
	S3_ACTION_LIST_MULTIPART_UPLOADS = "s3:ListBucketMultipartUploads"

	// Bucket operations
	S3_ACTION_CREATE_BUCKET        = "s3:CreateBucket"
	S3_ACTION_DELETE_BUCKET        = "s3:DeleteBucket"
	S3_ACTION_LIST_BUCKET          = "s3:ListBucket"
	S3_ACTION_LIST_BUCKET_VERSIONS = "s3:ListBucketVersions"

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

	// Bucket encryption operations
	// Note: DELETE bucket encryption uses s3:PutEncryptionConfiguration, matching AWS
	S3_ACTION_GET_BUCKET_ENCRYPTION = "s3:GetEncryptionConfiguration"
	S3_ACTION_PUT_BUCKET_ENCRYPTION = "s3:PutEncryptionConfiguration"

	// Bucket request payment operations
	S3_ACTION_GET_BUCKET_REQUEST_PAYMENT = "s3:GetBucketRequestPayment"
	S3_ACTION_PUT_BUCKET_REQUEST_PAYMENT = "s3:PutBucketRequestPayment"

	// Bucket public access block operations
	// Note: DELETE uses s3:PutBucketPublicAccessBlock, matching AWS
	S3_ACTION_GET_BUCKET_PUBLIC_ACCESS_BLOCK = "s3:GetBucketPublicAccessBlock"
	S3_ACTION_PUT_BUCKET_PUBLIC_ACCESS_BLOCK = "s3:PutBucketPublicAccessBlock"

	// Bucket ownership controls operations
	// Note: DELETE uses s3:PutBucketOwnershipControls, matching AWS
	S3_ACTION_GET_BUCKET_OWNERSHIP_CONTROLS = "s3:GetBucketOwnershipControls"
	S3_ACTION_PUT_BUCKET_OWNERSHIP_CONTROLS = "s3:PutBucketOwnershipControls"

	// Wildcard for all S3 actions
	S3_ACTION_ALL = "s3:*"
)

// S3 Tables action strings for policy evaluation.
// Source of truth for the operation names: the dispatch switch in
// weed/s3api/s3tables/handler.go. Keep this list in sync with that switch
// when operations are added, renamed, or removed.
const (
	// Table bucket operations
	S3TABLES_ACTION_CREATE_TABLE_BUCKET = "s3tables:CreateTableBucket"
	S3TABLES_ACTION_GET_TABLE_BUCKET    = "s3tables:GetTableBucket"
	S3TABLES_ACTION_LIST_TABLE_BUCKETS  = "s3tables:ListTableBuckets"
	S3TABLES_ACTION_DELETE_TABLE_BUCKET = "s3tables:DeleteTableBucket"

	// Table bucket policy operations
	S3TABLES_ACTION_PUT_TABLE_BUCKET_POLICY    = "s3tables:PutTableBucketPolicy"
	S3TABLES_ACTION_GET_TABLE_BUCKET_POLICY    = "s3tables:GetTableBucketPolicy"
	S3TABLES_ACTION_DELETE_TABLE_BUCKET_POLICY = "s3tables:DeleteTableBucketPolicy"

	// Namespace operations
	S3TABLES_ACTION_CREATE_NAMESPACE = "s3tables:CreateNamespace"
	S3TABLES_ACTION_GET_NAMESPACE    = "s3tables:GetNamespace"
	S3TABLES_ACTION_UPDATE_NAMESPACE = "s3tables:UpdateNamespace"
	S3TABLES_ACTION_LIST_NAMESPACES  = "s3tables:ListNamespaces"
	S3TABLES_ACTION_DELETE_NAMESPACE = "s3tables:DeleteNamespace"

	// Table operations
	S3TABLES_ACTION_CREATE_TABLE   = "s3tables:CreateTable"
	S3TABLES_ACTION_REGISTER_TABLE = "s3tables:RegisterTable"
	S3TABLES_ACTION_GET_TABLE      = "s3tables:GetTable"
	S3TABLES_ACTION_LIST_TABLES    = "s3tables:ListTables"
	S3TABLES_ACTION_UPDATE_TABLE   = "s3tables:UpdateTable"
	S3TABLES_ACTION_DELETE_TABLE   = "s3tables:DeleteTable"
	S3TABLES_ACTION_RENAME_TABLE   = "s3tables:RenameTable"

	// View operations
	S3TABLES_ACTION_CREATE_VIEW = "s3tables:CreateView"
	S3TABLES_ACTION_GET_VIEW    = "s3tables:GetView"
	S3TABLES_ACTION_LIST_VIEWS  = "s3tables:ListViews"
	S3TABLES_ACTION_UPDATE_VIEW = "s3tables:UpdateView"
	S3TABLES_ACTION_DELETE_VIEW = "s3tables:DeleteView"
	S3TABLES_ACTION_RENAME_VIEW = "s3tables:RenameView"

	// Table policy operations
	S3TABLES_ACTION_PUT_TABLE_POLICY    = "s3tables:PutTablePolicy"
	S3TABLES_ACTION_GET_TABLE_POLICY    = "s3tables:GetTablePolicy"
	S3TABLES_ACTION_DELETE_TABLE_POLICY = "s3tables:DeleteTablePolicy"

	// Maintenance configuration operations
	S3TABLES_ACTION_PUT_TABLE_BUCKET_MAINTENANCE_CONFIGURATION = "s3tables:PutTableBucketMaintenanceConfiguration"
	S3TABLES_ACTION_GET_TABLE_BUCKET_MAINTENANCE_CONFIGURATION = "s3tables:GetTableBucketMaintenanceConfiguration"
	S3TABLES_ACTION_PUT_TABLE_MAINTENANCE_CONFIGURATION        = "s3tables:PutTableMaintenanceConfiguration"
	S3TABLES_ACTION_GET_TABLE_MAINTENANCE_CONFIGURATION        = "s3tables:GetTableMaintenanceConfiguration"
	S3TABLES_ACTION_GET_TABLE_MAINTENANCE_JOB_STATUS           = "s3tables:GetTableMaintenanceJobStatus"

	// Tagging operations
	S3TABLES_ACTION_TAG_RESOURCE           = "s3tables:TagResource"
	S3TABLES_ACTION_LIST_TAGS_FOR_RESOURCE = "s3tables:ListTagsForResource"
	S3TABLES_ACTION_UNTAG_RESOURCE         = "s3tables:UntagResource"

	// Wildcard for all S3 Tables actions
	S3TABLES_ACTION_ALL = "s3tables:*"
)
