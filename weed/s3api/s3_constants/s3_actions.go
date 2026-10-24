package s3_constants

const (
	ACTION_READ                          = "Read"
	ACTION_READ_ACP                      = "ReadAcp"
	ACTION_WRITE                         = "Write"
	ACTION_WRITE_ACP                     = "WriteAcp"
	ACTION_ADMIN                         = "Admin"
	ACTION_TAGGING                       = "Tagging"
	ACTION_LIST                          = "List"
	ACTION_DELETE_BUCKET                 = "DeleteBucket"
	ACTION_BYPASS_GOVERNANCE_RETENTION   = "BypassGovernanceRetention"
	ACTION_GET_OBJECT_RETENTION          = "GetObjectRetention"
	ACTION_PUT_OBJECT_RETENTION          = "PutObjectRetention"
	ACTION_GET_OBJECT_LEGAL_HOLD         = "GetObjectLegalHold"
	ACTION_PUT_OBJECT_LEGAL_HOLD         = "PutObjectLegalHold"
	ACTION_GET_BUCKET_OBJECT_LOCK_CONFIG = "GetBucketObjectLockConfiguration"
	ACTION_PUT_BUCKET_OBJECT_LOCK_CONFIG = "PutBucketObjectLockConfiguration"
	ACTION_PUT_BUCKET_POLICY             = "PutBucketPolicy"
	ACTION_DELETE_BUCKET_POLICY          = "DeleteBucketPolicy"
	ACTION_PUT_BUCKET_QUOTA              = "PutBucketQuota"
	ACTION_GET_BUCKET_QUOTA              = "GetBucketQuota"
	// Scoped to their single operation: the multipart listing routes are
	// gated on ACTION_READ for compatibility, but a policy granting one of
	// these names must not widen into a general Read grant.
	ACTION_LIST_MULTIPART_UPLOADS = "ListBucketMultipartUploads"
	ACTION_LIST_PARTS             = "ListMultipartUploadParts"

	SeaweedStorageDestinationHeader = "x-seaweedfs-destination"
	MultipartUploadsFolder          = ".uploads"
	VersionsFolder                  = ".versions"
	SystemFolder                    = ".system"
	FolderMimeType                  = "httpd/unix-directory"
	DirectoryMimeType               = "application/x-directory"
)
