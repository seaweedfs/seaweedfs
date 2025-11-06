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

	// Granular multipart upload actions for fine-grained IAM policies
	ACTION_CREATE_MULTIPART_UPLOAD = "s3:CreateMultipartUpload"
	ACTION_UPLOAD_PART             = "s3:UploadPart"
	ACTION_COMPLETE_MULTIPART      = "s3:CompleteMultipartUpload"
	ACTION_ABORT_MULTIPART         = "s3:AbortMultipartUpload"
	ACTION_LIST_MULTIPART_UPLOADS  = "s3:ListMultipartUploads"
	ACTION_LIST_PARTS              = "s3:ListParts"

	SeaweedStorageDestinationHeader = "x-seaweedfs-destination"
	MultipartUploadsFolder          = ".uploads"
	VersionsFolder                  = ".versions"
	FolderMimeType                  = "httpd/unix-directory"
)
