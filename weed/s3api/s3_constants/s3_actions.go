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

	SeaweedStorageDestinationHeader = "x-seaweedfs-destination"
	MultipartUploadsFolder          = ".uploads"
	FolderMimeType                  = "httpd/unix-directory"
)
