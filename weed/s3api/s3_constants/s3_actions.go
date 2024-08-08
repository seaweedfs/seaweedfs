package s3_constants

const (
	ACTION_READ          = "Read"
	ACTION_READ_ACP      = "ReadAcp"
	ACTION_WRITE         = "Write"
	ACTION_WRITE_ACP     = "WriteAcp"
	ACTION_ADMIN         = "Admin"
	ACTION_TAGGING       = "Tagging"
	ACTION_LIST          = "List"
	ACTION_DELETE_BUCKET = "DeleteBucket"

	SeaweedStorageDestinationHeader = "x-seaweedfs-destination"
	MultipartUploadsFolder          = ".uploads"
	FolderMimeType                  = "httpd/unix-directory"
)
