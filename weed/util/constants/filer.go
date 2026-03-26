package constants

// Filer error messages — shared between filer (producing) and S3 API (matching).
// These cross a gRPC boundary as strings in CreateEntryResponse.Error, so typed
// errors cannot be used. Keeping the strings in one place prevents mismatches.
const (
	ErrMsgOperationNotPermitted = "operation not permitted"
	ErrMsgBadDigest             = "The Content-Md5 you specified did not match what we received."
	ErrMsgEntryNameTooLong      = "entry name too long"
	// Suffix appended to a path, e.g. "/buckets/b/foo/bar is a file"
	ErrMsgIsAFile = " is a file"
	// Prefix + suffix, e.g. "existing /buckets/b/foo is a directory"
	ErrMsgExistingPrefix  = "existing "
	ErrMsgIsADirectory    = " is a directory"
)
