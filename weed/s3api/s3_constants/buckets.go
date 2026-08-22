package s3_constants

const (
	// DefaultBucketsPath is the default path for S3 buckets in the filer
	DefaultBucketsPath = "/buckets"

	// ObjectWriteRouteKeyPrefix namespaces a full path into the ring key used to
	// resolve and forward writes to it. Every writer of an object or bucket
	// entry hashes this same key, so they serialize on one filer's path lock.
	ObjectWriteRouteKeyPrefix = "s3.object.write:"
)
