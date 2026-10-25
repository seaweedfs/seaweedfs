package s3api

import (
	"errors"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

var tableBucketFileValidator = s3tables.NewTableBucketFileValidator()

func (s3a *S3ApiServer) isTableBucket(bucket string) bool {
	if bucket == "" {
		return false
	}

	if s3a.bucketRegistry != nil {
		metadata, errCode := s3a.bucketRegistry.GetBucketMetadata(bucket)
		if errCode != s3err.ErrNone {
			if errCode != s3err.ErrNoSuchBucket {
				glog.V(1).Infof("bucket lookup failed for %s: %v", bucket, errCode)
			}
			return false
		}
		return metadata.IsTableBucket
	}

	entry, err := s3a.getEntry(s3a.option.BucketsPath, bucket)
	if err == nil && entry != nil {
		return s3tables.IsTableBucketEntry(entry)
	}

	if err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		glog.V(1).Infof("bucket lookup failed for %s: %v", bucket, err)
	}
	return false
}

func (s3a *S3ApiServer) bucketRoot(bucket string) string {
	// Returns the unified buckets root path for all bucket types
	return s3a.option.BucketsPath
}

func (s3a *S3ApiServer) bucketDir(bucket string) string {
	return path.Join(s3a.bucketRoot(bucket), bucket)
}

func (s3a *S3ApiServer) validateTableBucketObjectPath(bucket, object string) error {
	if !s3a.isTableBucket(bucket) {
		return nil
	}
	cleanObject := strings.TrimPrefix(object, "/")
	if cleanObject == "" {
		return &s3tables.IcebergLayoutError{
			Code:    s3tables.ErrCodeInvalidIcebergLayout,
			Message: "object must be under namespace/table/data or metadata",
		}
	}
	fullPath := s3a.bucketDir(bucket)
	if !strings.HasSuffix(fullPath, "/") {
		fullPath += "/"
	}
	fullPath += cleanObject
	if err := tableBucketFileValidator.ValidateTableBucketUpload(fullPath); err != nil {
		return err
	}
	parts := strings.SplitN(cleanObject, "/", 4)
	// A table's marker files sit at namespace/table/<marker>, one level above
	// everything else, so they are three parts rather than four.
	if len(parts) == 3 && s3tables.IsTableMarkerFile(parts[2]) {
		return nil
	}
	if len(parts) < 4 {
		return &s3tables.IcebergLayoutError{
			Code:    s3tables.ErrCodeInvalidIcebergLayout,
			Message: "object must be under namespace/table/data or metadata",
		}
	}
	return nil
}

// volumeVerifyKey matches the object Snowflake writes, reads, lists and
// deletes at the base of an S3-compatible external volume before it creates
// or writes Iceberg tables there (SYSTEM$VERIFY_EXTERNAL_VOLUME, also run
// implicitly): <base>/verify_<epoch milliseconds>_<number>, a 21-byte body.
// A table bucket refuses every key outside the Iceberg layout, so without
// this exception Snowflake cannot use a table bucket as an external volume.
//
// The exception is narrow so that it cannot be used to store data:
//   - only at the bucket root: nothing can be encoded in a prefix, written
//     into a hidden tree or take a namespace or table name;
//   - one shape: 13 digits of epoch milliseconds, then 1-10 digits;
//   - never a directory: a namespace may carry a name of that shape, and the
//     key then names its directory (volumeVerifyKeyIsDirectory);
//   - a PUT must be dated within volumeVerifyWindow of the server's clock,
//     declare a Content-Length of at most volumeVerifyMaxBytes (no chunked or
//     undeclared body), and carry no user metadata, tagging, stored content
//     headers or extraction request (volumeVerifyPutAllowed). Reads and
//     deletes take a key of any date, so an object a check left behind can
//     still be removed.
//
// Only the calls the check makes (PUT, GET, HEAD, DELETE, DeleteObjects)
// accept the key; multipart, copy, rename, ACL, tagging and POST policy keep
// the plain layout rule.
var volumeVerifyKey = regexp.MustCompile(`^verify_([0-9]{13})_[0-9]{1,10}$`)

const (
	volumeVerifyMaxBytes = 1024
	volumeVerifyWindow   = time.Hour
)

// volumeVerifyNow is the clock a PUT's key is checked against.
var volumeVerifyNow = time.Now

// volumeVerifyKeyTime is the time a verification key is dated, and whether
// the object has the key's shape.
func volumeVerifyKeyTime(object string) (time.Time, bool) {
	m := volumeVerifyKey.FindStringSubmatch(strings.TrimPrefix(object, "/"))
	if m == nil {
		return time.Time{}, false
	}
	ms, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.UnixMilli(ms), true
}

func isVolumeVerifyKey(object string) bool {
	_, ok := volumeVerifyKeyTime(object)
	return ok
}

// volumeVerifyLookup reads the entry a verification key names.
var volumeVerifyLookup = func(s3a *S3ApiServer, dir, name string) (*filer_pb.Entry, error) {
	return s3a.getEntry(dir, name)
}

// volumeVerifyKeyIsDirectory reports whether the key names a directory (a
// namespace), or whether that cannot be known.
func (s3a *S3ApiServer) volumeVerifyKeyIsDirectory(bucket, object string) bool {
	entry, err := volumeVerifyLookup(s3a, s3a.bucketDir(bucket), strings.TrimPrefix(object, "/"))
	if err != nil {
		return !errors.Is(err, filer_pb.ErrNotFound)
	}
	return entry != nil && entry.IsDirectory
}

// validateTableBucketObjectPathOrVerify is validateTableBucketObjectPath that
// also accepts the external-volume verification key, and reports it so the
// PUT handler can apply its limits.
func (s3a *S3ApiServer) validateTableBucketObjectPathOrVerify(bucket, object string) (isVerify bool, err error) {
	if !s3a.isTableBucket(bucket) {
		return false, nil
	}
	if isVolumeVerifyKey(object) && !s3a.volumeVerifyKeyIsDirectory(bucket, object) {
		return true, nil
	}
	return false, s3a.validateTableBucketObjectPath(bucket, object)
}

// volumeVerifyPutAllowed reports whether a PUT to the verification key is
// dated now, declares a small body and stores nothing beside it.
func volumeVerifyPutAllowed(r *http.Request, object string) bool {
	at, ok := volumeVerifyKeyTime(object)
	if !ok {
		return false
	}
	if d := volumeVerifyNow().Sub(at); d > volumeVerifyWindow || d < -volumeVerifyWindow {
		return false
	}
	if r.ContentLength < 0 || r.ContentLength > volumeVerifyMaxBytes || len(r.TransferEncoding) > 0 {
		return false
	}
	if isExtractZip(r) {
		return false
	}
	for name, values := range r.Header {
		n := strings.ToLower(name)
		if strings.HasPrefix(n, "x-amz-meta-") || n == "x-amz-tagging" || n == "content-disposition" ||
			n == "response-content-disposition" || n == "content-language" ||
			n == "cache-control" || n == "expires" || n == "x-amz-website-redirect-location" {
			return false
		}
		// aws-chunked only frames the body; any other coding is stored.
		if n == "content-encoding" && storedContentEncoding(values) != "" {
			return false
		}
	}
	return true
}

func (s3a *S3ApiServer) bucketPrefix(bucket string) string {
	return s3a.bucketDir(bucket) + "/"
}

func (s3a *S3ApiServer) bucketExists(bucket string) (bool, error) {
	entry, err := s3a.getBucketEntry(bucket)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return entry != nil, nil
}

func (s3a *S3ApiServer) getBucketEntry(bucket string) (*filer_pb.Entry, error) {
	// Dot-prefixed entries under /buckets are internal (.system) and can never
	// be valid bucket names; don't let them resolve as buckets.
	if strings.HasPrefix(bucket, ".") {
		return nil, filer_pb.ErrNotFound
	}
	return s3a.getEntry(s3a.option.BucketsPath, bucket)
}
