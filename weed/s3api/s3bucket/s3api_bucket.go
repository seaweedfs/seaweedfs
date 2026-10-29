package s3bucket

import (
	"fmt"
	"net"
	"strings"
	"unicode"
)

// Reserved because it is the default table/collection name of the filer
// store; a bucket of the same name collides with it and wedges the bucket
// (cannot be deleted, breaks fsck) on SQL backends with per-bucket tables.
const reservedBucketName = "filemeta"

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
func VerifyS3BucketName(name string) (err error) {
	if len(name) < 3 || len(name) > 63 {
		return fmt.Errorf("bucket name must be between 3 and 63 characters")
	}
	if name == reservedBucketName {
		return fmt.Errorf("bucket name %q is reserved", name)
	}
	for idx, ch := range name {
		if !(unicode.IsLower(ch) || ch == '.' || ch == '-' || unicode.IsNumber(ch)) {
			return fmt.Errorf("bucket name can only contain lowercase letters, numbers, dots, and hyphens")
		}
		if idx > 0 && (ch == '.' && name[idx-1] == '.') {
			return fmt.Errorf("bucket names must not contain two adjacent periods")
		}
		//TODO buckets with s3 transfer acceleration cannot have . in name
	}
	if name[0] == '.' || name[0] == '-' {
		return fmt.Errorf("bucket name must start with a letter or number")
	}
	if name[len(name)-1] == '.' || name[len(name)-1] == '-' {
		return fmt.Errorf("bucket name must end with a letter or number")
	}
	if strings.HasPrefix(name, "xn--") {
		return fmt.Errorf("bucket name must not start with 'xn--'")
	}
	if strings.HasSuffix(name, "-s3alias") {
		return fmt.Errorf("bucket name must not end with '-s3alias'")
	}
	if net.ParseIP(name) != nil {
		return fmt.Errorf("bucket name must not be formatted as an IP address")
	}
	return nil
}
