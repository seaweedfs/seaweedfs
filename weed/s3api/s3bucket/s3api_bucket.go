package s3bucket

import (
	"fmt"
	"net"
	"strings"
	"unicode"
)

// https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
func VerifyS3BucketName(name string) (err error) {
	if len(name) < 3 || len(name) > 63 {
		return fmt.Errorf("bucket name must between [3, 63] characters")
	}
	for idx, ch := range name {
		if !(unicode.IsLower(ch) || ch == '.' || ch == '-' || unicode.IsNumber(ch)) {
			return fmt.Errorf("bucket name can only contain lower case characters, numbers, dots, and hyphens")
		}
		if idx > 0 && (ch == '.' && name[idx-1] == '.') {
			return fmt.Errorf("bucket names must not contain two adjacent periods")
		}
		//TODO buckets with s3 transfer acceleration cannot have . in name
	}
	if name[0] == '.' || name[0] == '-' {
		return fmt.Errorf("name must start with number or lower case character")
	}
	if name[len(name)-1] == '.' || name[len(name)-1] == '-' {
		return fmt.Errorf("name must end with number or lower case character")
	}
	if strings.HasPrefix(name, "xn--") {
		return fmt.Errorf("prefix xn-- is reserved and not allowed in bucket prefix")
	}
	if strings.HasSuffix(name, "-s3alias") {
		return fmt.Errorf("suffix -s3alias is reserved and not allowed in bucket suffix")
	}
	if net.ParseIP(name) != nil {
		return fmt.Errorf("bucket name cannot be ip addresses")
	}
	return nil
}
