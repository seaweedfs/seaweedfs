package s3_constants

import (
	"strings"
)

var (
	S3ConfigDir              = "/etc/s3"
	CircuitBreakerConfigFile = "circuit_breaker.json"
	BucketACLsConfigFile     = "bucket_acls.json"
	AllowedActions           = []string{ACTION_READ, ACTION_READ_ACP, ACTION_WRITE, ACTION_WRITE_ACP, ACTION_LIST, ACTION_TAGGING, ACTION_ADMIN}
	LimitTypeCount           = "Count"
	LimitTypeBytes           = "MB"
	Separator                = ":"
)

func Concat(elements ...string) string {
	return strings.Join(elements, Separator)
}
