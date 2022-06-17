package s3_config

import (
	"github.com/chrislusf/seaweedfs/weed/s3api/s3_constants"
	"strings"
)

var (
	CircuitBreakerConfigDir  = "/etc/s3"
	CircuitBreakerConfigFile = "circuit_breaker.json"
	AllowedActions           = []string{s3_constants.ACTION_READ, s3_constants.ACTION_WRITE, s3_constants.ACTION_LIST, s3_constants.ACTION_TAGGING, s3_constants.ACTION_ADMIN}
	LimitTypeCount           = "count"
	LimitTypeBytes           = "bytes"
	Separator                = ":"
)

func Concat(elements ...string) string {
	return strings.Join(elements, Separator)
}
