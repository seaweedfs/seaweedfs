package config

import "strings"

var (
	CircuitBreakerConfigDir  = "/etc/s3"
	CircuitBreakerConfigFile = "circuit_breaker.json"
	AllowedActions           = []string{"Read", "Write", "List", "Tagging", "Admin"}
	LimitTypeCount           = "count"
	LimitTypeBytes           = "bytes"
	Separator                = ":"
)

func Concat(elements ...string) string {
	return strings.Join(elements, Separator)
}
