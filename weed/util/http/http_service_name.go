package http

import "strings"

type ServiceName int

//go:generate stringer -type=ServiceName -output=http_service_name_string.go
const (
	Master ServiceName = iota
	Volume
	Filer
	Unknown
)

func (name *ServiceName) LowerCaseString() string {
	return strings.ToLower(name.String())
}
