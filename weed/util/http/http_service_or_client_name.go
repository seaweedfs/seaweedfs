package http

import "strings"

type ServiceOrClientName int

//go:generate stringer -type=ServiceOrClientName -output=http_service_or_client_name_string.go
const (
	Master ServiceOrClientName = iota
	Volume
	Filer
	Global
)

func (name *ServiceOrClientName) LowerCaseString() string {
	return strings.ToLower(name.String())
}
