package http

import "strings"

type ClientName int

//go:generate stringer -type=ClientName -output=http_client_name_string.go
const (
	Global_client ClientName = iota
)

func (name *ClientName) LowerCaseString() string {
	return strings.ToLower(name.String())
}
