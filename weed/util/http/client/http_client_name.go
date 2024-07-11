package client

import "strings"

type ClientName int

//go:generate stringer -type=ClientName -output=http_client_name_string.go
const (
	Client ClientName = iota
)

func (name *ClientName) LowerCaseString() string {
	return strings.ToLower(name.String())
}
