package services

import "strings"

type Name int

//go:generate stringer -type=Name -output=name_string.go
const (
    Master Name = iota
    Volume
    Filer
)

func GetAll() []Name {
    return []Name{Master, Volume, Filer}
}

func (name *Name) LowerCaseString() string {
    return strings.ToLower(name.String())
}