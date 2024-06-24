package services

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
