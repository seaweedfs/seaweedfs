package master_ui

import (
	_ "embed"
	"html/template"
)

//go:embed master.html
var masterHtml string

var StatusTpl = template.Must(template.New("status").Parse(masterHtml))
