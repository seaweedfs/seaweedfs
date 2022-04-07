package master_ui

import (
	_ "embed"
	"html/template"
)

//go:embed master.html
var masterHtml string

//go:embed masterNewRaft.html
var masterNewRaftHtml string

var StatusTpl = template.Must(template.New("status").Parse(masterHtml))
var StatusNewRaftTpl = template.Must(template.New("status").Parse(masterNewRaftHtml))
