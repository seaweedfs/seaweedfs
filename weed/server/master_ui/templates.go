package master_ui

import (
	_ "embed"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"html/template"
)

//go:embed master.html
var masterHtml string

//go:embed masterNewRaft.html
var masterNewRaftHtml string

var funcMap = template.FuncMap{
	"bytesToHumanReadable": util.BytesToHumanReadable,
	"isNotEmpty":           util.IsNotEmpty,
}
var StatusTpl = template.Must(template.New("status").Funcs(funcMap).Parse(masterHtml))
var StatusNewRaftTpl = template.Must(template.New("status").Funcs(funcMap).Parse(masterNewRaftHtml))
