package master_ui

import (
	_ "embed"
	"html/template"
	"strings"
)

//go:embed master.html
var masterHtml string

//go:embed masterNewRaft.html
var masterNewRaftHtml string

var templateFunctions = template.FuncMap{
	"url": func(input string) string {

		if !strings.HasPrefix(input, "http://") && !strings.HasPrefix(input, "https://") {
			return "http://" + input
		}

		return input
	},
}

var StatusTpl = template.Must(template.New("status").Funcs(templateFunctions).Parse(masterHtml))

var StatusNewRaftTpl = template.Must(template.New("status").Parse(masterNewRaftHtml))
