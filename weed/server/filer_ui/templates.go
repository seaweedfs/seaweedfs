package filer_ui

import (
	_ "embed"
	"github.com/dustin/go-humanize"
	"html/template"
	"net/url"
	"strings"
)

func printpath(parts ...string) string {
	var escapedParts []string
	for _, p := range parts {
		if len(p) == 1 {
			escapedParts = append(escapedParts, p)
		} else {
			escapedParts = append(escapedParts, url.PathEscape(p))
		}
	}
	return strings.Join(escapedParts, "")
}

var funcMap = template.FuncMap{
	"humanizeBytes": humanize.Bytes,
	"printpath":     printpath,
}

//go:embed filer.html
var filerHtml string

var StatusTpl = template.Must(template.New("status").Funcs(funcMap).Parse(filerHtml))
