package volume_server_ui

import (
	_ "embed"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"html/template"
	"strconv"
	"strings"
)

func percentFrom(total uint64, part_of uint64) string {
	return fmt.Sprintf("%.2f", (float64(part_of)/float64(total))*100)
}

func join(data []int64) string {
	var ret []string
	for _, d := range data {
		ret = append(ret, strconv.Itoa(int(d)))
	}
	return strings.Join(ret, ",")
}

var funcMap = template.FuncMap{
	"join":                 join,
	"bytesToHumanReadable": util.BytesToHumanReadable,
	"percentFrom":          percentFrom,
	"isNotEmpty":           util.IsNotEmpty,
}

//go:embed volume.html
var volumeHtml string

var StatusTpl = template.Must(template.New("status").Funcs(funcMap).Parse(volumeHtml))
