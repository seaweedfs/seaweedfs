package volume_server_ui

import (
	_ "embed"
	"fmt"
	"html/template"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

func percentFrom(total uint64, part_of uint64) string {
	return fmt.Sprintf("%.2f", (float64(part_of)/float64(total))*100)
}

func bytesToHumanReadable(b interface{}) string {
	switch v := b.(type) {
	case uint64:
		return util.BytesToHumanReadable(v)
	case int64:
		if v < 0 {
			return fmt.Sprintf("%d B", v)
		}
		return util.BytesToHumanReadable(uint64(v))
	case int:
		if v < 0 {
			return fmt.Sprintf("%d B", v)
		}
		return util.BytesToHumanReadable(uint64(v))
	case uint32:
		return util.BytesToHumanReadable(uint64(v))
	case int32:
		if v < 0 {
			return fmt.Sprintf("%d B", v)
		}
		return util.BytesToHumanReadable(uint64(v))
	case uint:
		return util.BytesToHumanReadable(uint64(v))
	default:
		return fmt.Sprintf("%v", b)
	}
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
	"bytesToHumanReadable": bytesToHumanReadable,
	"percentFrom":          percentFrom,
	"isNotEmpty":           util.IsNotEmpty,
}

//go:embed volume.html
var volumeHtml string

var StatusTpl = template.Must(template.New("status").Funcs(funcMap).Parse(volumeHtml))
