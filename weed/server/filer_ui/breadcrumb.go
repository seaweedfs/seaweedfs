package filer_ui

import (
	"strings"

	"github.com/chrislusf/seaweedfs/weed/util"
)

type Breadcrumb struct {
	Name string
	Link string
}

func ToBreadcrumb(fullpath string) (crumbs []Breadcrumb) {
	parts := strings.Split(fullpath, "/")

	for i := 0; i < len(parts); i++ {
		crumb := Breadcrumb{
			Name: parts[i] + " /",
			Link: "/" + util.Join(parts[0:i+1]...),
		}
		if !strings.HasSuffix(crumb.Link, "/") {
			crumb.Link += "/"
		}
		crumbs = append(crumbs, crumb)
	}

	return
}
