package filer_ui

import (
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

type Breadcrumb struct {
	Name string
	Link string
}

func ToBreadcrumb(fullpath string) (crumbs []Breadcrumb) {
	parts := strings.Split(fullpath, "/")
	if fullpath == "/" {
		parts = []string{""}
	}

	for i := 0; i < len(parts); i++ {
		name := parts[i]
		if name == "" {
			name = "/"
		}
		crumb := Breadcrumb{
			Name: name,
			Link: "/" + util.Join(parts[0:i+1]...),
		}
		if !strings.HasSuffix(crumb.Link, "/") {
			crumb.Link += "/"
		}
		crumbs = append(crumbs, crumb)
	}

	return
}
