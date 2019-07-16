package master_ui

import (
	"path/filepath"
	"strings"
)

type Breadcrumb struct {
	Name string
	Link string
}

func ToBreadcrumb(fullpath string) (crumbs []Breadcrumb) {
	parts := strings.Split(fullpath, "/")

	for i := 0; i < len(parts); i++ {
		crumbs = append(crumbs, Breadcrumb{
			Name: parts[i] + "/",
			Link: "/" + filepath.ToSlash(filepath.Join(parts[0:i+1]...)),
		})
	}

	return
}
