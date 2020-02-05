package filer2

import (
	"path/filepath"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/util"
)

type FullPath string

func NewFullPath(dir, name string) FullPath {
	return FullPath(dir).Child(name)
}

func (fp FullPath) DirAndName() (string, string) {
	dir, name := filepath.Split(string(fp))
	if dir == "/" {
		return dir, name
	}
	if len(dir) < 1 {
		return "/", ""
	}
	return dir[:len(dir)-1], name
}

func (fp FullPath) Name() string {
	_, name := filepath.Split(string(fp))
	return name
}

func (fp FullPath) Child(name string) FullPath {
	dir := string(fp)
	if strings.HasSuffix(dir, "/") {
		return FullPath(dir + name)
	}
	return FullPath(dir + "/" + name)
}

func (fp FullPath) AsInode() uint64 {
	return uint64(util.HashStringToLong(string(fp)))
}
