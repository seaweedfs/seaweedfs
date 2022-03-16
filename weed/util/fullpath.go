package util

import (
	"path"
	"strings"
)

// FullPath will keep the literal passed in.
// file literal: /path/to/file
// directory literal: /path/
// change to file literal: fp.Child(".") or NewFullPath(name, ".")
// change to directory literal: fp.Child("./") or NewFullPath(name, "./")
type FullPath string

func NewFullPath(name ...string) FullPath {
	return FullPath(Join(name...))
}

func (fp FullPath) DirAndName() (string, string) {
	dir, name := path.Split(clearName(string(fp)))
	if dir == "/" {
		return dir, name
	}
	if len(dir) < 1 {
		return "/", ""
	}
	return dir[:len(dir)-1], name
}

func (fp FullPath) Name() string {
	_, name := fp.DirAndName()
	return name
}

func (fp FullPath) Child(name string) FullPath {
	return NewFullPath(string(fp), name)
}

// AsInode an in-memory only inode representation
func (fp FullPath) AsInode(unixTime int64) uint64 {
	inode := uint64(HashStringToLong(string(fp)))
	inode = inode + uint64(unixTime)*37
	return inode
}

func Join(names ...string) string {
	return clearName(path.Join(names...))
}

func clearName(name string) string {
	name = strings.ToValidUTF8(name, "?")
	name = strings.ReplaceAll(name, "\\", "/")
	name = path.Clean(name)
	if name == "." {
		name = "/"
	}
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}
	return name
}
