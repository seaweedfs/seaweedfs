package util

import (
	"os"
	"path/filepath"
	"strings"
)

type FullPath string

func NewFullPath(dir, name string) FullPath {
	return FullPath(dir).Child(name)
}

func (fp FullPath) DirAndName() (string, string) {
	dir, name := filepath.Split(string(fp))
	name = strings.ToValidUTF8(name, "?")
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
	name = strings.ToValidUTF8(name, "?")
	return name
}

func (fp FullPath) Child(name string) FullPath {
	dir := string(fp)
	noPrefix := name
	if strings.HasPrefix(name, "/") {
		noPrefix = name[1:]
	}
	if strings.HasSuffix(dir, "/") {
		return FullPath(dir + noPrefix)
	}
	return FullPath(dir + "/" + noPrefix)
}

// AsInode an in-memory only inode representation
func (fp FullPath) AsInode(fileMode os.FileMode) uint64 {
	inode := uint64(HashStringToLong(string(fp)))
	inode = inode - inode%16
	if fileMode == 0 {
	} else if fileMode&os.ModeDir > 0 {
		inode += 1
	} else if fileMode&os.ModeSymlink > 0 {
		inode += 2
	} else if fileMode&os.ModeDevice > 0 {
		if fileMode&os.ModeCharDevice > 0 {
			inode += 6
		} else {
			inode += 3
		}
	} else if fileMode&os.ModeNamedPipe > 0 {
		inode += 4
	} else if fileMode&os.ModeSocket > 0 {
		inode += 5
	} else if fileMode&os.ModeCharDevice > 0 {
		inode += 6
	} else if fileMode&os.ModeIrregular > 0 {
		inode += 7
	}
	return inode
}

// split, but skipping the root
func (fp FullPath) Split() []string {
	if fp == "" || fp == "/" {
		return []string{}
	}
	return strings.Split(string(fp)[1:], "/")
}

func Join(names ...string) string {
	return filepath.ToSlash(filepath.Join(names...))
}

func JoinPath(names ...string) FullPath {
	return FullPath(Join(names...))
}
