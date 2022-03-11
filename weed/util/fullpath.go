package util

import (
	"os"
	"path/filepath"
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
	dir, name := filepath.Split(clearName(string(fp)))
	if dir == "/" {
		return dir, name
	}
	if len(dir) < 1 {
		return "/", ""
	}
	return dir[:len(dir)-1], name
}

func (fp FullPath) ToDir() FullPath {
	return fp.Child("./")
}

func (fp FullPath) ToFile() FullPath {
	return fp.Child(".")
}

func (fp FullPath) IsFile() bool {
	return !fp.IsDir()
}

func (fp FullPath) IsDir() bool {
	return strings.HasSuffix(string(fp), "/")
}

func (fp FullPath) Name() string {
	_, name := filepath.Split(string(fp))
	name = strings.ToValidUTF8(name, "?")
	return name
}

func (fp FullPath) Child(name string) FullPath {
	return NewFullPath(string(fp), name)
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
	newName := clearName(filepath.ToSlash(filepath.Join(names...)))
	if newName != "/" && strings.HasSuffix(filepath.ToSlash(strings.Join(names, "")), "/") {
		newName += "/"
	}
	return newName
}

func clearName(name string) string {
	name = strings.ToValidUTF8(name, "?")
	slashed := strings.HasSuffix(name, "/")
	name = filepath.Clean(name)
	if name != "/" && slashed {
		name += "/"
	}
	return name
}
