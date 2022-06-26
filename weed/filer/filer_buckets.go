package filer

import (
	"strings"
)

func (f *Filer) isBucket(entry *Entry) bool {
	if !entry.IsDirectory() {
		return false
	}
	parent, dirName := entry.FullPath.DirAndName()
	if parent != f.DirBucketsPath {
		return false
	}
	if strings.HasPrefix(dirName, ".") {
		return false
	}

	return true

}
