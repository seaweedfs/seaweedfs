package weed_server

import (
	"context"
	"io/fs"
	"os"
	"path"

	"golang.org/x/net/webdav"
)

type wrappedFs struct {
	subFolder string
	webdav.FileSystem
}

// confine joins a client-supplied name under subFolder. The name is cleaned as
// a rooted path first so `..` segments cannot climb above subFolder; cleaning
// after the concat (as the underlying FileSystem does) would resolve them across
// the confinement boundary.
func (w wrappedFs) confine(name string) string {
	return w.subFolder + path.Clean("/"+name)
}

// NewWrappedFs returns a webdav.FileSystem identical to fs, except it
// provides access to a sub-folder of fs that is denominated by subFolder.
// It transparently handles renaming paths and filenames so that the outer part of the wrapped filesystem
// does not leak out.
func NewWrappedFs(fs webdav.FileSystem, subFolder string) webdav.FileSystem {
	return wrappedFs{
		subFolder:  subFolder,
		FileSystem: fs,
	}
}

func (w wrappedFs) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	name = w.confine(name)
	return w.FileSystem.Mkdir(ctx, name, perm)
}

func (w wrappedFs) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	name = w.confine(name)
	file, err := w.FileSystem.OpenFile(ctx, name, flag, perm)
	return wrappedFile{File: file}, err
}

func (w wrappedFs) RemoveAll(ctx context.Context, name string) error {
	name = w.confine(name)
	return w.FileSystem.RemoveAll(ctx, name)
}

func (w wrappedFs) Rename(ctx context.Context, oldName, newName string) error {
	oldName = w.confine(oldName)
	newName = w.confine(newName)
	return w.FileSystem.Rename(ctx, oldName, newName)
}

func (w wrappedFs) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	name = w.confine(name)
	info, err := w.FileSystem.Stat(ctx, name)
	return wrappedFileInfo{FileInfo: info}, err
}

type wrappedFile struct {
	webdav.File
}

func (w wrappedFile) Readdir(count int) ([]fs.FileInfo, error) {
	infos, err := w.File.Readdir(count)
	for i, info := range infos {
		infos[i] = wrappedFileInfo{FileInfo: info}
	}
	return infos, err
}

func (w wrappedFile) Stat() (fs.FileInfo, error) {
	info, err := w.File.Stat()
	return wrappedFileInfo{FileInfo: info}, err
}

type wrappedFileInfo struct {
	fs.FileInfo
}

func (w wrappedFileInfo) ETag(ctx context.Context) (string, error) {
	etag, _ := w.FileInfo.(webdav.ETager).ETag(ctx)
	if len(etag) == 0 {
		return etag, webdav.ErrNotImplemented
	}
	return etag, nil
}
