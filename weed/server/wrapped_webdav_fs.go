package weed_server

import (
	"context"
	"golang.org/x/net/webdav"
	"io/fs"
	"os"
	"strings"
)

type wrappedFs struct {
	subFolder string
	webdav.FileSystem
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
	name = w.subFolder + name
	return w.FileSystem.Mkdir(ctx, name, perm)
}

func (w wrappedFs) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	name = w.subFolder + name
	file, err := w.FileSystem.OpenFile(ctx, name, flag, perm)
	file = wrappedFile{
		File:      file,
		subFolder: &w.subFolder,
	}

	return file, err
}

func (w wrappedFs) RemoveAll(ctx context.Context, name string) error {
	name = w.subFolder + name
	return w.FileSystem.RemoveAll(ctx, name)
}

func (w wrappedFs) Rename(ctx context.Context, oldName, newName string) error {
	oldName = w.subFolder + oldName
	newName = w.subFolder + newName
	return w.FileSystem.Rename(ctx, oldName, newName)
}

func (w wrappedFs) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	name = w.subFolder + name
	info, err := w.FileSystem.Stat(ctx, name)
	info = wrappedFileInfo{
		subFolder: &w.subFolder,
		FileInfo:  info,
	}
	return info, err
}

type wrappedFile struct {
	webdav.File
	subFolder *string
}

func (w wrappedFile) Readdir(count int) ([]fs.FileInfo, error) {
	infos, err := w.File.Readdir(count)
	for i, info := range infos {
		infos[i] = wrappedFileInfo{
			subFolder: w.subFolder,
			FileInfo:  info,
		}
	}
	return infos, err
}

func (w wrappedFile) Stat() (fs.FileInfo, error) {
	info, err := w.File.Stat()
	info = wrappedFileInfo{
		subFolder: w.subFolder,
		FileInfo:  info,
	}
	return info, err
}

type wrappedFileInfo struct {
	subFolder *string
	fs.FileInfo
}

func (w wrappedFileInfo) Name() string {
	name := w.FileInfo.Name()
	return strings.TrimPrefix(name, *w.subFolder)
}

func (w wrappedFileInfo) ETag(ctx context.Context) (string, error) {
	etag, _ := w.FileInfo.(webdav.ETager).ETag(ctx)
	if len(etag) == 0 {
		return etag, webdav.ErrNotImplemented
	}
	return etag, nil
}
