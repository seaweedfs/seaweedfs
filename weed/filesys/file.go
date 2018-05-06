package filesys

import (
	"context"
	"fmt"

	"bazil.org/fuse"
	"github.com/chrislusf/seaweedfs/weed/filer"
)

type File struct {
	FileId filer.FileId
	Name   string
	wfs    *WFS
}

func (file *File) Attr(context context.Context, attr *fuse.Attr) error {
	attr.Mode = 0444
	ret, err := filer.GetFileSize(file.wfs.filer, string(file.FileId))
	if err == nil {
		attr.Size = ret.Size
	} else {
		fmt.Printf("Get file %s attr [ERROR] %s\n", file.Name, err)
	}
	return err
}

func (file *File) ReadAll(ctx context.Context) ([]byte, error) {
	ret, err := filer.GetFileContent(file.wfs.filer, string(file.FileId))
	if err == nil {
		return ret.Content, nil
	}
	fmt.Printf("Get file %s content [ERROR] %s\n", file.Name, err)
	return nil, err
}
