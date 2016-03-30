// +build linux darwin

package weedcmd

import (
	"fmt"
	"runtime"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/util"
	"golang.org/x/net/context"
)

func runMount(cmd *Command, args []string) bool {
	fmt.Printf("This is SeaweedFS version %s %s %s\n", util.VERSION, runtime.GOOS, runtime.GOARCH)
	if *mountOptions.dir == "" {
		fmt.Printf("Please specify the mount directory via \"-dir\"")
		return false
	}

	c, err := fuse.Mount(*mountOptions.dir)
	if err != nil {
		glog.Fatal(err)
		return false
	}

	OnInterrupt(func() {
		fuse.Unmount(*mountOptions.dir)
		c.Close()
	})

	err = fs.Serve(c, WFS{})
	if err != nil {
		fuse.Unmount(*mountOptions.dir)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		glog.Fatal(err)
	}

	return true
}

type File struct {
	FileId filer.FileId
	Name   string
}

func (File) Attr(context context.Context, attr *fuse.Attr) error {
	return nil
}
func (File) ReadAll(ctx context.Context) ([]byte, error) {
	return []byte("hello, world\n"), nil
}

type Dir struct {
	Path string
	Id   uint64
}

func (dir Dir) Attr(context context.Context, attr *fuse.Attr) error {
	return nil
}

func (dir Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	files_result, e := filer.ListFiles(*mountOptions.filer, dir.Path, name)
	if e != nil {
		return nil, fuse.ENOENT
	}
	if len(files_result.Files) > 0 {
		return File{files_result.Files[0].Id, files_result.Files[0].Name}, nil
	}
	return nil, fmt.Errorf("File Not Found for %s", name)
}

type WFS struct{}

func (WFS) Root() (fs.Node, error) {
	return Dir{}, nil
}

func (dir *Dir) ReadDir(ctx context.Context) ([]fuse.Dirent, error) {
	var ret []fuse.Dirent
	if dirs, e := filer.ListDirectories(*mountOptions.filer, dir.Path); e == nil {
		for _, d := range dirs.Directories {
			dirId := uint64(d.Id)
			ret = append(ret, fuse.Dirent{Inode: dirId, Name: d.Name, Type: fuse.DT_Dir})
		}
	}
	if files, e := filer.ListFiles(*mountOptions.filer, dir.Path, ""); e == nil {
		for _, f := range files.Files {
			if fileId, e := storage.ParseFileId(string(f.Id)); e == nil {
				fileInode := uint64(fileId.VolumeId)<<48 + fileId.Key
				ret = append(ret, fuse.Dirent{Inode: fileInode, Name: f.Name, Type: fuse.DT_File})
			}

		}
	}
	return ret, nil
}
