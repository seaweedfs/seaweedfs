// +build linux darwin

package command

import (
	"fmt"
	"os"
	"runtime"

	"path"

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

	c, err := fuse.Mount(*mountOptions.dir, fuse.LocalVolume())
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

type WFS struct{}

func (WFS) Root() (fs.Node, error) {
	return &Dir{Path: "/"}, nil
}

type Dir struct {
	Id        uint64
	Path      string
	DirentMap map[string]*fuse.Dirent
}

func (dir *Dir) Attr(context context.Context, attr *fuse.Attr) error {
	attr.Inode = dir.Id
	attr.Mode = os.ModeDir | 0555
	return nil
}

func (dir *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if dirent, ok := dir.DirentMap[name]; ok {
		if dirent.Type == fuse.DT_File {
			return &File{dirent.Inode, dirent.Name}, nil
		}
		return &Dir{
			Id:   dirent.Inode,
			Path: path.Join(dir.Path, dirent.Name),
		}, nil
	}
	return nil, fuse.ENOENT
}

func (dir *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	var ret []fuse.Dirent
	if dir.DirentMap == nil {
		dir.DirentMap = make(map[string]*fuse.Dirent)
	}
	if dirs, e := filer.ListDirectories(*mountOptions.filer, dir.Path); e == nil {
		for _, d := range dirs.Directories {
			dirId := uint64(d.Id)
			dirent := fuse.Dirent{Inode: dirId, Name: d.Name, Type: fuse.DT_Dir}
			ret = append(ret, dirent)
			dir.DirentMap[d.Name] = &dirent
		}
	}
	if files, e := filer.ListFiles(*mountOptions.filer, dir.Path, ""); e == nil {
		for _, f := range files.Files {
			if fileId, e := storage.ParseFileId(string(f.Id)); e == nil {
				fileInode := uint64(fileId.VolumeId)<<48 + fileId.Key
				dirent := fuse.Dirent{Inode: fileInode, Name: f.Name, Type: fuse.DT_File}
				ret = append(ret, dirent)
				dir.DirentMap[f.Name] = &dirent
			}
		}
	}
	return ret, nil
}

type File struct {
	Id uint64
	// FileId filer.FileId
	Name string
}

func (file *File) Attr(context context.Context, attr *fuse.Attr) error {
	attr.Inode = file.Id
	attr.Mode = 0000
	return nil
}
