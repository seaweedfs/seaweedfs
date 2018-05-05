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

	util.OnInterrupt(func() {
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

var fileIdMap = make(map[uint64]filer.FileId)

type Dir struct {
	Path      string
	DirentMap map[string]*fuse.Dirent
}

func (dir *Dir) Attr(context context.Context, attr *fuse.Attr) error {
	attr.Mode = os.ModeDir | 0555
	return nil
}

func (dir *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if entry, err := filer.LookupDirectoryEntry(*mountOptions.filer, dir.Path, name); err == nil {
		if !entry.Found {
			return nil, fuse.ENOENT
		}
		if entry.FileId != "" {
			return &File{FileId: filer.FileId(entry.FileId), Name: name}, nil
		} else {
			return &Dir{Path: path.Join(dir.Path, name)}, nil
		}
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
			dirent := fuse.Dirent{Name: string(d), Type: fuse.DT_Dir}
			ret = append(ret, dirent)
			dir.DirentMap[string(d)] = &dirent
		}
	}
	if files, e := filer.ListFiles(*mountOptions.filer, dir.Path, ""); e == nil {
		for _, f := range files.Files {
			if fileId, e := storage.ParseFileId(string(f.Id)); e == nil {
				fileInode := uint64(fileId.VolumeId)<<48 + fileId.Key
				dirent := fuse.Dirent{Name: f.Name, Type: fuse.DT_File}
				ret = append(ret, dirent)
				dir.DirentMap[f.Name] = &dirent
				fileIdMap[fileInode] = f.Id
			}
		}
	}
	return ret, nil
}

func (dir *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	name := path.Join(dir.Path, req.Name)
	err := filer.DeleteDirectoryOrFile(*mountOptions.filer, name, req.Dir)
	if err != nil {
		fmt.Printf("Delete file %s [ERROR] %s\n", name, err)
	}
	return err
}

type File struct {
	FileId filer.FileId
	Name   string
}

func (file *File) Attr(context context.Context, attr *fuse.Attr) error {
	attr.Mode = 0444
	ret, err := filer.GetFileSize(*mountOptions.filer, string(file.FileId))
	if err == nil {
		attr.Size = ret.Size
	} else {
		fmt.Printf("Get file %s attr [ERROR] %s\n", file.Name, err)
	}
	return err
}

func (file *File) ReadAll(ctx context.Context) ([]byte, error) {
	ret, err := filer.GetFileContent(*mountOptions.filer, string(file.FileId))
	if err == nil {
		return ret.Content, nil
	}
	fmt.Printf("Get file %s content [ERROR] %s\n", file.Name, err)
	return nil, err
}
