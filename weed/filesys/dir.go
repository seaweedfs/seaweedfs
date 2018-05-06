package filesys

import (
	"context"
	"fmt"
	"os"
	"path"

	"bazil.org/fuse/fs"
	"bazil.org/fuse"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"sync"
)

type Dir struct {
	Path        string
	NodeMap     map[string]fs.Node
	NodeMapLock sync.Mutex
	wfs         *WFS
}

func (dir *Dir) Attr(context context.Context, attr *fuse.Attr) error {
	attr.Mode = os.ModeDir | 0555
	return nil
}

func (dir *Dir) Lookup(ctx context.Context, name string) (node fs.Node, err error) {

	dir.NodeMapLock.Lock()
	defer dir.NodeMapLock.Unlock()

	if dir.NodeMap == nil {
		dir.NodeMap = make(map[string]fs.Node)
	}

	if node, ok := dir.NodeMap[name]; ok {
		return node, nil
	}

	if entry, err := filer.LookupDirectoryEntry(dir.wfs.filer, dir.Path, name); err == nil {
		if !entry.Found {
			return nil, fuse.ENOENT
		}
		if entry.FileId != "" {
			node = &File{FileId: filer.FileId(entry.FileId), Name: name, wfs: dir.wfs}
		} else {
			node = &Dir{Path: path.Join(dir.Path, name), wfs: dir.wfs}
		}
		dir.NodeMap[name] = node
		return node, nil
	}

	return nil, fuse.ENOENT
}

func (dir *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	var ret []fuse.Dirent
	if dirs, e := filer.ListDirectories(dir.wfs.filer, dir.Path); e == nil {
		for _, d := range dirs.Directories {
			dirent := fuse.Dirent{Name: string(d), Type: fuse.DT_Dir}
			ret = append(ret, dirent)
		}
	}
	if files, e := filer.ListFiles(dir.wfs.filer, dir.Path, ""); e == nil {
		for _, f := range files.Files {
			dirent := fuse.Dirent{Name: f.Name, Type: fuse.DT_File}
			ret = append(ret, dirent)
		}
	}
	return ret, nil
}

func (dir *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {

	dir.NodeMapLock.Lock()
	defer dir.NodeMapLock.Unlock()

	name := path.Join(dir.Path, req.Name)
	err := filer.DeleteDirectoryOrFile(dir.wfs.filer, name, req.Dir)
	if err != nil {
		fmt.Printf("Delete file %s [ERROR] %s\n", name, err)
	} else {
		delete(dir.NodeMap, req.Name)
	}

	return err
}
