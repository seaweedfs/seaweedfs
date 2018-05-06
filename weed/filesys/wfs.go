package filesys

import "bazil.org/fuse/fs"

type WFS struct {
	filer string
}

func NewSeaweedFileSystem(filer string) *WFS {
	return &WFS{
		filer: filer,
	}
}

func (wfs *WFS) Root() (fs.Node, error) {
	return &Dir{Path: "/", wfs: wfs}, nil
}
