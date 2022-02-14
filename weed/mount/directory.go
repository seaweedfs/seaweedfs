package mount

import (
	"bytes"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/hanwen/go-fuse/v2/fs"
	"strings"
)

type Directory struct {
	fs.Inode

	name   string
	wfs    *WFS
	entry  *filer_pb.Entry
	parent *Directory
	id     uint64
}

func (dir *Directory) FullPath() string {
	var parts []string
	for p := dir; p != nil; p = p.parent {
		if strings.HasPrefix(p.name, "/") {
			if len(p.name) > 1 {
				parts = append(parts, p.name[1:])
			}
		} else {
			parts = append(parts, p.name)
		}
	}

	if len(parts) == 0 {
		return "/"
	}

	var buf bytes.Buffer
	for i := len(parts) - 1; i >= 0; i-- {
		buf.WriteString("/")
		buf.WriteString(parts[i])
	}
	return buf.String()
}
