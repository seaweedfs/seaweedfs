package command

import (
	"strings"

	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/seaweedfs/fuse"
)

func osSpecificMountOptions() []fuse.MountOption {
	return []fuse.MountOption{
		fuse.AllowNonEmptyMount(),
	}
}

func checkMountPointAvailable(dir string) bool {
	mountPoint := dir
	if mountPoint != "/" && strings.HasSuffix(mountPoint, "/") {
		mountPoint = mountPoint[0 : len(mountPoint)-1]
	}

	if mounted, err := util.Mounted(mountPoint); err != nil || mounted {
		return false
	}

	return true
}
