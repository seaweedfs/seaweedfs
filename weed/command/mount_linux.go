package command

import (
	"github.com/seaweedfs/fuse"
)

func osSpecificMountOptions() []fuse.MountOption {
	return []fuse.MountOption{
		fuse.AllowNonEmptyMount(),
	}
}
