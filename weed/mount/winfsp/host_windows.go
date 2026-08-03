package winfsp

import (
	"fmt"
	"strconv"

	cgofuse "github.com/winfsp/cgofuse/fuse"

	"github.com/seaweedfs/seaweedfs/weed/mount"
)

// Options are the WinFsp-specific knobs the mount command passes through.
type Options struct {
	// VolumeName labels the drive in Explorer.
	VolumeName string

	// CaseInsensitive matches how most Windows software expects a filesystem
	// to behave. The filer itself is case-sensitive, so two names differing
	// only in case become unreachable when this is set.
	CaseInsensitive bool

	// Uid and Gid are reported for every entry, since the filer's ownership
	// does not map onto Windows accounts.
	Uid uint32
	Gid uint32

	// AttrTimeout bounds how long WinFsp caches attributes, in seconds.
	// Nothing invalidates its cache, so this is the only coherence knob.
	AttrTimeout float64

	// Debug turns on cgofuse's operation trace.
	Debug bool
}

// Host is a running WinFsp mount.
type Host struct {
	host *cgofuse.FileSystemHost
}

// Mount attaches wfs at mountPoint, which is a drive letter ("S:"), an empty
// directory, or a UNC path. It blocks until the filesystem is unmounted.
func Mount(wfs *mount.WFS, mountPoint string, options Options) error {
	host := cgofuse.NewFileSystemHost(NewWinFS(wfs, options.Uid, options.Gid))
	host.SetCapReaddirPlus(true)
	host.SetCapCaseInsensitive(options.CaseInsensitive)
	host.SetUseIno(true)

	opts := []string{
		"-o", "volname=" + volumeName(options.VolumeName),
		"-o", "uid=-1",
		"-o", "gid=-1",
	}
	if options.AttrTimeout > 0 {
		timeout := strconv.FormatFloat(options.AttrTimeout, 'f', -1, 64)
		opts = append(opts, "-o", "attr_timeout="+timeout, "-o", "entry_timeout="+timeout)
	}
	if options.Debug {
		opts = append(opts, "-d")
	}

	if !host.Mount(mountPoint, opts) {
		return fmt.Errorf("mount %s: WinFsp rejected the mount; is WinFsp installed?", mountPoint)
	}
	return nil
}

// Unmount detaches the filesystem, releasing the blocked Mount call.
func (h *Host) Unmount() bool {
	return h.host.Unmount()
}

func volumeName(name string) string {
	if name == "" {
		return "SeaweedFS"
	}
	return name
}
