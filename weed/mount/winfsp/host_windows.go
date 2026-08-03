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

	// Uid and Gid are reported for every entry. The default of -1 leaves
	// ownership to WinFsp, which attributes files to the mounting user.
	Uid uint32
	Gid uint32

	// AttrTimeout bounds how long WinFsp caches attributes, in seconds.
	// Nothing invalidates its cache, so this is the only coherence knob.
	AttrTimeout float64

	// ReadOnly rejects every modification at the WinFsp layer.
	ReadOnly bool

	// Debug turns on cgofuse's operation trace.
	Debug bool

	// ExtraOptions are passed through to WinFsp as -o arguments.
	ExtraOptions []string
}

// Host is a WinFsp mount that has not been started yet.
type Host struct {
	host    *cgofuse.FileSystemHost
	options Options
}

// New wires wfs up to WinFsp. Nothing is mounted until Serve.
func New(wfs *mount.WFS, options Options) *Host {
	host := cgofuse.NewFileSystemHost(NewWinFS(wfs, options.Uid, options.Gid))
	host.SetCapReaddirPlus(true)
	host.SetCapCaseInsensitive(options.CaseInsensitive)
	host.SetUseIno(true)
	return &Host{host: host, options: options}
}

// Serve attaches the filesystem at mountPoint, which is a drive letter ("S:"),
// a directory that does not yet exist, or a UNC path. It blocks until the
// filesystem is unmounted.
func (h *Host) Serve(mountPoint string) error {
	opts := []string{
		"-o", "volname=" + h.volumeName(),
		"-o", "uid=-1",
		"-o", "gid=-1",
	}
	if h.options.AttrTimeout > 0 {
		timeout := strconv.FormatFloat(h.options.AttrTimeout, 'f', -1, 64)
		opts = append(opts, "-o", "attr_timeout="+timeout, "-o", "entry_timeout="+timeout)
	}
	if h.options.ReadOnly {
		opts = append(opts, "-o", "ro")
	}
	if h.options.Debug {
		opts = append(opts, "-d")
	}
	for _, extra := range h.options.ExtraOptions {
		opts = append(opts, "-o", extra)
	}

	if !h.host.Mount(mountPoint, opts) {
		return fmt.Errorf("WinFsp refused to mount %s; check that WinFsp is installed and the mount point is free", mountPoint)
	}
	return nil
}

// Unmount detaches the filesystem, releasing a blocked Serve.
func (h *Host) Unmount() bool {
	return h.host.Unmount()
}

func (h *Host) volumeName() string {
	if h.options.VolumeName == "" {
		return "SeaweedFS"
	}
	return h.options.VolumeName
}
