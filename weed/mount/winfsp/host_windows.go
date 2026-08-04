package winfsp

import (
	"fmt"
	"strconv"
	"strings"

	cgofuse "github.com/winfsp/cgofuse/fuse"

	"github.com/seaweedfs/seaweedfs/weed/mount"
)

// Options are the WinFsp-specific knobs the mount command passes through.
type Options struct {
	// VolumeName labels the drive in Explorer.
	VolumeName string

	// Uid and Gid are reported for every entry. WinFsp overrides what is
	// reported anyway (see the uid=-1 option below), so these are what gets
	// written to the filer and read by every other client.
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

	if err := h.mount(mountPoint, opts); err != nil {
		return err
	}
	return nil
}

// mount turns a refusal into an error. cgofuse panics rather than returning
// when winfsp-x64.dll is missing, which is the most likely reason for a
// failure here and the one worth naming.
func (h *Host) mount(mountPoint string, opts []string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("mounting %s failed (%v); is WinFsp installed?", mountPoint, r)
		}
	}()
	if !h.host.Mount(mountPoint, opts) {
		return fmt.Errorf("WinFsp refused to mount %s; check that WinFsp is installed and the mount point is free", mountPoint)
	}
	return nil
}

// Unmount detaches the filesystem, releasing a blocked Serve.
func (h *Host) Unmount() bool {
	return h.host.Unmount()
}

// volumeName keeps the label parseable: WinFsp splits options on commas, so a
// label carrying one would be cut short and take the rest of the option string
// with it.
func (h *Host) volumeName() string {
	name := strings.ReplaceAll(h.options.VolumeName, ",", "+")
	if name == "" {
		return "SeaweedFS"
	}
	return name
}
