package winfsp

import (
	"fmt"
	"strconv"
	"strings"
	"time"

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

	// CacheTimeout bounds how long resolved paths and attributes may be
	// served from the adapter's cache, and how long WinFsp may serve cached
	// directory listings. Metadata events shorten it by purging; nothing
	// else invalidates these caches.
	CacheTimeout time.Duration

	// ReadOnly rejects every modification. WinFsp has no "ro" option — it
	// discards the flag and leaves the volume writable — so the refusal has
	// to happen in the operations themselves.
	ReadOnly bool

	// Debug turns on cgofuse's operation trace.
	Debug bool

	// ExtraOptions are passed through to WinFsp as -o arguments, after the
	// defaults, so they win when they name the same option.
	ExtraOptions []string
}

// Host is a WinFsp mount that has not been started yet.
type Host struct {
	host    *cgofuse.FileSystemHost
	options Options
}

// New wires wfs up to WinFsp. Nothing is mounted until Serve.
func New(wfs *mount.WFS, options Options) *Host {
	host := cgofuse.NewFileSystemHost(NewWinFS(wfs, options.Uid, options.Gid, options.ReadOnly))
	host.SetCapReaddirPlus(true)
	host.SetUseIno(true)
	return &Host{host: host, options: options}
}

// Notify wires the mount's metadata events to Windows. Called once before
// Serve; the host has to exist first, which is why it is not done in New.
func (h *Host) Notify(wfs *mount.WFS) {
	n := &notifier{host: h.host, mountRoot: wfs.MountRoot()}
	wfs.SetEntryChangeListener(n.notify)
}

// Serve attaches the filesystem at mountPoint, which is a drive letter ("S:"),
// a directory that does not yet exist, or a UNC path. It blocks until the
// filesystem is unmounted.
func (h *Host) Serve(mountPoint string) error {
	opts := []string{
		"-o", "volname=" + h.volumeName(),
		"-o", "uid=-1",
		"-o", "gid=-1",
		// Only an infinite FileInfoTimeout lets the Windows cache manager
		// cache file data; at any finite value every application read and
		// write is a synchronous trip into this process at whatever size the
		// application issued. Remote changes stay visible because every
		// applied metadata event goes through Notify, which purges the file's
		// cached pages along with its attributes.
		//
		// KeepFileCache is deliberately absent: it would keep the cache alive
		// past cleanup, deferring the close — and with it the flush that
		// persists a written file — until Windows reclaims the memory.
		"-o", "FileInfoTimeout=-1",
	}
	if h.options.CacheTimeout > 0 {
		ms := strconv.FormatInt(h.options.CacheTimeout.Milliseconds(), 10)
		// These would silently inherit the infinite FileInfoTimeout.
		opts = append(opts,
			"-o", "DirInfoTimeout="+ms,
			"-o", "VolumeInfoTimeout="+ms,
			"-o", "EaTimeout="+ms,
		)
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
