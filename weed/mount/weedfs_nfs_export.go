package mount

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// exportedHandleRemovalDelay keeps a closed file's record on the filer for a
// while, so a mount that stops shortly after the NFS server closed its files
// still finds them when it comes back.
const exportedHandleRemovalDelay = time.Minute

const exportedHandleRetryDelay = time.Second

// maxExportedHandleRegistrations bounds a record that registrations of mounts
// which stopped with the file open would otherwise grow.
const maxExportedHandleRegistrations = 8

var exportedHandleKeyPrefix = []byte("mount.nfsExport.inode.")

func exportedHandleKey(inode uint64) []byte {
	key := make([]byte, len(exportedHandleKeyPrefix)+8)
	copy(key, exportedHandleKeyPrefix)
	binary.BigEndian.PutUint64(key[len(exportedHandleKeyPrefix):], inode)
	return key
}

// exportedHandles registers on the filer the path of every file the mount has
// open, keyed by inode, so a handle the NFS server holds still resolves after
// the mount restarts or another mount takes over. Each mount keeps its own
// registration in an inode's record. A single writer applies the latest state
// of each changed inode, so registrations never go back to an older one.
type exportedHandles struct {
	mu       sync.Mutex
	owner    uint32
	paths    map[uint64]util.FullPath
	opens    map[uint64]int
	removals map[uint64]*time.Timer
	delay    time.Duration
	dirty    map[uint64]struct{}
	wake     chan struct{}
	put      func(key, value []byte) error
	get      func(ctx context.Context, key []byte) ([]byte, error)
	lock     func(inode uint64) (unlock func(), err error)
}

func newExportedHandles(owner uint32, put func(key, value []byte) error, get func(ctx context.Context, key []byte) ([]byte, error), lock func(inode uint64) (unlock func(), err error)) *exportedHandles {
	h := &exportedHandles{
		owner:    owner,
		paths:    make(map[uint64]util.FullPath),
		opens:    make(map[uint64]int),
		removals: make(map[uint64]*time.Timer),
		delay:    exportedHandleRemovalDelay,
		dirty:    make(map[uint64]struct{}),
		wake:     make(chan struct{}, 1),
		put:      put,
		get:      get,
		lock:     lock,
	}
	go h.loop()
	return h
}

func (h *exportedHandles) markDirty(inode uint64) {
	h.dirty[inode] = struct{}{}
	select {
	case h.wake <- struct{}{}:
	default:
	}
}

func (h *exportedHandles) loop() {
	for range h.wake {
		for {
			h.mu.Lock()
			var inode uint64
			found := false
			for inode = range h.dirty {
				found = true
				break
			}
			if !found {
				h.mu.Unlock()
				break
			}
			delete(h.dirty, inode)
			path := h.paths[inode]
			h.mu.Unlock()
			if err := h.write(inode, path); err != nil {
				glog.Warningf("record nfs export handle %d %q: %v", inode, path, err)
				time.AfterFunc(exportedHandleRetryDelay, func() {
					h.mu.Lock()
					defer h.mu.Unlock()
					if h.paths[inode] == path {
						h.markDirty(inode)
					}
				})
			}
		}
	}
}

// write replaces this mount's registration of the inode with path, or drops
// it when path is empty, keeping those of other mounts. The filer KV has no
// compare-and-swap, so mounts take turns on a record through the lock manager
// under -dlm; without it, mounts that update the same inode at the same moment
// can still overwrite each other's.
func (h *exportedHandles) write(inode uint64, path util.FullPath) error {
	unlock, err := h.lock(inode)
	if err != nil {
		return err
	}
	defer unlock()
	key := exportedHandleKey(inode)
	value, err := h.get(context.Background(), key)
	if err != nil {
		return err
	}
	registrations := parseExportedHandle(value)
	kept := make([]exportedHandleRegistration, 0, len(registrations)+1)
	for _, r := range registrations {
		if r.owner != h.owner {
			kept = append(kept, r)
		}
	}
	if path == "" {
		if len(kept) == len(registrations) {
			return nil
		}
	} else {
		kept = append(kept, exportedHandleRegistration{owner: h.owner, path: path})
		if len(kept) > maxExportedHandleRegistrations {
			kept = kept[len(kept)-maxExportedHandleRegistrations:]
		}
	}
	return h.put(key, formatExportedHandle(kept))
}

type exportedHandleRegistration struct {
	owner uint32
	path  util.FullPath
}

func parseExportedHandle(value []byte) (registrations []exportedHandleRegistration) {
	for len(value) >= 8 {
		n := binary.BigEndian.Uint32(value[4:])
		if uint64(n) > uint64(len(value)-8) {
			break
		}
		registrations = append(registrations, exportedHandleRegistration{
			owner: binary.BigEndian.Uint32(value),
			path:  util.FullPath(value[8 : 8+n]),
		})
		value = value[8+n:]
	}
	return registrations
}

func formatExportedHandle(registrations []exportedHandleRegistration) (value []byte) {
	for _, r := range registrations {
		value = binary.BigEndian.AppendUint32(value, r.owner)
		value = binary.BigEndian.AppendUint32(value, uint32(len(r.path)))
		value = append(value, r.path...)
	}
	return value
}

// AcquireHandle records the path of an inode the mount opens a new file
// handle for, cancelling a pending removal.
func (h *exportedHandles) AcquireHandle(inode uint64, path util.FullPath) {
	if h == nil || path == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.opens[inode]++
	if t := h.removals[inode]; t != nil {
		t.Stop()
		delete(h.removals, inode)
	}
	h.setPath(inode, path)
}

// RememberPath moves the record of an inode to its new path, if it has one.
func (h *exportedHandles) RememberPath(inode uint64, path util.FullPath) {
	if h == nil || path == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, recorded := h.paths[inode]; recorded {
		h.setPath(inode, path)
	}
}

func (h *exportedHandles) setPath(inode uint64, path util.FullPath) {
	if h.paths[inode] != path {
		h.paths[inode] = path
		h.markDirty(inode)
	}
}

// ReleaseHandle drops the record of an inode the mount no longer has open,
// after a delay unless it is opened again.
func (h *exportedHandles) ReleaseHandle(inode uint64) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.opens[inode]--; h.opens[inode] > 0 {
		return
	}
	delete(h.opens, inode)
	if _, recorded := h.paths[inode]; !recorded || h.removals[inode] != nil {
		return
	}
	var t *time.Timer
	t = time.AfterFunc(h.delay, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.removals[inode] != t {
			return
		}
		delete(h.removals, inode)
		delete(h.paths, inode)
		h.markDirty(inode)
	})
	h.removals[inode] = t
}

// GetPath reads the path of the newest registration the filer holds for an
// inode, which a mount, this or another one, had open.
func (h *exportedHandles) GetPath(ctx context.Context, inode uint64) (util.FullPath, fuse.Status) {
	if h == nil {
		return "", fuse.Status(syscall.ESTALE)
	}
	value, err := h.get(ctx, exportedHandleKey(inode))
	if err != nil {
		glog.Warningf("read nfs export handle %d: %v", inode, err)
		return "", fuse.EIO
	}
	registrations := parseExportedHandle(value)
	if len(registrations) == 0 {
		return "", fuse.Status(syscall.ESTALE)
	}
	return registrations[len(registrations)-1].path, fuse.OK
}

// newExportedHandles names the mount's registrations after its host and mount
// directory, so a mount restarted in the same place takes over, and in time
// removes, those its predecessor left.
func (wfs *WFS) newExportedHandles() *exportedHandles {
	owner := uint32(wfs.signature)
	if hostname, err := os.Hostname(); err == nil {
		owner = uint32(util.HashToInt32([]byte(hostname + ":" + wfs.option.MountDirectory)))
	}
	return newExportedHandles(owner, func(key, value []byte) error {
		return wfs.withFilerClient(context.Background(), false, wfs.filerRPCWait(), func(ctx context.Context, client filer_pb.SeaweedFilerClient) error {
			resp, err := client.KvPut(ctx, &filer_pb.KvPutRequest{Key: key, Value: value})
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return errors.New(resp.Error)
			}
			return nil
		})
	}, func(ctx context.Context, key []byte) (value []byte, err error) {
		err = wfs.withFilerClient(ctx, false, wfs.filerRPCWait(), func(ctx context.Context, client filer_pb.SeaweedFilerClient) error {
			resp, err := client.KvGet(ctx, &filer_pb.KvGetRequest{Key: key})
			if err != nil {
				return err
			}
			if resp.Error != "" {
				return errors.New(resp.Error)
			}
			value = resp.Value
			return nil
		})
		return
	}, func(inode uint64) (func(), error) {
		if wfs.lockClient == nil {
			return func() {}, nil
		}
		key := fmt.Sprintf("%s%d", exportedHandleKeyPrefix, inode)
		lock := wfs.lockClient.NewShortLivedLock(key, fmt.Sprintf("mount-%d", wfs.signature))
		if lock == nil {
			return nil, fmt.Errorf("lock %s", key)
		}
		return func() { lock.StopShortLivedLock() }, nil
	})
}
