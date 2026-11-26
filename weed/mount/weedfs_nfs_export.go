package mount

import (
	"context"
	"encoding/binary"
	"errors"
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

var exportedHandleKeyPrefix = []byte("mount.nfsExport.inode.")

func exportedHandleKey(inode uint64) []byte {
	key := make([]byte, len(exportedHandleKeyPrefix)+8)
	copy(key, exportedHandleKeyPrefix)
	binary.BigEndian.PutUint64(key[len(exportedHandleKeyPrefix):], inode)
	return key
}

// exportedHandles records on the filer the path of every file the mount has
// open, keyed by inode, so a handle the NFS server holds still resolves after
// the mount restarts or another mount takes over. A single writer applies the
// latest state of each changed inode, so records never go back to an older one.
type exportedHandles struct {
	mu       sync.Mutex
	paths    map[uint64]util.FullPath
	removals map[uint64]*time.Timer
	delay    time.Duration
	dirty    map[uint64]struct{}
	wake     chan struct{}
	put      func(key, value []byte) error
	get      func(ctx context.Context, key []byte) ([]byte, error)
}

func newExportedHandles(put func(key, value []byte) error, get func(ctx context.Context, key []byte) ([]byte, error)) *exportedHandles {
	h := &exportedHandles{
		paths:    make(map[uint64]util.FullPath),
		removals: make(map[uint64]*time.Timer),
		delay:    exportedHandleRemovalDelay,
		dirty:    make(map[uint64]struct{}),
		wake:     make(chan struct{}, 1),
		put:      put,
		get:      get,
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
			if err := h.put(exportedHandleKey(inode), []byte(path)); err != nil {
				glog.Warningf("record nfs export handle %d %q: %v", inode, path, err)
			}
		}
	}
}

// RememberPath records the path of an open inode, cancelling a pending removal.
func (h *exportedHandles) RememberPath(inode uint64, path util.FullPath) {
	if h == nil || path == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if t := h.removals[inode]; t != nil {
		t.Stop()
		delete(h.removals, inode)
	}
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

// GetPath reads the path the filer holds for an inode, which a mount, this or
// an earlier one, had open.
func (h *exportedHandles) GetPath(ctx context.Context, inode uint64) (util.FullPath, fuse.Status) {
	if h == nil {
		return "", fuse.Status(syscall.ESTALE)
	}
	value, err := h.get(ctx, exportedHandleKey(inode))
	if err != nil {
		glog.Warningf("read nfs export handle %d: %v", inode, err)
		return "", fuse.EIO
	}
	if len(value) == 0 {
		return "", fuse.Status(syscall.ESTALE)
	}
	return util.FullPath(value), fuse.OK
}

func (wfs *WFS) newExportedHandles() *exportedHandles {
	return newExportedHandles(func(key, value []byte) error {
		return wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.KvPut(context.Background(), &filer_pb.KvPutRequest{Key: key, Value: value})
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
	})
}
