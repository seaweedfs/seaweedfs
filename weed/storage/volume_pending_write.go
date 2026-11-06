package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// pendingWrite is a write that has entered the volume but not returned yet.
// A delete of its needle in that window cancels it: the write may still be
// queued behind the volume locks, and landing after the delete would revive
// the needle on this copy alone.
type pendingWrite struct {
	cancelled bool
}

type pendingWrites struct {
	sync.Mutex
	byId map[NeedleId][]*pendingWrite
}

func (v *Volume) trackPendingWrite(id NeedleId) *pendingWrite {
	w := &pendingWrite{}
	v.pendingWrites.Lock()
	defer v.pendingWrites.Unlock()
	if v.pendingWrites.byId == nil {
		v.pendingWrites.byId = make(map[NeedleId][]*pendingWrite)
	}
	v.pendingWrites.byId[id] = append(v.pendingWrites.byId[id], w)
	return w
}

// untrackPendingWrite reports whether a delete cancelled the write.
func (v *Volume) untrackPendingWrite(id NeedleId, w *pendingWrite) bool {
	v.pendingWrites.Lock()
	defer v.pendingWrites.Unlock()
	writes := v.pendingWrites.byId[id]
	for i, x := range writes {
		if x == w {
			writes = append(writes[:i], writes[i+1:]...)
			break
		}
	}
	if len(writes) == 0 {
		delete(v.pendingWrites.byId, id)
	} else {
		v.pendingWrites.byId[id] = writes
	}
	return w.cancelled
}

// CancelPendingWrites cancels the writes of the needle that are in flight. A
// caller that found the needle missing must look it up again afterwards: a
// write that finished in between is no longer pending.
func (v *Volume) CancelPendingWrites(id NeedleId) {
	v.pendingWrites.Lock()
	defer v.pendingWrites.Unlock()
	for _, w := range v.pendingWrites.byId[id] {
		w.cancelled = true
	}
}

// dropCancelledWrite tombstones a cancelled write that landed, unless the
// needle has been deleted or rewritten since.
func (v *Volume) dropCancelledWrite(n *needle.Needle, offset uint64) error {
	v.dataFileSyncLock.Lock()
	defer v.dataFileSyncLock.Unlock()
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	if v.nm == nil {
		return nil
	}
	if nv, ok := v.nm.Get(n.Id); !ok || nv.Size.IsDeleted() || nv.Offset != ToOffset(int64(offset)) {
		return nil
	}
	if err := v.UnavailableError(); err != nil {
		return err
	}
	if _, noWriteOrDelete, _, _ := v.ReadOnlyReasons(); noWriteOrDelete {
		return fmt.Errorf("volume %d is read only", v.Id)
	}
	tombstone := &needle.Needle{Id: n.Id, Cookie: n.Cookie, LastModified: uint64(time.Now().Unix())}
	if _, err := v.doDeleteRequest(tombstone); err != nil {
		return err
	}
	return fmt.Errorf("needle %s: %w", needle.NewFileIdFromNeedle(v.Id, n), ErrorDeleted)
}
