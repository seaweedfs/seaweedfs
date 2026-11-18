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
	n         *needle.Needle
	cookie    Cookie
	cancelled bool
}

type pendingWrites struct {
	sync.Mutex
	byId map[NeedleId][]*pendingWrite
}

func (v *Volume) trackPendingWrite(n *needle.Needle) *pendingWrite {
	w := &pendingWrite{n: n, cookie: n.Cookie}
	v.pendingWrites.Lock()
	defer v.pendingWrites.Unlock()
	if v.pendingWrites.byId == nil {
		v.pendingWrites.byId = make(map[NeedleId][]*pendingWrite)
	}
	v.pendingWrites.byId[n.Id] = append(v.pendingWrites.byId[n.Id], w)
	return w
}

func (v *Volume) untrackPendingWrite(w *pendingWrite) {
	v.pendingWrites.Lock()
	defer v.pendingWrites.Unlock()
	id := w.n.Id
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
}

// CancelPendingWrites cancels the writes of the needle that are in flight. A
// caller that found the needle missing must look it up again afterwards: a
// write that finished in between is no longer pending.
func (v *Volume) CancelPendingWrites(id NeedleId, cookie Cookie) {
	v.pendingWrites.Lock()
	defer v.pendingWrites.Unlock()
	for _, w := range v.pendingWrites.byId[id] {
		if w.cookie == cookie {
			w.cancelled = true
		}
	}
}

func (v *Volume) isCancelledWrite(n *needle.Needle) bool {
	v.pendingWrites.Lock()
	defer v.pendingWrites.Unlock()
	for _, w := range v.pendingWrites.byId[n.Id] {
		if w.n == n {
			return w.cancelled
		}
	}
	return false
}

// dropCancelledWrite tombstones a cancelled write right after it is published,
// under the same dataFileSyncLock and dataFileAccessLock hold, so no reader,
// later write or compaction sees it live in between.
func (v *Volume) dropCancelledWrite(n *needle.Needle, offset uint64) error {
	if !v.isCancelledWrite(n) {
		return nil
	}
	if nv, ok := v.nm.Get(n.Id); !ok || nv.Size.IsDeleted() || nv.Offset != ToOffset(int64(offset)) {
		return nil
	}
	tombstone := &needle.Needle{Id: n.Id, Cookie: n.Cookie, LastModified: uint64(time.Now().Unix())}
	if _, err := v.doDeleteRequest(tombstone); err != nil {
		return err
	}
	return fmt.Errorf("needle %s: %w", needle.NewFileIdFromNeedle(v.Id, n), ErrorDeleted)
}
