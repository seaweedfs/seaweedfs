package mount

import (
	"context"
	"errors"
	"syscall"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// A clean open handle must not become a dirty whole-entry write merely from
// chmod or setxattr. Pending content writes and deferred creates still use the
// handle's existing flush path, which owns their unsaved data.
func (wfs *WFS) attributeEntrySnapshot(fh *FileHandle, entry *filer_pb.Entry) (*filer_pb.Entry, bool) {
	if fh == nil {
		return entry, true
	}
	// Match Read: the handle's active lock precedes entryLock.
	lock := wfs.fhLockTable.AcquireLock("attributeEntrySnapshot", fh.fh, util.SharedLock)
	defer wfs.fhLockTable.ReleaseLock(fh.fh, lock)
	fh.entryLock.Lock()
	defer fh.entryLock.Unlock()
	if fh.dirtyMetadata {
		return nil, false
	}
	fh.entry.RLock()
	defer fh.entry.RUnlock()
	return proto.Clone(fh.entry.Entry).(*filer_pb.Entry), true
}

// saveEntryAttributes retries only an entry comparison miss, always replaying
// the syscall's attribute change on the new entry. In particular, the cached
// chunks, size, inline content and unrelated attributes never win a conflict.
func (wfs *WFS) saveEntryAttributes(ctx context.Context, path util.FullPath, base *filer_pb.Entry, idFields uint32, change func(*filer_pb.Entry) fuse.Status, retain func(live, acked *filer_pb.Entry)) (*filer_pb.Entry, fuse.Status) {
	// Like the other metadata mutations, ignore a FUSE interrupt: the owner
	// may already have applied the update the caller would be told failed.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), metadataFlushTimeout)
	defer cancel()
	dir, _ := path.DirAndName()
	inode := base.GetAttributes().GetInode()
	localUid, localGid := base.GetAttributes().GetUid(), base.GetAttributes().GetGid()
	// The snapshot has local ids only. Where the id mapping is not one-to-one
	// the filer ids derived from them can miss; the re-read supplies the real ones.
	filerUid, filerGid := wfs.option.UidGidMapper.LocalToFiler(localUid, localGid)
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			// Bypass the mount cache: it supplied the snapshot just refused.
			entry, _, err := wfs.lookupOwnerEntry(ctx, path)
			if errors.Is(err, filer_pb.ErrNotFound) {
				return nil, fuse.ENOENT
			}
			if err != nil {
				return nil, grpcErrorToFuseStatus(err)
			}
			base = entry
			filerUid, filerGid = base.GetAttributes().GetUid(), base.GetAttributes().GetGid()
			wfs.mapPbIdFromFilerToLocal(base)
			if inode != 0 && base.GetAttributes().GetInode() != inode {
				return nil, fuse.Status(syscall.ESTALE)
			}
		}
		next := proto.Clone(base).(*filer_pb.Entry)
		if code := change(next); code != fuse.OK {
			return nil, code
		}
		// Whole-entry clones are as large as the chunk list; switch ids in place.
		// Only fields absent from the syscall keep the filer's raw ids.
		// Equal local values still express an explicit chown/chgrp intent.
		toFiler := func(uid, gid uint32) (uint32, uint32) {
			mappedUid, mappedGid := wfs.option.UidGidMapper.LocalToFiler(uid, gid)
			if idFields&fuse.FATTR_UID == 0 {
				mappedUid = filerUid
			}
			if idFields&fuse.FATTR_GID == 0 {
				mappedGid = filerGid
			}
			return mappedUid, mappedGid
		}
		restoreBase := mapIdsToFiler(base, func(_, _ uint32) (uint32, uint32) { return filerUid, filerGid })
		restoreNext := mapIdsToFiler(next, toFiler)
		req := &filer_pb.UpdateEntryRequest{Directory: dir, Entry: next, Condition: &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{{Kind: filer_pb.WriteCondition_IF_ENTRY_EQUAL, ExpectedEntry: base}}}, Signatures: []int32{wfs.signature}}
		var resp *filer_pb.UpdateEntryResponse
		// The stream currently reduces FailedPrecondition to EIO. Use the unary
		// RPC so a comparison miss remains distinguishable from an I/O failure.
		err := wfs.withFilerClient(ctx, false, wfs.filerRPCWait(), func(ctx context.Context, client filer_pb.SeaweedFilerClient) error {
			var err error
			resp, err = client.UpdateEntry(ctx, req)
			return err
		})
		restoreBase()
		if status.Code(err) == codes.FailedPrecondition {
			continue
		}
		if err != nil {
			return nil, grpcErrorToFuseStatus(err)
		}
		wfs.applySavedEntry(path, next, resp)
		restoreNext()
		wfs.retainAcknowledgedAttributes(path, next, retain)
		return next, fuse.OK
	}
	return nil, fuse.Status(syscall.EAGAIN)
}

// mapIdsToFiler maps entry's uid/gid to the filer's and returns a func that
// puts the local ids back exactly; mapping back can differ when the ids collide.
func mapIdsToFiler(entry *filer_pb.Entry, toFiler func(uid, gid uint32) (uint32, uint32)) (restore func()) {
	attr := entry.GetAttributes()
	if attr == nil {
		return func() {}
	}
	uid, gid := attr.Uid, attr.Gid
	attr.Uid, attr.Gid = toFiler(uid, gid)
	return func() { attr.Uid, attr.Gid = uid, gid }
}

// A background WRITE may dirty the handle while the attribute RPC is in
// flight. installAckedEntry intentionally leaves its content alone. Retain
// only the acknowledged attribute delta there, so the next content flush
// cannot undo a successful syscall. Do not recheck CREATE/REPLACE or WORM:
// those preconditions were already validated by the acknowledged mutation.
func (wfs *WFS) retainAcknowledgedAttributes(path util.FullPath, acked *filer_pb.Entry, retain func(live, acked *filer_pb.Entry)) {
	inode, found := wfs.inodeToPath.GetInode(path)
	if !found {
		return
	}
	fh, found := wfs.fhMap.FindFileHandle(inode)
	if !found {
		return
	}
	// Match Read: the handle's active lock precedes entryLock.
	lock := wfs.fhLockTable.AcquireLock("retainAcknowledgedAttributes", fh.fh, util.ExclusiveLock)
	defer wfs.fhLockTable.ReleaseLock(fh.fh, lock)
	fh.entryLock.Lock()
	defer fh.entryLock.Unlock()
	if !fh.dirtyMetadata || fh.isDeleted {
		return
	}
	fh.entry.Lock()
	defer fh.entry.Unlock()
	live := fh.entry.Entry
	if acked.GetAttributes().GetInode() != 0 && live.GetAttributes().GetInode() != acked.GetAttributes().GetInode() {
		return
	}
	retain(live, acked)
}

// A path lock covers the owner RPC through local installation/retention, and
// whole-entry metadata publication. WRITE and chunk upload remain independent.
// Keying by path also covers a handle opened while the RPC is in flight.
func (wfs *WFS) metadataPublicationTable() *util.LockTable[util.FullPath] {
	wfs.metadataPublicationOnce.Do(func() { wfs.metadataPublicationLocks = util.NewLockTable[util.FullPath]() })
	return wfs.metadataPublicationLocks
}
func (wfs *WFS) lockMetadataPublication(path util.FullPath) func() {
	table := wfs.metadataPublicationTable()
	lock := table.AcquireLock("metadata publication", path, util.ExclusiveLock)
	return func() { table.ReleaseLock(path, lock) }
}
