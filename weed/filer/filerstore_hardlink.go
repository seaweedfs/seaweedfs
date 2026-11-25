package filer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// handleUpdateToHardLinks saves the shared record of entry, a file a write is
// about to store, and returns the hard link of the name at entry's path when
// the write replaces that name with another file. The caller drops that link
// once the write has gone through (dropLink), not before. The name is looked
// up with findCtx.
func (fsw *FilerStoreWrapper) handleUpdateToHardLinks(ctx, findCtx context.Context, entry *Entry) (replaced HardLinkId, err error) {

	if entry.IsDirectory() {
		return nil, nil
	}

	// check what is existing entry
	// glog.V(4).Infof("handleUpdateToHardLinks FindEntry %s", entry.FullPath)
	actualStore := fsw.getActualStore(entry.FullPath)
	existingEntry, err := actualStore.FindEntry(findCtx, entry.FullPath)
	if err != nil && err != filer_pb.ErrNotFound {
		return nil, fmt.Errorf("update existing entry %s: %v", entry.FullPath, err)
	}
	// the name at entry's path is one of the file's names already
	held := err == nil && bytes.Equal(existingEntry.HardLinkId, entry.HardLinkId)

	if len(entry.HardLinkId) > 0 {
		// handle hard links
		glog.V(4).InfofCtx(ctx, "handleUpdateToHardLinks %s HardLinkId %x counter=%d chunks=%d",
			entry.FullPath, entry.HardLinkId, entry.HardLinkCounter, len(entry.GetChunks()))
		if err := fsw.saveHardLink(ctx, entry, !held); err != nil {
			return nil, fmt.Errorf("setHardLink %d: %v", entry.HardLinkId, err)
		}
	}

	// the old hard link, dropped after the write
	if err == nil && len(existingEntry.HardLinkId) != 0 && !held {
		return existingEntry.HardLinkId, nil
	}
	return nil, nil
}

// writeWithHardLink saves entry's shared record, if it has one, and stores
// entry with write. It returns the hard link the name at entry's path had
// before, for the caller to drop, when the write put another file there.
//
// A store that counts a file's names itself looks the name up, counts it,
// saves the record and stores the name as one change of the record
// (changeLinkRecord): another filer that deletes the name in between would
// otherwise leave it stored and not counted.
func (fsw *FilerStoreWrapper) writeWithHardLink(ctx context.Context, entry *Entry, write func(context.Context, *Entry) error) (replaced HardLinkId, err error) {
	// A name in a path-specific store is outside the record's transaction: a
	// change run again would find the name its first run stored and count it
	// as one the file already had. It is stored once the count is settled.
	together := fsw.getActualStore(entry.FullPath) == fsw.defaultStore
	err = fsw.changeLinkRecord(ctx, entry, func(txCtx context.Context) error {
		// A store of the same kind would read the name through the
		// default store's transaction, from the default store.
		findCtx := txCtx
		if !together {
			findCtx = ctx
		}
		var err error
		if replaced, err = fsw.handleUpdateToHardLinks(txCtx, findCtx, entry); err != nil || !together {
			return err
		}
		return write(txCtx, entry)
	})
	if err == nil && !together {
		err = write(ctx, entry)
	}
	return replaced, err
}

// saveHardLink saves the shared record of entry, whose name joins the file's
// names with this write or is one of them already. A store that counts the
// names itself (countsLinks) first sets entry's count to theirs; the caller
// is inside changeLinkRecord.
func (fsw *FilerStoreWrapper) saveHardLink(ctx context.Context, entry *Entry, joins bool) error {
	if fsw.countsLinks {
		if err := fsw.countLink(ctx, entry, joins); err != nil {
			return err
		}
	}
	return fsw.setHardLink(ctx, entry)
}

type movedLinkKey struct{}

// WithMovedLink marks the context of the write that stores a renamed entry
// under its new name. The name brings its hard link along: it is not counted
// as a new one, as the delete of its old name (OP "MV") does not count it off.
func WithMovedLink(ctx context.Context) context.Context {
	return context.WithValue(ctx, movedLinkKey{}, true)
}

// countLink sets entry's link count to the number of names its file has once
// this write is stored: one more than the shared record holds when the name
// joins them, the same when it is one of them or moves (WithMovedLink).
//
// The count a client sends is not taken. A client works it out from the entry
// it last saw, and its write replaces the whole record: two mounts that each
// add a name to a file with two both send three, and one that only changes
// the mode sends the count it had before another added a name. The record
// then counted fewer names than the file had, the delete of the last counted
// one deleted the record and the chunks (lastLinkDropped), and the names left
// over read chunks that were gone.
func (fsw *FilerStoreWrapper) countLink(ctx context.Context, entry *Entry, joins bool) error {
	joins = joins && ctx.Value(movedLinkKey{}) == nil
	value, err := fsw.KvGet(ctx, entry.HardLinkId)
	if errors.Is(err, ErrKvNotFound) {
		if joins || entry.HardLinkCounter < 1 {
			// the first name of a record. The count sent with it already has
			// the name a link is about to add, which is counted when it comes.
			entry.HardLinkCounter = 1
		}
		// else a name whose record is lost: what it says is all there is to go by
		return nil
	}
	if err != nil {
		return err
	}
	shared := &Entry{}
	if err = shared.DecodeAttributesAndChunks(value); err != nil {
		return err
	}
	count := shared.HardLinkCounter
	if joins {
		count++
	}
	if count < 1 {
		count = 1
	}
	entry.HardLinkCounter = count
	return nil
}

// changeLinkRecord runs change, which reads the shared record of entry's hard
// link and writes it back, so that no other change of the record comes
// between the two. Entries without a hard link, and stores that take the
// count as it is sent, just run it.
//
// On this filer the record's lock sees to that. Another filer on the same
// store is kept out by a transaction, where the store refuses to commit one
// over a change made since it began (ConflictRefusingStore); change then runs
// again, with the count the client sent. A caller that has a transaction open
// already leaves the refusal to it. Other stores shared by several filers have
// no such guard, nor has a change inside a transaction of a store that does
// not refuse such commits (the SQL stores).
func (fsw *FilerStoreWrapper) changeLinkRecord(ctx context.Context, entry *Entry, change func(ctx context.Context) error) error {
	if !fsw.countsLinks || len(entry.HardLinkId) == 0 || entry.IsDirectory() {
		return change(ctx)
	}
	sent := entry.HardLinkCounter
	return fsw.lockLinkRecord(ctx, entry.HardLinkId, func(ctx context.Context) error {
		entry.HardLinkCounter = sent
		return change(ctx)
	})
}

func (fsw *FilerStoreWrapper) lockLinkRecord(ctx context.Context, hardLinkId HardLinkId, change func(ctx context.Context) error) error {
	inTransaction := ctx.Value(storeTransactionKey{}) != nil
	// A real transaction keeps its connection until it commits, and the
	// holder of the lock may be waiting for one (as for lockDirectoryCreate):
	// inside one the lock is not taken, and the change is as safe from the
	// others as the store's transactions make it.
	isolating, _ := fsw.defaultStore.(IsolatingTransactionStore)
	if !inTransaction || isolating == nil || !isolating.IsolatesTransactions() {
		var h uint32
		for _, b := range hardLinkId {
			h = h*31 + uint32(b)
		}
		lock := &fsw.linkLocks[h%uint32(len(fsw.linkLocks))]
		lock.Lock()
		defer lock.Unlock()
	}

	refusing, ok := fsw.defaultStore.(ConflictRefusingStore)
	if !ok || !refusing.RefusesConflictingCommits() || inTransaction {
		return change(ctx)
	}
	for attempt := 1; ; attempt++ {
		txCtx, err := fsw.BeginTransaction(ctx)
		if err != nil {
			return err
		}
		if err = change(txCtx); err != nil {
			fsw.RollbackTransaction(txCtx)
			return err
		}
		if err = fsw.CommitTransaction(txCtx); err == nil {
			return nil
		}
		fsw.RollbackTransaction(txCtx)
		if !errors.Is(err, ErrTransactionConflict) || attempt == linkRecordAttempts {
			return err
		}
		glog.V(1).InfofCtx(ctx, "hard link %x: changing its record again (%d of %d): %v", hardLinkId, attempt+1, linkRecordAttempts, err)
		time.Sleep(time.Duration(attempt) * 10 * time.Millisecond)
	}
}

// linkRecordAttempts bounds how often a change of a hard link's record runs
// its transaction while the store refuses the commit.
const linkRecordAttempts = 5

// dropLink takes the name at p off its file's hard link once the store no
// longer holds the name: deleted, or replaced by a write of another file.
//
// Never before. A delete or write the store refuses (a TiKV write conflict,
// a database timeout) then leaves the name with its link and the count right,
// so it can run again. Taken off first, the count stayed one short of the names
// left, each run again took one more off, and at zero the file's record and
// chunks were deleted while another name still read them.
//
// When it fails the count stays one too high: the file's record and chunks
// outlive its last name, leaked rather than lost.
func (fsw *FilerStoreWrapper) dropLink(ctx context.Context, p util.FullPath, hardLinkId HardLinkId) {
	if len(hardLinkId) == 0 {
		return
	}
	glog.V(4).InfofCtx(ctx, "DeleteHardLink %s HardLinkId %x", p, hardLinkId)
	if err := fsw.DeleteHardLink(ctx, hardLinkId); err != nil {
		glog.ErrorfCtx(ctx, "%s is gone but hard link %x still counts it: %v", p, hardLinkId, err)
	}
}

func (fsw *FilerStoreWrapper) setHardLink(ctx context.Context, entry *Entry) error {
	if len(entry.HardLinkId) == 0 {
		return nil
	}
	key := entry.HardLinkId

	newBlob, encodeErr := entry.EncodeAttributesAndChunks()
	if encodeErr != nil {
		return encodeErr
	}

	glog.V(4).InfofCtx(ctx, "setHardLink KvPut %s HardLinkId %x nlink:%d blobLen=%d",
		entry.FullPath, entry.HardLinkId, entry.HardLinkCounter, len(newBlob))

	return fsw.KvPut(ctx, key, newBlob)
}

func (fsw *FilerStoreWrapper) maybeReadHardLink(ctx context.Context, entry *Entry) error {
	if len(entry.HardLinkId) == 0 {
		return nil
	}
	key := entry.HardLinkId

	glog.V(1).InfofCtx(ctx, "maybeReadHardLink %s HardLinkId %x", entry.FullPath, entry.HardLinkId)

	value, err := fsw.KvGet(ctx, key)
	if err != nil {
		if errors.Is(err, ErrKvNotFound) {
			glog.V(4).InfofCtx(ctx, "maybeReadHardLink %s HardLinkId %x: not found", entry.FullPath, entry.HardLinkId)
		} else {
			glog.ErrorfCtx(ctx, "read %s HardLinkId %x: %v", entry.FullPath, entry.HardLinkId, err)
		}
		return err
	}

	if err = entry.DecodeAttributesAndChunks(value); err != nil {
		glog.ErrorfCtx(ctx, "decode %s HardLinkId %x: %v", entry.FullPath, entry.HardLinkId, err)
		return err
	}

	glog.V(1).InfofCtx(ctx, "maybeReadHardLink %s HardLinkId %x nlink:%d chunks=%d",
		entry.FullPath, entry.HardLinkId, entry.HardLinkCounter, len(entry.GetChunks()))

	return nil
}

func (fsw *FilerStoreWrapper) DeleteHardLink(ctx context.Context, hardLinkId HardLinkId) error {
	if !fsw.countsLinks {
		return fsw.deleteHardLink(ctx, hardLinkId)
	}
	return fsw.lockLinkRecord(ctx, hardLinkId, func(ctx context.Context) error {
		return fsw.deleteHardLink(ctx, hardLinkId)
	})
}

func (fsw *FilerStoreWrapper) deleteHardLink(ctx context.Context, hardLinkId HardLinkId) error {
	key := hardLinkId
	glog.V(4).InfofCtx(ctx, "DeleteHardLink HardLinkId %x", key)
	value, err := fsw.KvGet(ctx, key)
	if err == ErrKvNotFound {
		glog.V(4).InfofCtx(ctx, "DeleteHardLink HardLinkId %x: already gone", key)
		return nil
	}
	if err != nil {
		return err
	}

	entry := &Entry{}
	if err = entry.DecodeAttributesAndChunks(value); err != nil {
		return err
	}

	entry.HardLinkCounter--
	if entry.HardLinkCounter <= 0 {
		glog.V(4).InfofCtx(ctx, "DeleteHardLink KvDelete HardLinkId %x counter reached %d",
			key, entry.HardLinkCounter)
		return fsw.KvDelete(ctx, key)
	}

	// POSIX: update ctime when nlink changes (a hard link was removed).
	entry.Attr.Ctime = time.Now().UTC()

	newBlob, encodeErr := entry.EncodeAttributesAndChunks()
	if encodeErr != nil {
		return encodeErr
	}

	glog.V(4).InfofCtx(ctx, "DeleteHardLink KvPut HardLinkId %x counter decremented to %d",
		key, entry.HardLinkCounter)
	return fsw.KvPut(ctx, key, newBlob)

}
