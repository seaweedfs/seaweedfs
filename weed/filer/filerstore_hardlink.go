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
// up with findCtx. names is for the caller to record once entry is stored
// (recordLinkNames).
func (fsw *FilerStoreWrapper) handleUpdateToHardLinks(ctx, findCtx context.Context, entry *Entry) (replaced HardLinkId, names *linkNames, err error) {

	if entry.IsDirectory() {
		return nil, nil, nil
	}

	// check what is existing entry
	// glog.V(4).Infof("handleUpdateToHardLinks FindEntry %s", entry.FullPath)
	actualStore := fsw.getActualStore(entry.FullPath)
	existingEntry, err := actualStore.FindEntry(findCtx, entry.FullPath)
	if err != nil && err != filer_pb.ErrNotFound {
		return nil, nil, fmt.Errorf("update existing entry %s: %v", entry.FullPath, err)
	}
	// the name at entry's path is one of the file's names already
	held := err == nil && bytes.Equal(existingEntry.HardLinkId, entry.HardLinkId)

	if len(entry.HardLinkId) > 0 {
		// handle hard links
		glog.V(4).InfofCtx(ctx, "handleUpdateToHardLinks %s HardLinkId %x counter=%d chunks=%d",
			entry.FullPath, entry.HardLinkId, entry.HardLinkCounter, len(entry.GetChunks()))
		var saveErr error
		if names, saveErr = fsw.saveHardLink(ctx, entry, !held); saveErr != nil {
			return nil, nil, fmt.Errorf("setHardLink %d: %v", entry.HardLinkId, saveErr)
		}
	}

	// the old hard link, dropped after the write
	if err == nil && len(existingEntry.HardLinkId) != 0 && !held {
		return existingEntry.HardLinkId, names, nil
	}
	return nil, names, nil
}

// writeWithHardLink saves entry's shared record, if it has one, and stores
// entry with write. It returns the hard link the name at entry's path had
// before, for the caller to drop, when the write put another file there.
//
// A filer's own store looks the name up, counts it, saves the record, stores
// the name and records it as one change of the record (changeLinkRecord):
// another filer that deletes the name in between would otherwise leave it
// stored and not counted.
func (fsw *FilerStoreWrapper) writeWithHardLink(ctx context.Context, entry *Entry, write func(context.Context, *Entry) error) (replaced HardLinkId, err error) {
	// A name in a path-specific store is outside the record's transaction: a
	// change run again would find the name its first run stored and count it
	// as one the file already had. It is stored once the count is settled.
	// Two filers that store the same such name at the same moment both find
	// it absent and both count it: the count is one too high from then on,
	// and the file's record and chunks outlive its last name.
	together := fsw.getActualStore(entry.FullPath) == fsw.defaultStore
	var names *linkNames
	var store func(context.Context) error
	if !together {
		store = func(ctx context.Context) error {
			if err := write(ctx, entry); err != nil {
				return err
			}
			return fsw.recordLinkNames(ctx, names, false)
		}
	}
	err = fsw.changeLinkRecord(ctx, entry, func(txCtx context.Context) error {
		// A store of the same kind would read the name through the
		// default store's transaction, from the default store.
		findCtx := txCtx
		if !together {
			findCtx = ctx
		}
		var err error
		if replaced, names, err = fsw.handleUpdateToHardLinks(txCtx, findCtx, entry); err != nil || !together {
			return err
		}
		if err = write(txCtx, entry); err != nil {
			return err
		}
		return fsw.recordLinkNames(txCtx, names, true)
	}, store)
	return replaced, err
}

// saveHardLink saves the shared record of entry, whose name joins the file's
// names with this write or is one of them already. A filer's own store
// (countsLinks) first raises entry's count to the names it counts itself
// (countLink); the caller is inside changeLinkRecord, and records the names
// it gets back once the name is stored.
func (fsw *FilerStoreWrapper) saveHardLink(ctx context.Context, entry *Entry, joins bool) (names *linkNames, err error) {
	if fsw.countsLinks {
		if names, err = fsw.countLink(ctx, entry, joins); err != nil {
			return nil, err
		}
	}
	return names, fsw.setHardLink(ctx, entry)
}

type movedLinkKey struct{}

// WithMovedLink marks the context of the write that stores a renamed entry
// under its new name. The name brings its hard link along: it is not counted
// as a new one, as the delete of its old name (OP "MV") does not count it off.
func WithMovedLink(ctx context.Context) context.Context {
	return context.WithValue(ctx, movedLinkKey{}, true)
}

// linkNames is the number of names a filer's own store counts for a
// hard-linked file, kept next to the file's shared record under linkNamesKey.
//
// The count in the record is the clients': each works it out from the entry
// it last read, one more for the name it is about to make, and its write
// replaces the whole record. Two mounts that each add a name to a file with
// two both send three, and one that only changes the mode sends the count it
// had before another added a name. The record then counted fewer names than
// the file had, the delete of the last counted one deleted the record and
// the chunks (lastLinkDropped), and the names left over read chunks that
// were gone.
//
// So the filer counts as well: one more for each name stored that did not
// hold the link before, one less for each deleted. The record gets whichever
// is higher, the count a client sends or the names counted here. It is never
// lower than what the client alone would have left, which still has the name
// of a link in the making in it while only the source's update has arrived,
// and never lower than the names the file has.
type linkNames struct {
	id       HardLinkId
	count    int32 // the names once the write is stored
	joined   bool  // the write adds one
	recorded int32 // the names recorded now; -1: none
}

func linkNamesKey(hardLinkId HardLinkId) []byte {
	return append(append(make([]byte, 0, len(hardLinkId)+6), hardLinkId...), "/names"...)
}

// readLinkNames reads the names counted for hardLinkId; known is false where
// none were counted yet (a record from before the filer counted).
func (fsw *FilerStoreWrapper) readLinkNames(ctx context.Context, hardLinkId HardLinkId) (count int32, known bool, err error) {
	value, err := fsw.KvGet(ctx, linkNamesKey(hardLinkId))
	if errors.Is(err, ErrKvNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if len(value) != 4 {
		return 0, false, nil
	}
	return int32(util.BytesToUint32(value)), true, nil
}

func (fsw *FilerStoreWrapper) writeLinkNames(ctx context.Context, hardLinkId HardLinkId, count int32) error {
	value := make([]byte, 4)
	util.Uint32toBytes(value, uint32(count))
	return fsw.KvPut(ctx, linkNamesKey(hardLinkId), value)
}

// countLink raises entry's link count to the names the file has once this
// write is stored, and returns them for recordLinkNames. The caller is inside
// changeLinkRecord.
func (fsw *FilerStoreWrapper) countLink(ctx context.Context, entry *Entry, joins bool) (*linkNames, error) {
	names := &linkNames{id: entry.HardLinkId, joined: joins && ctx.Value(movedLinkKey{}) == nil, recorded: -1}
	count, known, err := fsw.readLinkNames(ctx, entry.HardLinkId)
	if err != nil {
		return nil, err
	}
	if known {
		names.count, names.recorded = count, count
	} else if value, err := fsw.KvGet(ctx, entry.HardLinkId); err == nil {
		// A record from before the filer counted: its count stands in, and is
		// recorded now. Read again after a write the store refused, the record
		// would have that write's count in it.
		shared := &Entry{}
		if err = shared.DecodeAttributesAndChunks(value); err != nil {
			return nil, err
		}
		names.count, names.recorded = shared.HardLinkCounter, shared.HardLinkCounter
		if err = fsw.writeLinkNames(ctx, entry.HardLinkId, names.recorded); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, ErrKvNotFound) {
		return nil, err
	}
	if names.joined {
		names.count++
	}
	if names.count < 1 {
		names.count = 1 // the name being written
	}
	if entry.HardLinkCounter < names.count {
		entry.HardLinkCounter = names.count
	}
	return names, nil
}

// recordLinkNames records the names counted for a hard link once the name
// that was counted is stored: a write the store refused leaves them as they
// were, and sent again it counts the name once. A caller no longer inside
// the change of the record that counted (inChange false, under the record's
// lock) adds its name to whatever is recorded by now.
func (fsw *FilerStoreWrapper) recordLinkNames(ctx context.Context, names *linkNames, inChange bool) error {
	if names == nil || names.recorded == names.count {
		return nil
	}
	if inChange {
		return fsw.writeLinkNames(ctx, names.id, names.count)
	}
	return fsw.commitLinkRecord(ctx, names.id, func(ctx context.Context) error {
		count, known, err := fsw.readLinkNames(ctx, names.id)
		if err != nil {
			return err
		}
		switch {
		case !known:
			count = names.count
		case names.joined:
			count++
		case count < 1:
			count = 1
		default:
			return nil
		}
		return fsw.writeLinkNames(ctx, names.id, count)
	})
}

// changeLinkRecord runs change, which reads the shared record of entry's hard
// link and writes it back, so that no other change of the record comes
// between the two. Entries without a hard link, and stores that take the
// count as it is sent, just run it. store, if any, stores a name the record's
// transaction does not hold once change is saved, before this filer changes
// the record again: the next change finds the name.
//
// On this filer the record's lock sees to that. Another filer on the same
// store is kept out by a transaction, where the store refuses to commit one
// over a change made since it began (ConflictRefusingStore); change then runs
// again, with the count the client sent. A caller that has a transaction open
// already leaves the refusal to it. Other stores shared by several filers have
// no such guard, nor has a change inside a transaction of a store that does
// not refuse such commits (the SQL stores).
func (fsw *FilerStoreWrapper) changeLinkRecord(ctx context.Context, entry *Entry, change, store func(ctx context.Context) error) error {
	if !fsw.countsLinks || len(entry.HardLinkId) == 0 || entry.IsDirectory() {
		if err := change(ctx); err != nil || store == nil {
			return err
		}
		return store(ctx)
	}
	sent := entry.HardLinkCounter
	return fsw.lockLinkRecord(ctx, entry.HardLinkId, func(ctx context.Context) error {
		entry.HardLinkCounter = sent
		return change(ctx)
	}, store)
}

func (fsw *FilerStoreWrapper) lockLinkRecord(ctx context.Context, hardLinkId HardLinkId, change, store func(ctx context.Context) error) error {
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
	if err := fsw.commitLinkRecord(ctx, hardLinkId, change); err != nil || store == nil {
		return err
	}
	return store(ctx)
}

// commitLinkRecord runs change in a transaction of its own where the store
// refuses conflicting commits, and again while it does; the caller holds the
// record's lock.
func (fsw *FilerStoreWrapper) commitLinkRecord(ctx context.Context, hardLinkId HardLinkId, change func(ctx context.Context) error) error {
	refusing, ok := fsw.defaultStore.(ConflictRefusingStore)
	if !ok || !refusing.RefusesConflictingCommits() || ctx.Value(storeTransactionKey{}) != nil {
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
	}, nil)
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
	names, counted := int32(0), false
	if fsw.countsLinks {
		if names, counted, err = fsw.readLinkNames(ctx, hardLinkId); err != nil {
			return err
		}
		if names--; counted && entry.HardLinkCounter < names {
			// the names counted here outlast a count a client left too low
			entry.HardLinkCounter = names
		}
	}
	if entry.HardLinkCounter <= 0 {
		glog.V(4).InfofCtx(ctx, "DeleteHardLink KvDelete HardLinkId %x counter reached %d",
			key, entry.HardLinkCounter)
		if counted {
			if err = fsw.KvDelete(ctx, linkNamesKey(hardLinkId)); err != nil {
				return err
			}
		}
		return fsw.KvDelete(ctx, key)
	}
	if counted {
		if names < 0 {
			names = 0
		}
		if err = fsw.writeLinkNames(ctx, hardLinkId, names); err != nil {
			return err
		}
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
