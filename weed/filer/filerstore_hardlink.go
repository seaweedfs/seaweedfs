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
// once the write has gone through (dropLink), not before.
func (fsw *FilerStoreWrapper) handleUpdateToHardLinks(ctx context.Context, entry *Entry) (replaced HardLinkId, err error) {

	if entry.IsDirectory() {
		return nil, nil
	}

	if len(entry.HardLinkId) > 0 {
		// handle hard links
		glog.V(4).InfofCtx(ctx, "handleUpdateToHardLinks %s HardLinkId %x counter=%d chunks=%d",
			entry.FullPath, entry.HardLinkId, entry.HardLinkCounter, len(entry.GetChunks()))
		if err := fsw.setHardLink(ctx, entry); err != nil {
			return nil, fmt.Errorf("setHardLink %d: %v", entry.HardLinkId, err)
		}
	}

	// check what is existing entry
	// glog.V(4).Infof("handleUpdateToHardLinks FindEntry %s", entry.FullPath)
	actualStore := fsw.getActualStore(entry.FullPath)
	existingEntry, err := actualStore.FindEntry(ctx, entry.FullPath)
	if err != nil && err != filer_pb.ErrNotFound {
		return nil, fmt.Errorf("update existing entry %s: %v", entry.FullPath, err)
	}

	// the old hard link, dropped after the write
	if err == nil && len(existingEntry.HardLinkId) != 0 && !bytes.Equal(existingEntry.HardLinkId, entry.HardLinkId) {
		return existingEntry.HardLinkId, nil
	}
	return nil, nil
}

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
