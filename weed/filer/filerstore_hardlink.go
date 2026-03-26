package filer

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func (fsw *FilerStoreWrapper) handleUpdateToHardLinks(ctx context.Context, entry *Entry) error {

	if entry.IsDirectory() {
		return nil
	}

	if len(entry.HardLinkId) > 0 {
		// handle hard links
		glog.V(4).InfofCtx(ctx, "handleUpdateToHardLinks %s HardLinkId %x counter=%d chunks=%d",
			entry.FullPath, entry.HardLinkId, entry.HardLinkCounter, len(entry.GetChunks()))
		if err := fsw.setHardLink(ctx, entry); err != nil {
			return fmt.Errorf("setHardLink %d: %v", entry.HardLinkId, err)
		}
	}

	// check what is existing entry
	// glog.V(4).Infof("handleUpdateToHardLinks FindEntry %s", entry.FullPath)
	actualStore := fsw.getActualStore(entry.FullPath)
	existingEntry, err := actualStore.FindEntry(ctx, entry.FullPath)
	if err != nil && err != filer_pb.ErrNotFound {
		return fmt.Errorf("update existing entry %s: %v", entry.FullPath, err)
	}

	// remove old hard link
	if err == nil && len(existingEntry.HardLinkId) != 0 && bytes.Compare(existingEntry.HardLinkId, entry.HardLinkId) != 0 {
		glog.V(4).InfofCtx(ctx, "handleUpdateToHardLinks DeleteHardLink %s", entry.FullPath)
		if err = fsw.DeleteHardLink(ctx, existingEntry.HardLinkId); err != nil {
			return err
		}
	}
	return nil
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

	newBlob, encodeErr := entry.EncodeAttributesAndChunks()
	if encodeErr != nil {
		return encodeErr
	}

	glog.V(4).InfofCtx(ctx, "DeleteHardLink KvPut HardLinkId %x counter decremented to %d",
		key, entry.HardLinkCounter)
	return fsw.KvPut(ctx, key, newBlob)

}
