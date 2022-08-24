package filer

import (
	"bytes"
	"context"
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
		glog.V(4).Infof("handleUpdateToHardLinks DeleteHardLink %s", entry.FullPath)
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

	glog.V(4).Infof("setHardLink %v nlink:%d", entry.FullPath, entry.HardLinkCounter)

	return fsw.KvPut(ctx, key, newBlob)
}

func (fsw *FilerStoreWrapper) maybeReadHardLink(ctx context.Context, entry *Entry) error {
	if len(entry.HardLinkId) == 0 {
		return nil
	}
	key := entry.HardLinkId

	value, err := fsw.KvGet(ctx, key)
	if err != nil {
		glog.Errorf("read %s hardlink %d: %v", entry.FullPath, entry.HardLinkId, err)
		return err
	}

	if err = entry.DecodeAttributesAndChunks(value); err != nil {
		glog.Errorf("decode %s hardlink %d: %v", entry.FullPath, entry.HardLinkId, err)
		return err
	}

	glog.V(4).Infof("maybeReadHardLink %v nlink:%d", entry.FullPath, entry.HardLinkCounter)

	return nil
}

func (fsw *FilerStoreWrapper) DeleteHardLink(ctx context.Context, hardLinkId HardLinkId) error {
	key := hardLinkId
	value, err := fsw.KvGet(ctx, key)
	if err == ErrKvNotFound {
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
		glog.V(4).Infof("DeleteHardLink KvDelete %v", key)
		return fsw.KvDelete(ctx, key)
	}

	newBlob, encodeErr := entry.EncodeAttributesAndChunks()
	if encodeErr != nil {
		return encodeErr
	}

	glog.V(4).Infof("DeleteHardLink KvPut %v", key)
	return fsw.KvPut(ctx, key, newBlob)

}
