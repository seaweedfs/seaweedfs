package filer

import (
	"bytes"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// onMetadataChangeEvent is triggered after filer processed change events from local or remote filers
func (f *Filer) onMetadataChangeEvent(event *filer_pb.SubscribeMetadataResponse) {
	f.maybeReloadFilerConfiguration(event)
	f.maybeReloadRemoteStorageConfigurationAndMapping(event)
	f.onBucketEvents(event)
	f.onEmptyFolderCleanupEvents(event)
}

func (f *Filer) onBucketEvents(event *filer_pb.SubscribeMetadataResponse) {
	message := event.EventNotification

	if f.DirBucketsPath == event.Directory {
		if filer_pb.IsCreate(event) {
			if message.NewEntry.IsDirectory {
				f.Store.OnBucketCreation(message.NewEntry.Name)
			}
		}
		if filer_pb.IsDelete(event) {
			if message.OldEntry.IsDirectory {
				f.Store.OnBucketDeletion(message.OldEntry.Name)
			}
		}
	}
}

// onEmptyFolderCleanupEvents handles create/delete events for empty folder cleanup
func (f *Filer) onEmptyFolderCleanupEvents(event *filer_pb.SubscribeMetadataResponse) {
	if f.EmptyFolderCleaner == nil || !f.EmptyFolderCleaner.IsEnabled() {
		return
	}

	message := event.EventNotification
	directory := event.Directory
	eventTime := time.Unix(0, event.TsNs)

	// Handle delete events - trigger folder cleanup check
	if filer_pb.IsDelete(event) && message.OldEntry != nil {
		f.EmptyFolderCleaner.OnDeleteEvent(directory, message.OldEntry.Name, message.OldEntry.IsDirectory, eventTime)
	}

	// Handle create events - cancel pending cleanup for the folder
	if filer_pb.IsCreate(event) && message.NewEntry != nil {
		f.EmptyFolderCleaner.OnCreateEvent(directory, message.NewEntry.Name, message.NewEntry.IsDirectory)
	}

	// Handle rename/move events
	if filer_pb.IsRename(event) {
		// Treat the old location as a delete
		if message.OldEntry != nil {
			f.EmptyFolderCleaner.OnDeleteEvent(directory, message.OldEntry.Name, message.OldEntry.IsDirectory, eventTime)
		}
		// Treat the new location as a create
		if message.NewEntry != nil {
			newDir := message.NewParentPath
			if newDir == "" {
				newDir = directory
			}
			f.EmptyFolderCleaner.OnCreateEvent(newDir, message.NewEntry.Name, message.NewEntry.IsDirectory)
		}
	}
}

func (f *Filer) maybeReloadFilerConfiguration(event *filer_pb.SubscribeMetadataResponse) {
	if DirectoryEtcSeaweedFS != event.Directory {
		if DirectoryEtcSeaweedFS != event.EventNotification.NewParentPath {
			return
		}
	}

	entry := event.EventNotification.NewEntry
	if entry == nil {
		return
	}

	glog.V(0).Infof("procesing %v", event)
	if entry.Name == FilerConfName {
		f.reloadFilerConfiguration(entry)
	}
}

func (f *Filer) readEntry(chunks []*filer_pb.FileChunk, size uint64) ([]byte, error) {
	var buf bytes.Buffer
	err := StreamContent(f.MasterClient, &buf, chunks, 0, int64(size))
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (f *Filer) reloadFilerConfiguration(entry *filer_pb.Entry) {
	fc := NewFilerConf()
	err := fc.loadFromChunks(f, entry.Content, entry.GetChunks(), FileSize(entry))
	if err != nil {
		glog.Errorf("read filer conf chunks: %v", err)
		return
	}
	f.FilerConf = fc
}

func (f *Filer) LoadFilerConf() {
	fc := NewFilerConf()
	err := util.Retry("loadFilerConf", func() error {
		return fc.loadFromFiler(f)
	})
	if err != nil {
		glog.Errorf("read filer conf: %v", err)
		return
	}
	f.FilerConf = fc
}

// //////////////////////////////////
// load and maintain remote storages
// //////////////////////////////////
func (f *Filer) LoadRemoteStorageConfAndMapping() {
	if err := f.RemoteStorage.LoadRemoteStorageConfigurationsAndMapping(f); err != nil {
		glog.Errorf("read remote conf and mapping: %v", err)
		return
	}
}
func (f *Filer) maybeReloadRemoteStorageConfigurationAndMapping(event *filer_pb.SubscribeMetadataResponse) {
	// FIXME add reloading
}
