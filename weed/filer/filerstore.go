package filer

import (
	"context"
	"errors"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	ErrUnsupportedListDirectoryPrefixed = errors.New("unsupported directory prefix listing")
	ErrKvNotImplemented                 = errors.New("kv not implemented yet")
	ErrKvNotFound                       = errors.New("kv: not found")
)

type FilerStore interface {
	// GetName gets the name to locate the configuration in filer.toml file
	GetName() string
	// Initialize initializes the file store
	Initialize(configuration util.Configuration, prefix string) error
	InsertEntry(context.Context, *Entry) error
	UpdateEntry(context.Context, *Entry) (err error)
	// err == filer_pb.ErrNotFound if not found
	FindEntry(context.Context, util.FullPath) (entry *Entry, err error)
	DeleteEntry(context.Context, util.FullPath) (err error)
	DeleteFolderChildren(context.Context, util.FullPath) (err error)
	ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int) ([]*Entry, error)
	ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int, prefix string) ([]*Entry, error)

	BeginTransaction(ctx context.Context) (context.Context, error)
	CommitTransaction(ctx context.Context) error
	RollbackTransaction(ctx context.Context) error

	KvPut(ctx context.Context, key []byte, value []byte) (err error)
	KvGet(ctx context.Context, key []byte) (value []byte, err error)
	KvDelete(ctx context.Context, key []byte) (err error)

	Shutdown()
}

type VirtualFilerStore interface {
	FilerStore
	DeleteHardLink(ctx context.Context, hardLinkId HardLinkId) error
}

type FilerStoreWrapper struct {
	ActualStore FilerStore
}

func NewFilerStoreWrapper(store FilerStore) *FilerStoreWrapper {
	if innerStore, ok := store.(*FilerStoreWrapper); ok {
		return innerStore
	}
	return &FilerStoreWrapper{
		ActualStore: store,
	}
}

func (fsw *FilerStoreWrapper) GetName() string {
	return fsw.ActualStore.GetName()
}

func (fsw *FilerStoreWrapper) Initialize(configuration util.Configuration, prefix string) error {
	return fsw.ActualStore.Initialize(configuration, prefix)
}

func (fsw *FilerStoreWrapper) InsertEntry(ctx context.Context, entry *Entry) error {
	stats.FilerStoreCounter.WithLabelValues(fsw.ActualStore.GetName(), "insert").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.ActualStore.GetName(), "insert").Observe(time.Since(start).Seconds())
	}()

	filer_pb.BeforeEntrySerialization(entry.Chunks)
	if entry.Mime == "application/octet-stream" {
		entry.Mime = ""
	}

	if entry.HardLinkId != 0 {
		// check what is existing entry
		existingEntry, err := fsw.ActualStore.FindEntry(ctx, entry.FullPath)

		if err == nil && entry.HardLinkId == existingEntry.HardLinkId {
			// updating the same entry
			if err := fsw.updateHardLink(ctx, entry); err != nil {
				return err
			}
			return nil
		} else {
			if err == nil && existingEntry.HardLinkId != 0 {
				// break away from the old hard link
				if err := fsw.DeleteHardLink(ctx, entry.HardLinkId); err != nil {
					return err
				}
			}
			// CreateLink 1.2 : update new file to hardlink mode
			// update one existing hard link, counter ++
			if err := fsw.increaseHardLink(ctx, entry.HardLinkId); err != nil {
				return err
			}
		}
	}

	return fsw.ActualStore.InsertEntry(ctx, entry)
}

func (fsw *FilerStoreWrapper) UpdateEntry(ctx context.Context, entry *Entry) error {
	stats.FilerStoreCounter.WithLabelValues(fsw.ActualStore.GetName(), "update").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.ActualStore.GetName(), "update").Observe(time.Since(start).Seconds())
	}()

	filer_pb.BeforeEntrySerialization(entry.Chunks)
	if entry.Mime == "application/octet-stream" {
		entry.Mime = ""
	}

	if entry.HardLinkId != 0 {
		// handle hard link

		// check what is existing entry
		existingEntry, err := fsw.ActualStore.FindEntry(ctx, entry.FullPath)
		if err != nil {
			return fmt.Errorf("update existing entry %s: %v", entry.FullPath, err)
		}

		err = fsw.maybeReadHardLink(ctx, &Entry{HardLinkId: entry.HardLinkId})
		if err == ErrKvNotFound {

			// CreateLink 1.1 : split source entry into hardlink+empty_entry

			// create hard link from existing entry, counter ++
			existingEntry.HardLinkId = entry.HardLinkId
			if err = fsw.createHardLink(ctx, existingEntry); err != nil {
				return fmt.Errorf("createHardLink %d: %v", existingEntry.HardLinkId, err)
			}

			// create the empty entry
			if err = fsw.ActualStore.UpdateEntry(ctx, &Entry{
				FullPath:   entry.FullPath,
				HardLinkId: entry.HardLinkId,
			}); err != nil {
				return fmt.Errorf("UpdateEntry to link %d: %v", entry.FullPath, err)
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("update entry %s: %v", entry.FullPath, err)
		}

		if entry.HardLinkId != existingEntry.HardLinkId {
			// if different hard link id, moving to a new hard link
			glog.Fatalf("unexpected. update entry to a new link. not implemented yet.")
		} else {
			// updating hardlink with new metadata
			if err = fsw.updateHardLink(ctx, entry); err != nil {
				return fmt.Errorf("updateHardLink %d from %s: %v", entry.HardLinkId, entry.FullPath, err)
			}
		}

		return nil
	}

	return fsw.ActualStore.UpdateEntry(ctx, entry)
}

func (fsw *FilerStoreWrapper) FindEntry(ctx context.Context, fp util.FullPath) (entry *Entry, err error) {
	stats.FilerStoreCounter.WithLabelValues(fsw.ActualStore.GetName(), "find").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.ActualStore.GetName(), "find").Observe(time.Since(start).Seconds())
	}()

	entry, err = fsw.ActualStore.FindEntry(ctx, fp)
	if err != nil {
		return nil, err
	}

	fsw.maybeReadHardLink(ctx, entry)

	filer_pb.AfterEntryDeserialization(entry.Chunks)
	return
}

func (fsw *FilerStoreWrapper) DeleteEntry(ctx context.Context, fp util.FullPath) (err error) {
	stats.FilerStoreCounter.WithLabelValues(fsw.ActualStore.GetName(), "delete").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.ActualStore.GetName(), "delete").Observe(time.Since(start).Seconds())
	}()

	existingEntry, findErr := fsw.FindEntry(ctx, fp)
	if findErr == filer_pb.ErrNotFound {
		return nil
	}
	if existingEntry.HardLinkId != 0 {
		// remove hard link
		if err = fsw.DeleteHardLink(ctx, existingEntry.HardLinkId); err != nil {
			return err
		}
	}

	return fsw.ActualStore.DeleteEntry(ctx, fp)
}

func (fsw *FilerStoreWrapper) DeleteFolderChildren(ctx context.Context, fp util.FullPath) (err error) {
	stats.FilerStoreCounter.WithLabelValues(fsw.ActualStore.GetName(), "deleteFolderChildren").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.ActualStore.GetName(), "deleteFolderChildren").Observe(time.Since(start).Seconds())
	}()

	return fsw.ActualStore.DeleteFolderChildren(ctx, fp)
}

func (fsw *FilerStoreWrapper) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int) ([]*Entry, error) {
	stats.FilerStoreCounter.WithLabelValues(fsw.ActualStore.GetName(), "list").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.ActualStore.GetName(), "list").Observe(time.Since(start).Seconds())
	}()

	entries, err := fsw.ActualStore.ListDirectoryEntries(ctx, dirPath, startFileName, includeStartFile, limit)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		fsw.maybeReadHardLink(ctx, entry)
		filer_pb.AfterEntryDeserialization(entry.Chunks)
	}
	return entries, err
}

func (fsw *FilerStoreWrapper) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int, prefix string) ([]*Entry, error) {
	stats.FilerStoreCounter.WithLabelValues(fsw.ActualStore.GetName(), "prefixList").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.ActualStore.GetName(), "prefixList").Observe(time.Since(start).Seconds())
	}()
	entries, err := fsw.ActualStore.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, prefix)
	if err == ErrUnsupportedListDirectoryPrefixed {
		entries, err = fsw.prefixFilterEntries(ctx, dirPath, startFileName, includeStartFile, limit, prefix)
	}
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		fsw.maybeReadHardLink(ctx, entry)
		filer_pb.AfterEntryDeserialization(entry.Chunks)
	}
	return entries, nil
}

func (fsw *FilerStoreWrapper) prefixFilterEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int, prefix string) (entries []*Entry, err error) {
	entries, err = fsw.ActualStore.ListDirectoryEntries(ctx, dirPath, startFileName, includeStartFile, limit)
	if err != nil {
		return nil, err
	}

	if prefix == "" {
		return
	}

	count := 0
	var lastFileName string
	notPrefixed := entries
	entries = nil
	for count < limit && len(notPrefixed) > 0 {
		for _, entry := range notPrefixed {
			lastFileName = entry.Name()
			if strings.HasPrefix(entry.Name(), prefix) {
				count++
				entries = append(entries, entry)
				if count >= limit {
					break
				}
			}
		}
		if count < limit {
			notPrefixed, err = fsw.ActualStore.ListDirectoryEntries(ctx, dirPath, lastFileName, false, limit)
			if err != nil {
				return
			}
		}
	}
	return
}

func (fsw *FilerStoreWrapper) BeginTransaction(ctx context.Context) (context.Context, error) {
	return fsw.ActualStore.BeginTransaction(ctx)
}

func (fsw *FilerStoreWrapper) CommitTransaction(ctx context.Context) error {
	return fsw.ActualStore.CommitTransaction(ctx)
}

func (fsw *FilerStoreWrapper) RollbackTransaction(ctx context.Context) error {
	return fsw.ActualStore.RollbackTransaction(ctx)
}

func (fsw *FilerStoreWrapper) Shutdown() {
	fsw.ActualStore.Shutdown()
}

func (fsw *FilerStoreWrapper) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	return fsw.ActualStore.KvPut(ctx, key, value)
}
func (fsw *FilerStoreWrapper) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	return fsw.ActualStore.KvGet(ctx, key)
}
func (fsw *FilerStoreWrapper) KvDelete(ctx context.Context, key []byte) (err error) {
	return fsw.ActualStore.KvDelete(ctx, key)
}

func (fsw *FilerStoreWrapper) createHardLink(ctx context.Context, entry *Entry) error {
	if entry.HardLinkId == 0 {
		return nil
	}
	key := entry.HardLinkId.Key()

	_, err := fsw.KvGet(ctx, key)
	if err != ErrKvNotFound {
		return fmt.Errorf("create hardlink %d: already exists: %v", entry.HardLinkId, err)
	}

	entry.HardLinkCounter = 1

	newBlob, encodeErr := entry.EncodeAttributesAndChunks()
	if encodeErr != nil {
		return encodeErr
	}

	return fsw.KvPut(ctx, key, newBlob)
}

func (fsw *FilerStoreWrapper) updateHardLink(ctx context.Context, entry *Entry) error {
	if entry.HardLinkId == 0 {
		return nil
	}
	key := entry.HardLinkId.Key()

	value, err := fsw.KvGet(ctx, key)
	if err == ErrKvNotFound {
		return fmt.Errorf("update hardlink %d: missing", entry.HardLinkId)
	}
	if err != nil {
		return fmt.Errorf("update hardlink %d err: %v", entry.HardLinkId, err)
	}

	existingEntry := &Entry{}
	if err = existingEntry.DecodeAttributesAndChunks(value); err != nil {
		return err
	}

	entry.HardLinkCounter = existingEntry.HardLinkCounter

	newBlob, encodeErr := entry.EncodeAttributesAndChunks()
	if encodeErr != nil {
		return encodeErr
	}

	return fsw.KvPut(ctx, key, newBlob)
}

func (fsw *FilerStoreWrapper) increaseHardLink(ctx context.Context, hardLinkId HardLinkId) error {
	if hardLinkId == 0 {
		return nil
	}
	key := hardLinkId.Key()

	value, err := fsw.KvGet(ctx, key)
	if err == ErrKvNotFound {
		return fmt.Errorf("increaseHardLink %d: missing", hardLinkId)
	}
	if err != nil {
		return fmt.Errorf("increaseHardLink %d err: %v", hardLinkId, err)
	}

	existingEntry := &Entry{}
	if err = existingEntry.DecodeAttributesAndChunks(value); err != nil {
		return err
	}

	existingEntry.HardLinkCounter++

	newBlob, encodeErr := existingEntry.EncodeAttributesAndChunks()
	if encodeErr != nil {
		return encodeErr
	}

	return fsw.KvPut(ctx, key, newBlob)
}

func (fsw *FilerStoreWrapper) maybeReadHardLink(ctx context.Context, entry *Entry) error {
	if entry.HardLinkId == 0 {
		return nil
	}
	key := entry.HardLinkId.Key()

	value, err := fsw.KvGet(ctx, key)
	if err != nil {
		glog.Errorf("read %s hardlink %d: %v", entry.FullPath, entry.HardLinkId, err)
		return err
	}

	if err = entry.DecodeAttributesAndChunks(value); err != nil {
		glog.Errorf("decode %s hardlink %d: %v", entry.FullPath, entry.HardLinkId, err)
		return err
	}

	return nil
}

func (fsw *FilerStoreWrapper) DeleteHardLink(ctx context.Context, hardLinkId HardLinkId) error {
	key := hardLinkId.Key()
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
		return fsw.KvDelete(ctx, key)
	}

	newBlob, encodeErr := entry.EncodeAttributesAndChunks()
	if encodeErr != nil {
		return encodeErr
	}

	return fsw.KvPut(ctx, key, newBlob)

}
