package filer

import (
	"context"
	"errors"
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

	_ = VirtualFilerStore(&FilerStoreWrapper{})
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
	DeleteOneEntry(ctx context.Context, entry *Entry) error
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

func (fsw *FilerStoreWrapper) getActualStore(path string) FilerStore {
	return fsw.ActualStore
}

func (fsw *FilerStoreWrapper) GetName() string {
	return fsw.getActualStore("").GetName()
}

func (fsw *FilerStoreWrapper) Initialize(configuration util.Configuration, prefix string) error {
	return fsw.getActualStore("").Initialize(configuration, prefix)
}

func (fsw *FilerStoreWrapper) InsertEntry(ctx context.Context, entry *Entry) error {
	stats.FilerStoreCounter.WithLabelValues(fsw.getActualStore("").GetName(), "insert").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.getActualStore("").GetName(), "insert").Observe(time.Since(start).Seconds())
	}()

	filer_pb.BeforeEntrySerialization(entry.Chunks)
	if entry.Mime == "application/octet-stream" {
		entry.Mime = ""
	}

	if err := fsw.handleUpdateToHardLinks(ctx, entry); err != nil {
		return err
	}

	glog.V(4).Infof("InsertEntry %s", entry.FullPath)
	return fsw.getActualStore("").InsertEntry(ctx, entry)
}

func (fsw *FilerStoreWrapper) UpdateEntry(ctx context.Context, entry *Entry) error {
	stats.FilerStoreCounter.WithLabelValues(fsw.getActualStore("").GetName(), "update").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.getActualStore("").GetName(), "update").Observe(time.Since(start).Seconds())
	}()

	filer_pb.BeforeEntrySerialization(entry.Chunks)
	if entry.Mime == "application/octet-stream" {
		entry.Mime = ""
	}

	if err := fsw.handleUpdateToHardLinks(ctx, entry); err != nil {
		return err
	}

	glog.V(4).Infof("UpdateEntry %s", entry.FullPath)
	return fsw.getActualStore("").UpdateEntry(ctx, entry)
}

func (fsw *FilerStoreWrapper) FindEntry(ctx context.Context, fp util.FullPath) (entry *Entry, err error) {
	stats.FilerStoreCounter.WithLabelValues(fsw.getActualStore("").GetName(), "find").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.getActualStore("").GetName(), "find").Observe(time.Since(start).Seconds())
	}()

	glog.V(4).Infof("FindEntry %s", fp)
	entry, err = fsw.getActualStore("").FindEntry(ctx, fp)
	if err != nil {
		return nil, err
	}

	fsw.maybeReadHardLink(ctx, entry)

	filer_pb.AfterEntryDeserialization(entry.Chunks)
	return
}

func (fsw *FilerStoreWrapper) DeleteEntry(ctx context.Context, fp util.FullPath) (err error) {
	stats.FilerStoreCounter.WithLabelValues(fsw.getActualStore("").GetName(), "delete").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.getActualStore("").GetName(), "delete").Observe(time.Since(start).Seconds())
	}()

	existingEntry, findErr := fsw.FindEntry(ctx, fp)
	if findErr == filer_pb.ErrNotFound {
		return nil
	}
	if len(existingEntry.HardLinkId) != 0 {
		// remove hard link
		glog.V(4).Infof("DeleteHardLink %s", existingEntry.FullPath)
		if err = fsw.DeleteHardLink(ctx, existingEntry.HardLinkId); err != nil {
			return err
		}
	}

	glog.V(4).Infof("DeleteEntry %s", fp)
	return fsw.getActualStore("").DeleteEntry(ctx, fp)
}

func (fsw *FilerStoreWrapper) DeleteOneEntry(ctx context.Context, existingEntry *Entry) (err error) {
	stats.FilerStoreCounter.WithLabelValues(fsw.getActualStore("").GetName(), "delete").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.getActualStore("").GetName(), "delete").Observe(time.Since(start).Seconds())
	}()

	if len(existingEntry.HardLinkId) != 0 {
		// remove hard link
		glog.V(4).Infof("DeleteHardLink %s", existingEntry.FullPath)
		if err = fsw.DeleteHardLink(ctx, existingEntry.HardLinkId); err != nil {
			return err
		}
	}

	glog.V(4).Infof("DeleteOneEntry %s", existingEntry.FullPath)
	return fsw.getActualStore("").DeleteEntry(ctx, existingEntry.FullPath)
}

func (fsw *FilerStoreWrapper) DeleteFolderChildren(ctx context.Context, fp util.FullPath) (err error) {
	stats.FilerStoreCounter.WithLabelValues(fsw.getActualStore("").GetName(), "deleteFolderChildren").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.getActualStore("").GetName(), "deleteFolderChildren").Observe(time.Since(start).Seconds())
	}()

	glog.V(4).Infof("DeleteFolderChildren %s", fp)
	return fsw.getActualStore("").DeleteFolderChildren(ctx, fp)
}

func (fsw *FilerStoreWrapper) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int) ([]*Entry, error) {
	stats.FilerStoreCounter.WithLabelValues(fsw.getActualStore("").GetName(), "list").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.getActualStore("").GetName(), "list").Observe(time.Since(start).Seconds())
	}()

	glog.V(4).Infof("ListDirectoryEntries %s from %s limit %d", dirPath, startFileName, limit)
	entries, err := fsw.getActualStore("").ListDirectoryEntries(ctx, dirPath, startFileName, includeStartFile, limit)
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
	stats.FilerStoreCounter.WithLabelValues(fsw.getActualStore("").GetName(), "prefixList").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.getActualStore("").GetName(), "prefixList").Observe(time.Since(start).Seconds())
	}()
	glog.V(4).Infof("ListDirectoryPrefixedEntries %s from %s prefix %s limit %d", dirPath, startFileName, prefix, limit)
	entries, err := fsw.getActualStore("").ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, prefix)
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
	entries, err = fsw.getActualStore("").ListDirectoryEntries(ctx, dirPath, startFileName, includeStartFile, limit)
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
			notPrefixed, err = fsw.getActualStore("").ListDirectoryEntries(ctx, dirPath, lastFileName, false, limit)
			if err != nil {
				return
			}
		}
	}
	return
}

func (fsw *FilerStoreWrapper) BeginTransaction(ctx context.Context) (context.Context, error) {
	return fsw.getActualStore("").BeginTransaction(ctx)
}

func (fsw *FilerStoreWrapper) CommitTransaction(ctx context.Context) error {
	return fsw.getActualStore("").CommitTransaction(ctx)
}

func (fsw *FilerStoreWrapper) RollbackTransaction(ctx context.Context) error {
	return fsw.getActualStore("").RollbackTransaction(ctx)
}

func (fsw *FilerStoreWrapper) Shutdown() {
	fsw.getActualStore("").Shutdown()
}

func (fsw *FilerStoreWrapper) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	return fsw.getActualStore("").KvPut(ctx, key, value)
}
func (fsw *FilerStoreWrapper) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	return fsw.getActualStore("").KvGet(ctx, key)
}
func (fsw *FilerStoreWrapper) KvDelete(ctx context.Context, key []byte) (err error) {
	return fsw.getActualStore("").KvDelete(ctx, key)
}
