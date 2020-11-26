package filer

import (
	"context"
	"errors"
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

	if err := fsw.handleUpdateToHardLinks(ctx, entry); err != nil {
		return err
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

	if err := fsw.handleUpdateToHardLinks(ctx, entry); err != nil {
		return err
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
	if len(existingEntry.HardLinkId) != 0 {
		// remove hard link
		if err = fsw.DeleteHardLink(ctx, existingEntry.HardLinkId); err != nil {
			return err
		}
	}

	return fsw.ActualStore.DeleteEntry(ctx, fp)
}

func (fsw *FilerStoreWrapper) DeleteOneEntry(ctx context.Context, existingEntry *Entry) (err error) {
	stats.FilerStoreCounter.WithLabelValues(fsw.ActualStore.GetName(), "delete").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.ActualStore.GetName(), "delete").Observe(time.Since(start).Seconds())
	}()

	if len(existingEntry.HardLinkId) != 0 {
		// remove hard link
		if err = fsw.DeleteHardLink(ctx, existingEntry.HardLinkId); err != nil {
			return err
		}
	}

	return fsw.ActualStore.DeleteEntry(ctx, existingEntry.FullPath)
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
