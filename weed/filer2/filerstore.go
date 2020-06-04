package filer2

import (
	"context"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type FilerStore interface {
	// GetName gets the name to locate the configuration in filer.toml file
	GetName() string
	// Initialize initializes the file store
	Initialize(configuration util.Configuration, prefix string) error
	InsertEntry(context.Context, *Entry) error
	UpdateEntry(context.Context, *Entry) (err error)
	// err == filer2.ErrNotFound if not found
	FindEntry(context.Context, util.FullPath) (entry *Entry, err error)
	DeleteEntry(context.Context, util.FullPath) (err error)
	DeleteFolderChildren(context.Context, util.FullPath) (err error)
	ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int) ([]*Entry, error)

	BeginTransaction(ctx context.Context) (context.Context, error)
	CommitTransaction(ctx context.Context) error
	RollbackTransaction(ctx context.Context) error

	Shutdown()
}

type FilerStoreWrapper struct {
	actualStore FilerStore
}

func NewFilerStoreWrapper(store FilerStore) *FilerStoreWrapper {
	if innerStore, ok := store.(*FilerStoreWrapper); ok {
		return innerStore
	}
	return &FilerStoreWrapper{
		actualStore: store,
	}
}

func (fsw *FilerStoreWrapper) GetName() string {
	return fsw.actualStore.GetName()
}

func (fsw *FilerStoreWrapper) Initialize(configuration util.Configuration, prefix string) error {
	return fsw.actualStore.Initialize(configuration, prefix)
}

func (fsw *FilerStoreWrapper) InsertEntry(ctx context.Context, entry *Entry) error {
	stats.FilerStoreCounter.WithLabelValues(fsw.actualStore.GetName(), "insert").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.actualStore.GetName(), "insert").Observe(time.Since(start).Seconds())
	}()

	filer_pb.BeforeEntrySerialization(entry.Chunks)
	return fsw.actualStore.InsertEntry(ctx, entry)
}

func (fsw *FilerStoreWrapper) UpdateEntry(ctx context.Context, entry *Entry) error {
	stats.FilerStoreCounter.WithLabelValues(fsw.actualStore.GetName(), "update").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.actualStore.GetName(), "update").Observe(time.Since(start).Seconds())
	}()

	filer_pb.BeforeEntrySerialization(entry.Chunks)
	return fsw.actualStore.UpdateEntry(ctx, entry)
}

func (fsw *FilerStoreWrapper) FindEntry(ctx context.Context, fp util.FullPath) (entry *Entry, err error) {
	stats.FilerStoreCounter.WithLabelValues(fsw.actualStore.GetName(), "find").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.actualStore.GetName(), "find").Observe(time.Since(start).Seconds())
	}()

	entry, err = fsw.actualStore.FindEntry(ctx, fp)
	if err != nil {
		return nil, err
	}
	filer_pb.AfterEntryDeserialization(entry.Chunks)
	return
}

func (fsw *FilerStoreWrapper) DeleteEntry(ctx context.Context, fp util.FullPath) (err error) {
	stats.FilerStoreCounter.WithLabelValues(fsw.actualStore.GetName(), "delete").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.actualStore.GetName(), "delete").Observe(time.Since(start).Seconds())
	}()

	return fsw.actualStore.DeleteEntry(ctx, fp)
}

func (fsw *FilerStoreWrapper) DeleteFolderChildren(ctx context.Context, fp util.FullPath) (err error) {
	stats.FilerStoreCounter.WithLabelValues(fsw.actualStore.GetName(), "deleteFolderChildren").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.actualStore.GetName(), "deleteFolderChildren").Observe(time.Since(start).Seconds())
	}()

	return fsw.actualStore.DeleteFolderChildren(ctx, fp)
}

func (fsw *FilerStoreWrapper) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int) ([]*Entry, error) {
	stats.FilerStoreCounter.WithLabelValues(fsw.actualStore.GetName(), "list").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.actualStore.GetName(), "list").Observe(time.Since(start).Seconds())
	}()

	entries, err := fsw.actualStore.ListDirectoryEntries(ctx, dirPath, startFileName, includeStartFile, limit)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		filer_pb.AfterEntryDeserialization(entry.Chunks)
	}
	return entries, err
}

func (fsw *FilerStoreWrapper) BeginTransaction(ctx context.Context) (context.Context, error) {
	return fsw.actualStore.BeginTransaction(ctx)
}

func (fsw *FilerStoreWrapper) CommitTransaction(ctx context.Context) error {
	return fsw.actualStore.CommitTransaction(ctx)
}

func (fsw *FilerStoreWrapper) RollbackTransaction(ctx context.Context) error {
	return fsw.actualStore.RollbackTransaction(ctx)
}

func (fsw *FilerStoreWrapper) Shutdown() {
	fsw.actualStore.Shutdown()
}
