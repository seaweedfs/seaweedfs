package filer

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/util"
	"strings"
)

var (
	_ = FilerStore(&FilerStorePathTranlator{})
)

type FilerStorePathTranlator struct {
	actualStore FilerStore
	storeRoot   string
}

func NewFilerStorePathTranlator(storeRoot string, store FilerStore) *FilerStorePathTranlator {
	if innerStore, ok := store.(*FilerStorePathTranlator); ok {
		return innerStore
	}

	if !strings.HasSuffix(storeRoot, "/") {
		storeRoot += "/"
	}

	return &FilerStorePathTranlator{
		actualStore: store,
		storeRoot:   storeRoot,
	}
}

func (t *FilerStorePathTranlator) changeEntryPath(entry *Entry) (previousPath util.FullPath){
	previousPath = entry.FullPath
	if t.storeRoot == "/" {
		return
	}
	entry.FullPath = previousPath[len(t.storeRoot)-1:]
	return
}
func (t *FilerStorePathTranlator) recoverEntryPath(entry *Entry, previousPath util.FullPath) {
	entry.FullPath = previousPath
}

func (t *FilerStorePathTranlator) GetName() string {
	return t.actualStore.GetName()
}

func (t *FilerStorePathTranlator) Initialize(configuration util.Configuration, prefix string) error {
	return t.actualStore.Initialize(configuration, prefix)
}

func (t *FilerStorePathTranlator) InsertEntry(ctx context.Context, entry *Entry) error {
	previousPath := t.changeEntryPath(entry)
	defer t.recoverEntryPath(entry, previousPath)

	return t.actualStore.InsertEntry(ctx, entry)
}

func (t *FilerStorePathTranlator) UpdateEntry(ctx context.Context, entry *Entry) error {
	previousPath := t.changeEntryPath(entry)
	defer t.recoverEntryPath(entry, previousPath)

	return t.actualStore.UpdateEntry(ctx, entry)
}

func (t *FilerStorePathTranlator) FindEntry(ctx context.Context, fp util.FullPath) (entry *Entry, err error) {
	if t.storeRoot == "/" {
		return t.actualStore.FindEntry(ctx, fp)
	}
	newFullPath := fp[len(t.storeRoot)-1:]
	entry, err = t.actualStore.FindEntry(ctx, newFullPath)
	if err == nil {
		entry.FullPath = fp[:len(t.storeRoot)-1] + entry.FullPath
	}
	return
}

func (t *FilerStorePathTranlator) DeleteEntry(ctx context.Context, fp util.FullPath) (err error) {
	newFullPath := fp[len(t.storeRoot)-1:]
	return t.actualStore.DeleteEntry(ctx, newFullPath)
}

func (t *FilerStorePathTranlator) DeleteOneEntry(ctx context.Context, existingEntry *Entry) (err error) {

	previousPath := t.changeEntryPath(existingEntry)
	defer t.recoverEntryPath(existingEntry, previousPath)

	return t.actualStore.DeleteEntry(ctx, existingEntry.FullPath)
}

func (t *FilerStorePathTranlator) DeleteFolderChildren(ctx context.Context, fp util.FullPath) (err error) {
	newFullPath := fp[len(t.storeRoot)-1:]

	return t.actualStore.DeleteFolderChildren(ctx, newFullPath)
}

func (t *FilerStorePathTranlator) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int) ([]*Entry, error) {

	newFullPath := dirPath[len(t.storeRoot)-1:]

	entries, err := t.actualStore.ListDirectoryEntries(ctx, newFullPath, startFileName, includeStartFile, limit)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		entry.FullPath = dirPath[:len(t.storeRoot)-1] + entry.FullPath
	}
	return entries, err
}

func (t *FilerStorePathTranlator) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int, prefix string) ([]*Entry, error) {

	newFullPath := dirPath[len(t.storeRoot)-1:]

	entries, err := t.actualStore.ListDirectoryPrefixedEntries(ctx, newFullPath, startFileName, includeStartFile, limit, prefix)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		entry.FullPath = dirPath[:len(t.storeRoot)-1] + entry.FullPath
	}
	return entries, nil
}

func (t *FilerStorePathTranlator) BeginTransaction(ctx context.Context) (context.Context, error) {
	return t.actualStore.BeginTransaction(ctx)
}

func (t *FilerStorePathTranlator) CommitTransaction(ctx context.Context) error {
	return t.actualStore.CommitTransaction(ctx)
}

func (t *FilerStorePathTranlator) RollbackTransaction(ctx context.Context) error {
	return t.actualStore.RollbackTransaction(ctx)
}

func (t *FilerStorePathTranlator) Shutdown() {
	t.actualStore.Shutdown()
}

func (t *FilerStorePathTranlator) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	return t.actualStore.KvPut(ctx, key, value)
}
func (t *FilerStorePathTranlator) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	return t.actualStore.KvGet(ctx, key)
}
func (t *FilerStorePathTranlator) KvDelete(ctx context.Context, key []byte) (err error) {
	return t.actualStore.KvDelete(ctx, key)
}
