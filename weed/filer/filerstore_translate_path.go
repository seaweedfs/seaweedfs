package filer

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"math"
	"strings"
)

var (
	_ = FilerStore(&FilerStorePathTranslator{})
)

type FilerStorePathTranslator struct {
	actualStore FilerStore
	storeRoot   string
}

func NewFilerStorePathTranslator(storeRoot string, store FilerStore) *FilerStorePathTranslator {
	if innerStore, ok := store.(*FilerStorePathTranslator); ok {
		return innerStore
	}

	if !strings.HasSuffix(storeRoot, "/") {
		storeRoot += "/"
	}

	return &FilerStorePathTranslator{
		actualStore: store,
		storeRoot:   storeRoot,
	}
}

func (t *FilerStorePathTranslator) translatePath(fp util.FullPath) (newPath util.FullPath) {
	newPath = fp
	if t.storeRoot == "/" {
		return
	}
	newPath = fp[len(t.storeRoot)-1:]
	if newPath == "" {
		newPath = "/"
	}
	return
}
func (t *FilerStorePathTranslator) changeEntryPath(entry *Entry) (previousPath util.FullPath) {
	previousPath = entry.FullPath
	if t.storeRoot == "/" {
		return
	}
	entry.FullPath = t.translatePath(previousPath)
	return
}
func (t *FilerStorePathTranslator) recoverEntryPath(entry *Entry, previousPath util.FullPath) {
	entry.FullPath = previousPath
}

func (t *FilerStorePathTranslator) GetName() string {
	return t.actualStore.GetName()
}

func (t *FilerStorePathTranslator) Initialize(configuration util.Configuration, prefix string) error {
	return t.actualStore.Initialize(configuration, prefix)
}

func (t *FilerStorePathTranslator) InsertEntry(ctx context.Context, entry *Entry) error {
	previousPath := t.changeEntryPath(entry)
	defer t.recoverEntryPath(entry, previousPath)

	return t.actualStore.InsertEntry(ctx, entry)
}

func (t *FilerStorePathTranslator) UpdateEntry(ctx context.Context, entry *Entry) error {
	previousPath := t.changeEntryPath(entry)
	defer t.recoverEntryPath(entry, previousPath)

	return t.actualStore.UpdateEntry(ctx, entry)
}

func (t *FilerStorePathTranslator) FindEntry(ctx context.Context, fp util.FullPath) (entry *Entry, err error) {
	if t.storeRoot == "/" {
		return t.actualStore.FindEntry(ctx, fp)
	}
	newFullPath := t.translatePath(fp)
	entry, err = t.actualStore.FindEntry(ctx, newFullPath)
	if err == nil {
		entry.FullPath = fp[:len(t.storeRoot)-1] + entry.FullPath
	}
	return
}

func (t *FilerStorePathTranslator) DeleteEntry(ctx context.Context, fp util.FullPath) (err error) {
	newFullPath := t.translatePath(fp)
	return t.actualStore.DeleteEntry(ctx, newFullPath)
}

func (t *FilerStorePathTranslator) DeleteOneEntry(ctx context.Context, existingEntry *Entry) (err error) {

	previousPath := t.changeEntryPath(existingEntry)
	defer t.recoverEntryPath(existingEntry, previousPath)

	return t.actualStore.DeleteEntry(ctx, existingEntry.FullPath)
}

func (t *FilerStorePathTranslator) DeleteFolderChildren(ctx context.Context, fp util.FullPath) (err error) {
	newFullPath := t.translatePath(fp)

	return t.actualStore.DeleteFolderChildren(ctx, newFullPath)
}

func (t *FilerStorePathTranslator) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc ListEachEntryFunc) (string, error) {

	newFullPath := t.translatePath(dirPath)

	return t.actualStore.ListDirectoryEntries(ctx, newFullPath, startFileName, includeStartFile, limit, func(entry *Entry) bool {
		entry.FullPath = dirPath[:len(t.storeRoot)-1] + entry.FullPath
		return eachEntryFunc(entry)
	})
}

func (t *FilerStorePathTranslator) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc ListEachEntryFunc) (string, error) {

	newFullPath := t.translatePath(dirPath)

	if limit > math.MaxInt32-1 {
		limit = math.MaxInt32 - 1
	}

	return t.actualStore.ListDirectoryPrefixedEntries(ctx, newFullPath, startFileName, includeStartFile, limit, prefix, func(entry *Entry) bool {
		entry.FullPath = dirPath[:len(t.storeRoot)-1] + entry.FullPath
		return eachEntryFunc(entry)
	})
}

func (t *FilerStorePathTranslator) BeginTransaction(ctx context.Context) (context.Context, error) {
	return t.actualStore.BeginTransaction(ctx)
}

func (t *FilerStorePathTranslator) CommitTransaction(ctx context.Context) error {
	return t.actualStore.CommitTransaction(ctx)
}

func (t *FilerStorePathTranslator) RollbackTransaction(ctx context.Context) error {
	return t.actualStore.RollbackTransaction(ctx)
}

func (t *FilerStorePathTranslator) Shutdown() {
	t.actualStore.Shutdown()
}

func (t *FilerStorePathTranslator) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	return t.actualStore.KvPut(ctx, key, value)
}
func (t *FilerStorePathTranslator) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	return t.actualStore.KvGet(ctx, key)
}
func (t *FilerStorePathTranslator) KvDelete(ctx context.Context, key []byte) (err error) {
	return t.actualStore.KvDelete(ctx, key)
}
