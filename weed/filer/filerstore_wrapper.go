package filer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/viant/ptrie"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	_ = VirtualFilerStore(&FilerStoreWrapper{})
	_ = Debuggable(&FilerStoreWrapper{})
	_ = EntryScanner(&FilerStoreWrapper{})
)

type VirtualFilerStore interface {
	FilerStore
	DeleteHardLink(ctx context.Context, hardLinkId HardLinkId) error
	DeleteOneEntry(ctx context.Context, entry *Entry) error
	InsertEntryKnownAbsent(ctx context.Context, entry *Entry) error
	AddPathSpecificStore(path string, storeId string, store FilerStore)
	OnBucketCreation(bucket string)
	OnBucketDeletion(bucket string)
	CanDropWholeBucket() bool
	SameActualStore(a, b util.FullPath) bool
}

type FilerStoreWrapper struct {
	defaultStore         FilerStore
	pathToStore          ptrie.Trie[string]
	storeIdToStore       map[string]FilerStore
	hasPathSpecificStore bool // fast check to skip MatchPrefix when no path-specific stores
}

func NewFilerStoreWrapper(store FilerStore) *FilerStoreWrapper {
	if innerStore, ok := store.(*FilerStoreWrapper); ok {
		return innerStore
	}
	return &FilerStoreWrapper{
		defaultStore:   store,
		pathToStore:    ptrie.New[string](),
		storeIdToStore: make(map[string]FilerStore),
	}
}

func (fsw *FilerStoreWrapper) CanDropWholeBucket() bool {
	if ba, ok := fsw.defaultStore.(BucketAware); ok {
		return ba.CanDropWholeBucket()
	}
	return false
}

func (fsw *FilerStoreWrapper) OnBucketCreation(bucket string) {
	for _, store := range fsw.storeIdToStore {
		if ba, ok := store.(BucketAware); ok {
			ba.OnBucketCreation(bucket)
		}
	}
	if ba, ok := fsw.defaultStore.(BucketAware); ok {
		ba.OnBucketCreation(bucket)
	}
}
func (fsw *FilerStoreWrapper) OnBucketDeletion(bucket string) {
	for _, store := range fsw.storeIdToStore {
		if ba, ok := store.(BucketAware); ok {
			ba.OnBucketDeletion(bucket)
		}
	}
	if ba, ok := fsw.defaultStore.(BucketAware); ok {
		ba.OnBucketDeletion(bucket)
	}
}

func (fsw *FilerStoreWrapper) AddPathSpecificStore(path string, storeId string, store FilerStore) {
	fsw.storeIdToStore[storeId] = NewFilerStorePathTranslator(path, store)
	err := fsw.pathToStore.Put([]byte(path), storeId)
	if err != nil {
		glog.Fatalf("put path specific store: %v", err)
	}
	fsw.hasPathSpecificStore = true
}

func (fsw *FilerStoreWrapper) getActualStore(path util.FullPath) (store FilerStore) {
	store = fsw.defaultStore
	// Fast path: skip MatchPrefix if no path-specific stores are configured (common case)
	if !fsw.hasPathSpecificStore {
		return
	}
	if path == "/" || path == "//" {
		return
	}
	var storeId string
	fsw.pathToStore.MatchPrefix([]byte(path), func(key []byte, value string) bool {
		storeId = value
		return false
	})
	if storeId != "" {
		store = fsw.storeIdToStore[storeId]
	}
	return
}

// listsInNameOrder reports whether a listing of dirPath can start at a file
// name the store did not hand out itself.
func (fsw *FilerStoreWrapper) listsInNameOrder(dirPath util.FullPath) bool {
	store := fsw.getActualStore(dirPath + "/")
	if t, ok := store.(*FilerStorePathTranslator); ok {
		store = t.actualStore
	}
	u, ok := store.(UnorderedListingStore)
	return !ok || !u.ListsUnordered()
}

// SameActualStore reports whether two paths resolve to the same underlying
// store. When path-specific stores are configured, different subtrees may
// be served by different backends.
func (fsw *FilerStoreWrapper) SameActualStore(a, b util.FullPath) bool {
	return fsw.getActualStore(a) == fsw.getActualStore(b)
}

func (fsw *FilerStoreWrapper) getDefaultStore() (store FilerStore) {
	return fsw.defaultStore
}

func (fsw *FilerStoreWrapper) GetName() string {
	return fsw.getDefaultStore().GetName()
}

func (fsw *FilerStoreWrapper) Initialize(configuration util.Configuration, prefix string) error {
	return fsw.getDefaultStore().Initialize(configuration, prefix)
}

func (fsw *FilerStoreWrapper) InsertEntry(ctx context.Context, entry *Entry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx = context.WithoutCancel(ctx)
	actualStore := fsw.getActualStore(entry.FullPath)
	stats.FilerStoreCounter.WithLabelValues(actualStore.GetName(), "insert").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(actualStore.GetName(), "insert").Observe(time.Since(start).Seconds())
	}()

	filer_pb.BeforeEntrySerialization(entry.GetChunks())
	normalizeEntryMimeForStore(entry)
	delete(entry.Extended, ExtKeepRemoteObjectKey)

	if len(entry.HardLinkId) > 0 {
		glog.V(4).InfofCtx(ctx, "InsertEntry %s has HardLinkId %x counter=%d",
			entry.FullPath, entry.HardLinkId, entry.HardLinkCounter)
	}

	replacedLink, err := fsw.handleUpdateToHardLinks(ctx, entry)
	if err != nil {
		return err
	}

	if err := actualStore.InsertEntry(ctx, entry); err != nil {
		return err
	}
	fsw.dropLink(ctx, entry.FullPath, replacedLink)
	return nil
}

// InsertEntryKnownAbsent skips the pre-insert FindEntry path when the caller has
// already established that the target path does not exist.
func (fsw *FilerStoreWrapper) InsertEntryKnownAbsent(ctx context.Context, entry *Entry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx = context.WithoutCancel(ctx)
	actualStore := fsw.getActualStore(entry.FullPath)
	stats.FilerStoreCounter.WithLabelValues(actualStore.GetName(), "insert").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(actualStore.GetName(), "insert").Observe(time.Since(start).Seconds())
	}()

	filer_pb.BeforeEntrySerialization(entry.GetChunks())
	normalizeEntryMimeForStore(entry)
	delete(entry.Extended, ExtKeepRemoteObjectKey)

	if len(entry.HardLinkId) > 0 {
		glog.V(4).InfofCtx(ctx, "InsertEntryKnownAbsent %s has HardLinkId %x counter=%d",
			entry.FullPath, entry.HardLinkId, entry.HardLinkCounter)
		if err := fsw.setHardLink(ctx, entry); err != nil {
			return fmt.Errorf("setHardLink %x: %v", entry.HardLinkId, err)
		}
	}

	return actualStore.InsertEntry(ctx, entry)
}

func (fsw *FilerStoreWrapper) UpdateEntry(ctx context.Context, entry *Entry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx = context.WithoutCancel(ctx)
	actualStore := fsw.getActualStore(entry.FullPath)
	stats.FilerStoreCounter.WithLabelValues(actualStore.GetName(), "update").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(actualStore.GetName(), "update").Observe(time.Since(start).Seconds())
	}()

	filer_pb.BeforeEntrySerialization(entry.GetChunks())
	normalizeEntryMimeForStore(entry)
	delete(entry.Extended, ExtKeepRemoteObjectKey)

	if len(entry.HardLinkId) > 0 {
		glog.V(4).InfofCtx(ctx, "UpdateEntry %s has HardLinkId %x counter=%d",
			entry.FullPath, entry.HardLinkId, entry.HardLinkCounter)
	}

	replacedLink, err := fsw.handleUpdateToHardLinks(ctx, entry)
	if err != nil {
		return err
	}

	if err := actualStore.UpdateEntry(ctx, entry); err != nil {
		return err
	}
	fsw.dropLink(ctx, entry.FullPath, replacedLink)
	return nil
}

func normalizeEntryMimeForStore(entry *Entry) {
	if entry == nil {
		return
	}
	if !entry.IsDirectory() && entry.Mime == "application/octet-stream" {
		entry.Mime = ""
	}
}

func (fsw *FilerStoreWrapper) FindEntry(ctx context.Context, fp util.FullPath) (entry *Entry, err error) {
	ctx = context.WithoutCancel(ctx)
	actualStore := fsw.getActualStore(fp)
	stats.FilerStoreCounter.WithLabelValues(actualStore.GetName(), "find").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(actualStore.GetName(), "find").Observe(time.Since(start).Seconds())
	}()

	entry, err = actualStore.FindEntry(ctx, fp)
	// glog.V(4).Infof("FindEntry %s: %v", fp, err)
	if err != nil {
		if fsw.CanDropWholeBucket() && strings.Contains(err.Error(), "Table") && strings.Contains(err.Error(), "doesn't exist") {
			err = filer_pb.ErrNotFound
		}
		return nil, err
	}

	fsw.maybeReadHardLink(ctx, entry)

	filer_pb.AfterEntryDeserialization(entry.GetChunks())
	return
}

func (fsw *FilerStoreWrapper) DeleteEntry(ctx context.Context, fp util.FullPath) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx = context.WithoutCancel(ctx)
	actualStore := fsw.getActualStore(fp)
	stats.FilerStoreCounter.WithLabelValues(actualStore.GetName(), "delete").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(actualStore.GetName(), "delete").Observe(time.Since(start).Seconds())
	}()

	existingEntry, findErr := fsw.FindEntry(ctx, fp)
	if findErr != nil && !errors.Is(findErr, filer_pb.ErrNotFound) {
		return findErr
	}
	if existingEntry == nil {
		return nil
	}
	if err := actualStore.DeleteEntry(ctx, fp); err != nil {
		return err
	}
	if ctx.Value("OP") != "MV" {
		// a rename takes the name, link and all, elsewhere
		fsw.dropLink(ctx, fp, existingEntry.HardLinkId)
	}
	return nil
}

func (fsw *FilerStoreWrapper) DeleteOneEntry(ctx context.Context, existingEntry *Entry) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx = context.WithoutCancel(ctx)
	actualStore := fsw.getActualStore(existingEntry.FullPath)
	stats.FilerStoreCounter.WithLabelValues(actualStore.GetName(), "delete").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(actualStore.GetName(), "delete").Observe(time.Since(start).Seconds())
	}()

	if err := actualStore.DeleteEntry(ctx, existingEntry.FullPath); err != nil {
		return err
	}
	if ctx.Value("OP") != "MV" {
		// a rename takes the name, link and all, elsewhere
		fsw.dropLink(ctx, existingEntry.FullPath, existingEntry.HardLinkId)
	}
	return nil
}

func (fsw *FilerStoreWrapper) DeleteFolderChildren(ctx context.Context, fp util.FullPath) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx = context.WithoutCancel(ctx)
	actualStore := fsw.getActualStore(fp + "/")
	stats.FilerStoreCounter.WithLabelValues(actualStore.GetName(), "deleteFolderChildren").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(actualStore.GetName(), "deleteFolderChildren").Observe(time.Since(start).Seconds())
	}()

	return actualStore.DeleteFolderChildren(ctx, fp)
}

func (fsw *FilerStoreWrapper) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc ListEachEntryFunc) (string, error) {
	ctx = context.WithoutCancel(ctx)
	actualStore := fsw.getActualStore(dirPath + "/")
	stats.FilerStoreCounter.WithLabelValues(actualStore.GetName(), "list").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(actualStore.GetName(), "list").Observe(time.Since(start).Seconds())
	}()

	// glog.V(4).Infof("ListDirectoryEntries %s from %s limit %d", dirPath, startFileName, limit)
	return actualStore.ListDirectoryEntries(ctx, dirPath, startFileName, includeStartFile, limit, func(entry *Entry) (bool, error) {
		fsw.maybeReadHardLink(ctx, entry)
		filer_pb.AfterEntryDeserialization(entry.GetChunks())
		return eachEntryFunc(entry)
	})
}

func (fsw *FilerStoreWrapper) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc ListEachEntryFunc) (lastFileName string, err error) {
	ctx = context.WithoutCancel(ctx)
	actualStore := fsw.getActualStore(dirPath + "/")
	stats.FilerStoreCounter.WithLabelValues(actualStore.GetName(), "prefixList").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(actualStore.GetName(), "prefixList").Observe(time.Since(start).Seconds())
	}()
	if limit > math.MaxInt32-1 {
		limit = math.MaxInt32 - 1
	}
	// glog.V(4).Infof("ListDirectoryPrefixedEntries %s from %s prefix %s limit %d", dirPath, startFileName, prefix, limit)
	adjustedEntryFunc := func(entry *Entry) (bool, error) {
		fsw.maybeReadHardLink(ctx, entry)
		filer_pb.AfterEntryDeserialization(entry.GetChunks())
		return eachEntryFunc(entry)
	}
	lastFileName, err = actualStore.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, prefix, adjustedEntryFunc)
	if err == ErrUnsupportedListDirectoryPrefixed {
		lastFileName, err = fsw.prefixFilterEntries(ctx, dirPath, startFileName, includeStartFile, limit, prefix, adjustedEntryFunc)
	}
	return lastFileName, err
}

// ScanEntries delegates to the default store. With path-specific stores
// configured, entries below dirPath could live in several stores and one
// snapshot per store is not one snapshot, so the scan is declined. Hard links
// are resolved inside the snapshot; a store error fails the scan (the per-path
// copy can be stale), a missing record falls back to it as listings do.
func (fsw *FilerStoreWrapper) ScanEntries(ctx context.Context, dirPath util.FullPath, fn func(ctx context.Context, entry *Entry) error) error {
	if fsw.hasPathSpecificStore {
		return ErrUnsupportedEntryScan
	}
	scanner, ok := fsw.defaultStore.(EntryScanner)
	if !ok {
		return ErrUnsupportedEntryScan
	}
	stats.FilerStoreCounter.WithLabelValues(fsw.defaultStore.GetName(), "scan").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(fsw.defaultStore.GetName(), "scan").Observe(time.Since(start).Seconds())
	}()
	return scanner.ScanEntries(ctx, dirPath, func(scanCtx context.Context, entry *Entry) error {
		if err := fsw.maybeReadHardLink(scanCtx, entry); err != nil && !errors.Is(err, ErrKvNotFound) {
			return fmt.Errorf("resolve hard link of %s: %w", entry.FullPath, err)
		}
		filer_pb.AfterEntryDeserialization(entry.GetChunks())
		return fn(scanCtx, entry)
	})
}

func (fsw *FilerStoreWrapper) prefixFilterEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc ListEachEntryFunc) (lastFileName string, err error) {
	actualStore := fsw.getActualStore(dirPath + "/")

	if prefix == "" {
		return actualStore.ListDirectoryEntries(ctx, dirPath, startFileName, includeStartFile, limit, eachEntryFunc)
	}

	// lastFileName must stay at the last name delivered to eachEntryFunc; the
	// raw scan cursor runs ahead over non-matching names and cannot be returned.
	count := int64(0)
	scanName, scanInclusive := startFileName, includeStartFile
	for count < limit {
		var page []*Entry
		if scanName, err = actualStore.ListDirectoryEntries(ctx, dirPath, scanName, scanInclusive, limit, func(entry *Entry) (bool, error) {
			page = append(page, entry)
			return true, nil
		}); err != nil {
			return
		}
		scanInclusive = false
		if len(page) == 0 {
			return
		}
		for _, entry := range page {
			if !strings.HasPrefix(entry.Name(), prefix) {
				continue
			}
			count++
			res, resErr := eachEntryFunc(entry)
			if resErr != nil {
				err = fmt.Errorf("failed to process eachEntryFunc for entry %q: %w", entry.Name(), resErr)
				return
			}
			lastFileName = entry.Name()
			if !res || count >= limit {
				return
			}
		}
		if scanName >= prefix && !strings.HasPrefix(scanName, prefix) {
			return
		}
	}
	return
}

// transactionCovers reports whether a transaction of the default store covers
// the writes to the entries at paths: the store's transactions are real
// (IsolatingTransactionStore) and it holds those entries. The writes of a
// path-specific store bypass the default store's transaction. An entry in the
// default store has its parents there too.
func (fsw *FilerStoreWrapper) transactionCovers(paths ...util.FullPath) bool {
	isolating, ok := fsw.defaultStore.(IsolatingTransactionStore)
	if !ok || !isolating.IsolatesTransactions() {
		return false
	}
	for _, p := range paths {
		if fsw.getActualStore(p) != fsw.defaultStore {
			return false
		}
	}
	return true
}

// refusesConflictingCommits reports whether a transaction of the default store
// that read and wrote the entry at path is refused when another writer
// committed that entry meanwhile: the store's commits are (ConflictRefusingStore)
// and it holds the entry.
func (fsw *FilerStoreWrapper) refusesConflictingCommits(path util.FullPath) bool {
	refusing, ok := fsw.defaultStore.(ConflictRefusingStore)
	return ok && refusing.RefusesConflictingCommits() && fsw.getActualStore(path) == fsw.defaultStore
}

// overwritesAtOnce reports whether an entry inserted at path replaces what is
// stored there at once and whole: the default store says that its inserts do
// (OverwritingStore), and it holds the entry.
func (fsw *FilerStoreWrapper) overwritesAtOnce(path util.FullPath) bool {
	overwriting, ok := fsw.defaultStore.(OverwritingStore)
	return ok && overwriting.OverwritesAtOnce() && fsw.getActualStore(path) == fsw.defaultStore
}

func (fsw *FilerStoreWrapper) BeginTransaction(ctx context.Context) (context.Context, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx = context.WithoutCancel(ctx)
	return fsw.getDefaultStore().BeginTransaction(ctx)
}

func (fsw *FilerStoreWrapper) CommitTransaction(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx = context.WithoutCancel(ctx)
	return fsw.getDefaultStore().CommitTransaction(ctx)
}

func (fsw *FilerStoreWrapper) RollbackTransaction(ctx context.Context) error {
	ctx = context.WithoutCancel(ctx)
	return fsw.getDefaultStore().RollbackTransaction(ctx)
}

func (fsw *FilerStoreWrapper) Shutdown() {
	fsw.getDefaultStore().Shutdown()
}

func (fsw *FilerStoreWrapper) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx = context.WithoutCancel(ctx)
	return fsw.getDefaultStore().KvPut(ctx, key, value)
}
func (fsw *FilerStoreWrapper) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	ctx = context.WithoutCancel(ctx)
	return fsw.getDefaultStore().KvGet(ctx, key)
}
func (fsw *FilerStoreWrapper) KvDelete(ctx context.Context, key []byte) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx = context.WithoutCancel(ctx)
	return fsw.getDefaultStore().KvDelete(ctx, key)
}

func (fsw *FilerStoreWrapper) Debug(writer io.Writer) {
	if debuggable, ok := fsw.getDefaultStore().(Debuggable); ok {
		debuggable.Debug(writer)
	}
}
