package filer

import (
	"context"
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
)

type VirtualFilerStore interface {
	FilerStore
	DeleteHardLink(ctx context.Context, hardLinkId HardLinkId) error
	DeleteOneEntry(ctx context.Context, entry *Entry) error
	AddPathSpecificStore(path string, storeId string, store FilerStore)
	OnBucketCreation(bucket string)
	OnBucketDeletion(bucket string)
	CanDropWholeBucket() bool
}

type FilerStoreWrapper struct {
	defaultStore   FilerStore
	pathToStore    ptrie.Trie[string]
	storeIdToStore map[string]FilerStore
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
}

func (fsw *FilerStoreWrapper) getActualStore(path util.FullPath) (store FilerStore) {
	store = fsw.defaultStore
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
	actualStore := fsw.getActualStore(entry.FullPath)
	stats.FilerStoreCounter.WithLabelValues(actualStore.GetName(), "insert").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(actualStore.GetName(), "insert").Observe(time.Since(start).Seconds())
	}()

	filer_pb.BeforeEntrySerialization(entry.GetChunks())
	if entry.Mime == "application/octet-stream" {
		entry.Mime = ""
	}

	if err := fsw.handleUpdateToHardLinks(ctx, entry); err != nil {
		return err
	}

	// glog.V(4).Infof("InsertEntry %s", entry.FullPath)
	return actualStore.InsertEntry(ctx, entry)
}

func (fsw *FilerStoreWrapper) UpdateEntry(ctx context.Context, entry *Entry) error {
	actualStore := fsw.getActualStore(entry.FullPath)
	stats.FilerStoreCounter.WithLabelValues(actualStore.GetName(), "update").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(actualStore.GetName(), "update").Observe(time.Since(start).Seconds())
	}()

	filer_pb.BeforeEntrySerialization(entry.GetChunks())
	if entry.Mime == "application/octet-stream" {
		entry.Mime = ""
	}

	if err := fsw.handleUpdateToHardLinks(ctx, entry); err != nil {
		return err
	}

	// glog.V(4).Infof("UpdateEntry %s", entry.FullPath)
	return actualStore.UpdateEntry(ctx, entry)
}

func (fsw *FilerStoreWrapper) FindEntry(ctx context.Context, fp util.FullPath) (entry *Entry, err error) {
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
	actualStore := fsw.getActualStore(fp)
	stats.FilerStoreCounter.WithLabelValues(actualStore.GetName(), "delete").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(actualStore.GetName(), "delete").Observe(time.Since(start).Seconds())
	}()

	existingEntry, findErr := fsw.FindEntry(ctx, fp)
	if findErr == filer_pb.ErrNotFound || existingEntry == nil {
		return nil
	}
	if len(existingEntry.HardLinkId) != 0 {
		// remove hard link
		op := ctx.Value("OP")
		if op != "MV" {
			glog.V(4).Infof("DeleteHardLink %s", existingEntry.FullPath)
			if err = fsw.DeleteHardLink(ctx, existingEntry.HardLinkId); err != nil {
				return err
			}
		}
	}

	// glog.V(4).Infof("DeleteEntry %s", fp)
	return actualStore.DeleteEntry(ctx, fp)
}

func (fsw *FilerStoreWrapper) DeleteOneEntry(ctx context.Context, existingEntry *Entry) (err error) {
	actualStore := fsw.getActualStore(existingEntry.FullPath)
	stats.FilerStoreCounter.WithLabelValues(actualStore.GetName(), "delete").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(actualStore.GetName(), "delete").Observe(time.Since(start).Seconds())
	}()

	if len(existingEntry.HardLinkId) != 0 {
		// remove hard link
		op := ctx.Value("OP")
		if op != "MV" {
			glog.V(4).Infof("DeleteHardLink %s", existingEntry.FullPath)
			if err = fsw.DeleteHardLink(ctx, existingEntry.HardLinkId); err != nil {
				return err
			}
		}
	}

	// glog.V(4).Infof("DeleteOneEntry %s", existingEntry.FullPath)
	return actualStore.DeleteEntry(ctx, existingEntry.FullPath)
}

func (fsw *FilerStoreWrapper) DeleteFolderChildren(ctx context.Context, fp util.FullPath) (err error) {
	actualStore := fsw.getActualStore(fp + "/")
	stats.FilerStoreCounter.WithLabelValues(actualStore.GetName(), "deleteFolderChildren").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(actualStore.GetName(), "deleteFolderChildren").Observe(time.Since(start).Seconds())
	}()

	// glog.V(4).Infof("DeleteFolderChildren %s", fp)
	return actualStore.DeleteFolderChildren(ctx, fp)
}

func (fsw *FilerStoreWrapper) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc ListEachEntryFunc) (string, error) {
	actualStore := fsw.getActualStore(dirPath + "/")
	stats.FilerStoreCounter.WithLabelValues(actualStore.GetName(), "list").Inc()
	start := time.Now()
	defer func() {
		stats.FilerStoreHistogram.WithLabelValues(actualStore.GetName(), "list").Observe(time.Since(start).Seconds())
	}()

	// glog.V(4).Infof("ListDirectoryEntries %s from %s limit %d", dirPath, startFileName, limit)
	return actualStore.ListDirectoryEntries(ctx, dirPath, startFileName, includeStartFile, limit, func(entry *Entry) bool {
		fsw.maybeReadHardLink(ctx, entry)
		filer_pb.AfterEntryDeserialization(entry.GetChunks())
		return eachEntryFunc(entry)
	})
}

func (fsw *FilerStoreWrapper) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc ListEachEntryFunc) (lastFileName string, err error) {
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
	adjustedEntryFunc := func(entry *Entry) bool {
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

func (fsw *FilerStoreWrapper) prefixFilterEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc ListEachEntryFunc) (lastFileName string, err error) {
	actualStore := fsw.getActualStore(dirPath + "/")

	if prefix == "" {
		return actualStore.ListDirectoryEntries(ctx, dirPath, startFileName, includeStartFile, limit, eachEntryFunc)
	}

	var notPrefixed []*Entry
	lastFileName, err = actualStore.ListDirectoryEntries(ctx, dirPath, startFileName, includeStartFile, limit, func(entry *Entry) bool {
		notPrefixed = append(notPrefixed, entry)
		return true
	})
	if err != nil {
		return
	}

	count := int64(0)
	for count < limit && len(notPrefixed) > 0 {
		for _, entry := range notPrefixed {
			if strings.HasPrefix(entry.Name(), prefix) {
				count++
				if !eachEntryFunc(entry) {
					return
				}
				if count >= limit {
					break
				}
			}
		}
		if count < limit && lastFileName < prefix {
			notPrefixed = notPrefixed[:0]
			lastFileName, err = actualStore.ListDirectoryEntries(ctx, dirPath, lastFileName, false, limit, func(entry *Entry) bool {
				notPrefixed = append(notPrefixed, entry)
				return true
			})
			if err != nil {
				return
			}
		} else {
			break
		}
	}
	return
}

func (fsw *FilerStoreWrapper) BeginTransaction(ctx context.Context) (context.Context, error) {
	return fsw.getDefaultStore().BeginTransaction(ctx)
}

func (fsw *FilerStoreWrapper) CommitTransaction(ctx context.Context) error {
	return fsw.getDefaultStore().CommitTransaction(ctx)
}

func (fsw *FilerStoreWrapper) RollbackTransaction(ctx context.Context) error {
	return fsw.getDefaultStore().RollbackTransaction(ctx)
}

func (fsw *FilerStoreWrapper) Shutdown() {
	fsw.getDefaultStore().Shutdown()
}

func (fsw *FilerStoreWrapper) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	return fsw.getDefaultStore().KvPut(ctx, key, value)
}
func (fsw *FilerStoreWrapper) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	return fsw.getDefaultStore().KvGet(ctx, key)
}
func (fsw *FilerStoreWrapper) KvDelete(ctx context.Context, key []byte) (err error) {
	return fsw.getDefaultStore().KvDelete(ctx, key)
}

func (fsw *FilerStoreWrapper) Debug(writer io.Writer) {
	if debuggable, ok := fsw.getDefaultStore().(Debuggable); ok {
		debuggable.Debug(writer)
	}
}
