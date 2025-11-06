//go:build foundationdb
// +build foundationdb

package foundationdb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	DIR_FILE_SEPARATOR = byte(0x00)
	// FoundationDB transaction size limit is 10MB
	FDB_TRANSACTION_SIZE_LIMIT = 10 * 1024 * 1024
)

// Helper function to create prefix range (tries idiomatic approach first, falls back to custom)
func prefixRange(prefix fdb.Key) fdb.KeyRange {
	// Try to use idiomatic FoundationDB PrefixRange if available
	// Note: This may not be available in older Go bindings, so we provide fallback
	begin := prefix
	end := prefixEndFallback(prefix)
	return fdb.KeyRange{Begin: begin, End: end}
}

// Fallback implementation for prefix end calculation
func prefixEndFallback(prefix fdb.Key) fdb.Key {
	if len(prefix) == 0 {
		return fdb.Key("\xff")
	}

	// Create a copy and increment the last byte
	end := make([]byte, len(prefix))
	copy(end, prefix)

	// Find the last byte that can be incremented
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return fdb.Key(end[:i+1])
		}
	}

	// All bytes are 0xff, append 0x00
	return fdb.Key(append(end, 0x00))
}

func init() {
	filer.Stores = append(filer.Stores, &FoundationDBStore{})
}

type FoundationDBStore struct {
	database        fdb.Database
	seaweedfsDir    directory.DirectorySubspace
	kvDir           directory.DirectorySubspace
	directoryPrefix string
	timeout         time.Duration
	maxRetryDelay   time.Duration
}

// Context key type for storing transactions
type contextKey string

const transactionKey contextKey = "fdb_transaction"

// Helper functions for context-scoped transactions
func (store *FoundationDBStore) getTransactionFromContext(ctx context.Context) (fdb.Transaction, bool) {
	if tx, ok := ctx.Value(transactionKey).(fdb.Transaction); ok {
		return tx, true
	}
	var emptyTx fdb.Transaction
	return emptyTx, false
}

func (store *FoundationDBStore) setTransactionInContext(ctx context.Context, tx fdb.Transaction) context.Context {
	return context.WithValue(ctx, transactionKey, tx)
}

func (store *FoundationDBStore) GetName() string {
	return "foundationdb"
}

func (store *FoundationDBStore) Initialize(configuration util.Configuration, prefix string) error {
	// Set default configuration values
	configuration.SetDefault(prefix+"cluster_file", "/etc/foundationdb/fdb.cluster")
	configuration.SetDefault(prefix+"api_version", 630)
	configuration.SetDefault(prefix+"timeout", "5s")
	configuration.SetDefault(prefix+"max_retry_delay", "1s")
	configuration.SetDefault(prefix+"directory_prefix", "seaweedfs")

	clusterFile := configuration.GetString(prefix + "cluster_file")
	apiVersion := configuration.GetInt(prefix + "api_version")
	timeoutStr := configuration.GetString(prefix + "timeout")
	maxRetryDelayStr := configuration.GetString(prefix + "max_retry_delay")
	store.directoryPrefix = configuration.GetString(prefix + "directory_prefix")

	// Parse timeout values
	var err error
	store.timeout, err = time.ParseDuration(timeoutStr)
	if err != nil {
		return fmt.Errorf("invalid timeout duration %s: %v", timeoutStr, err)
	}

	store.maxRetryDelay, err = time.ParseDuration(maxRetryDelayStr)
	if err != nil {
		return fmt.Errorf("invalid max_retry_delay duration %s: %v", maxRetryDelayStr, err)
	}

	return store.initialize(clusterFile, apiVersion)
}

func (store *FoundationDBStore) initialize(clusterFile string, apiVersion int) error {
	glog.V(0).Infof("FoundationDB: connecting to cluster file: %s, API version: %d", clusterFile, apiVersion)

	// Set FDB API version
	fdb.MustAPIVersion(apiVersion)

	// Open database
	var err error
	store.database, err = fdb.OpenDatabase(clusterFile)
	if err != nil {
		return fmt.Errorf("failed to open FoundationDB database: %v", err)
	}

	// Create/open seaweedfs directory
	store.seaweedfsDir, err = directory.CreateOrOpen(store.database, []string{store.directoryPrefix, "data"}, nil)
	if err != nil {
		return fmt.Errorf("failed to create/open seaweedfs directory: %v", err)
	}

	// Create/open kv subdirectory for key-value operations
	store.kvDir, err = directory.CreateOrOpen(store.database, []string{store.directoryPrefix, "kv"}, nil)
	if err != nil {
		return fmt.Errorf("failed to create/open kv directory: %v", err)
	}

	glog.V(0).Infof("FoundationDB store initialized successfully with directory prefix: %s", store.directoryPrefix)
	return nil
}

func (store *FoundationDBStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	// Check if there's already a transaction in this context
	if _, exists := store.getTransactionFromContext(ctx); exists {
		return ctx, fmt.Errorf("transaction already in progress for this context")
	}

	// Create a new transaction
	tx, err := store.database.CreateTransaction()
	if err != nil {
		return ctx, fmt.Errorf("failed to create transaction: %v", err)
	}

	// Store the transaction in context and return the new context
	newCtx := store.setTransactionInContext(ctx, tx)
	return newCtx, nil
}

func (store *FoundationDBStore) CommitTransaction(ctx context.Context) error {
	// Get transaction from context
	tx, exists := store.getTransactionFromContext(ctx)
	if !exists {
		return fmt.Errorf("no transaction in progress for this context")
	}

	// Commit the transaction
	err := tx.Commit().Get()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

func (store *FoundationDBStore) RollbackTransaction(ctx context.Context) error {
	// Get transaction from context
	tx, exists := store.getTransactionFromContext(ctx)
	if !exists {
		return fmt.Errorf("no transaction in progress for this context")
	}

	// Cancel the transaction
	tx.Cancel()
	return nil
}

func (store *FoundationDBStore) InsertEntry(ctx context.Context, entry *filer.Entry) error {
	return store.UpdateEntry(ctx, entry)
}

func (store *FoundationDBStore) UpdateEntry(ctx context.Context, entry *filer.Entry) error {
	key := store.genKey(entry.DirAndName())

	value, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	if len(entry.GetChunks()) > filer.CountEntryChunksForGzip {
		value = util.MaybeGzipData(value)
	}

	// Check if there's a transaction in context
	if tx, exists := store.getTransactionFromContext(ctx); exists {
		tx.Set(key, value)
		return nil
	}

	// Execute in a new transaction if not in an existing one
	_, err = store.database.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(key, value)
		return nil, nil
	})

	if err != nil {
		return fmt.Errorf("persisting %s: %v", entry.FullPath, err)
	}

	return nil
}

func (store *FoundationDBStore) FindEntry(ctx context.Context, fullpath util.FullPath) (entry *filer.Entry, err error) {
	key := store.genKey(util.FullPath(fullpath).DirAndName())

	var data []byte
	// Check if there's a transaction in context
	if tx, exists := store.getTransactionFromContext(ctx); exists {
		data, err = tx.Get(key).Get()
	} else {
		result, err := store.database.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			return rtr.Get(key).Get()
		})
		if err == nil {
			if resultBytes, ok := result.([]byte); ok {
				data = resultBytes
			}
		}
	}

	if err != nil {
		return nil, filer_pb.ErrNotFound
	}

	if len(data) == 0 {
		return nil, filer_pb.ErrNotFound
	}

	entry = &filer.Entry{
		FullPath: fullpath,
	}

	err = entry.DecodeAttributesAndChunks(util.MaybeDecompressData(data))
	if err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}

	return entry, nil
}

func (store *FoundationDBStore) DeleteEntry(ctx context.Context, fullpath util.FullPath) error {
	key := store.genKey(util.FullPath(fullpath).DirAndName())

	// Check if there's a transaction in context
	if tx, exists := store.getTransactionFromContext(ctx); exists {
		tx.Clear(key)
		return nil
	}

	// Execute in a new transaction if not in an existing one
	_, err := store.database.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Clear(key)
		return nil, nil
	})

	if err != nil {
		return fmt.Errorf("deleting %s: %v", fullpath, err)
	}

	return nil
}

func (store *FoundationDBStore) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) error {
	directoryPrefix := store.genDirectoryKeyPrefix(string(fullpath), "")

	// Check if there's a transaction in context
	if tx, exists := store.getTransactionFromContext(ctx); exists {
		kr := prefixRange(directoryPrefix)
		tx.ClearRange(kr)
		return nil
	}

	// Execute in a new transaction if not in an existing one
	_, err := store.database.Transact(func(tr fdb.Transaction) (interface{}, error) {
		kr := prefixRange(directoryPrefix)
		tr.ClearRange(kr)
		return nil, nil
	})

	if err != nil {
		return fmt.Errorf("deleting folder children %s: %v", fullpath, err)
	}

	return nil
}

func (store *FoundationDBStore) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return store.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, "", eachEntryFunc)
}

func (store *FoundationDBStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	if limit > 1000 {
		limit = 1000
	}

	directoryPrefix := store.genDirectoryKeyPrefix(string(dirPath), prefix)
	startKey := store.genDirectoryKeyPrefix(string(dirPath), startFileName)

	if !includeStartFile {
		startKey = append(startKey, 0x00)
	}

	var kvs []fdb.KeyValue
	// Check if there's a transaction in context
	if tx, exists := store.getTransactionFromContext(ctx); exists {
		kr := fdb.KeyRange{Begin: fdb.Key(startKey), End: prefixEndFallback(directoryPrefix)}
		kvs = tx.GetRange(kr, fdb.RangeOptions{Limit: int(limit)}).GetSliceOrPanic()
	} else {
		result, err := store.database.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			kr := fdb.KeyRange{Begin: fdb.Key(startKey), End: prefixEndFallback(directoryPrefix)}
			return rtr.GetRange(kr, fdb.RangeOptions{Limit: int(limit)}).GetSliceOrPanic(), nil
		})
		if err != nil {
			return "", fmt.Errorf("scanning %s: %v", dirPath, err)
		}
		kvs = result.([]fdb.KeyValue)
	}

	for _, kv := range kvs {
		fileName := store.extractFileName(kv.Key)
		if fileName == "" {
			continue
		}

		if !strings.HasPrefix(fileName, prefix) {
			continue
		}

		entry := &filer.Entry{
			FullPath: util.NewFullPath(string(dirPath), fileName),
		}

		if decodeErr := entry.DecodeAttributesAndChunks(util.MaybeDecompressData(kv.Value)); decodeErr != nil {
			glog.V(0).Infof("list %s : %v", entry.FullPath, decodeErr)
			continue
		}

		if !eachEntryFunc(entry) {
			break
		}
		lastFileName = fileName
	}

	return lastFileName, nil
}

// KV operations
func (store *FoundationDBStore) KvPut(ctx context.Context, key []byte, value []byte) error {
	fdbKey := store.kvDir.Pack(tuple.Tuple{key})

	// Check if there's a transaction in context
	if tx, exists := store.getTransactionFromContext(ctx); exists {
		tx.Set(fdbKey, value)
		return nil
	}

	_, err := store.database.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(fdbKey, value)
		return nil, nil
	})

	return err
}

func (store *FoundationDBStore) KvGet(ctx context.Context, key []byte) ([]byte, error) {
	fdbKey := store.kvDir.Pack(tuple.Tuple{key})

	var data []byte
	var err error

	// Check if there's a transaction in context
	if tx, exists := store.getTransactionFromContext(ctx); exists {
		data, err = tx.Get(fdbKey).Get()
	} else {
		result, err := store.database.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			return rtr.Get(fdbKey).Get()
		})
		if err == nil {
			if resultBytes, ok := result.([]byte); ok {
				data = resultBytes
			}
		}
	}

	if err != nil || len(data) == 0 {
		return nil, filer.ErrKvNotFound
	}

	return data, nil
}

func (store *FoundationDBStore) KvDelete(ctx context.Context, key []byte) error {
	fdbKey := store.kvDir.Pack(tuple.Tuple{key})

	// Check if there's a transaction in context
	if tx, exists := store.getTransactionFromContext(ctx); exists {
		tx.Clear(fdbKey)
		return nil
	}

	_, err := store.database.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Clear(fdbKey)
		return nil, nil
	})

	return err
}

func (store *FoundationDBStore) Shutdown() {
	// FoundationDB doesn't have an explicit close method for Database
	glog.V(0).Infof("FoundationDB store shutdown")
}

// Helper functions
func (store *FoundationDBStore) genKey(dirPath, fileName string) fdb.Key {
	return store.seaweedfsDir.Pack(tuple.Tuple{dirPath, fileName})
}

func (store *FoundationDBStore) genDirectoryKeyPrefix(dirPath, prefix string) fdb.Key {
	if prefix == "" {
		return store.seaweedfsDir.Pack(tuple.Tuple{dirPath, ""})
	}
	return store.seaweedfsDir.Pack(tuple.Tuple{dirPath, prefix})
}

func (store *FoundationDBStore) extractFileName(key fdb.Key) string {
	t, err := store.seaweedfsDir.Unpack(key)
	if err != nil || len(t) < 2 {
		return ""
	}

	if fileName, ok := t[1].(string); ok {
		return fileName
	}
	return ""
}
