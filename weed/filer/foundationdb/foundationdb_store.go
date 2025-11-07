//go:build foundationdb
// +build foundationdb

package foundationdb

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	// FoundationDB transaction size limit is 10MB
	FDB_TRANSACTION_SIZE_LIMIT = 10 * 1024 * 1024
)

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
	configuration.SetDefault(prefix+"api_version", 740)
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
		return fmt.Errorf("invalid timeout duration %s: %w", timeoutStr, err)
	}

	store.maxRetryDelay, err = time.ParseDuration(maxRetryDelayStr)
	if err != nil {
		return fmt.Errorf("invalid max_retry_delay duration %s: %w", maxRetryDelayStr, err)
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
		return fmt.Errorf("failed to open FoundationDB database: %w", err)
	}

	// Create/open seaweedfs directory
	store.seaweedfsDir, err = directory.CreateOrOpen(store.database, []string{store.directoryPrefix}, nil)
	if err != nil {
		return fmt.Errorf("failed to create/open seaweedfs directory: %w", err)
	}

	// Create/open kv subdirectory for key-value operations
	store.kvDir, err = directory.CreateOrOpen(store.database, []string{store.directoryPrefix, "kv"}, nil)
	if err != nil {
		return fmt.Errorf("failed to create/open kv directory: %w", err)
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
		return ctx, fmt.Errorf("failed to create transaction: %w", err)
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
		return fmt.Errorf("failed to commit transaction: %w", err)
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
		return fmt.Errorf("encoding %s %+v: %w", entry.FullPath, entry.Attr, err)
	}

	if len(entry.GetChunks()) > filer.CountEntryChunksForGzip {
		value = util.MaybeGzipData(value)
	}

	// Check transaction size limit
	if len(value) > FDB_TRANSACTION_SIZE_LIMIT {
		return fmt.Errorf("entry %s exceeds FoundationDB transaction size limit (%d > %d bytes)",
			entry.FullPath, len(value), FDB_TRANSACTION_SIZE_LIMIT)
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
		return fmt.Errorf("persisting %s: %w", entry.FullPath, err)
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
		var result interface{}
		result, err = store.database.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			return rtr.Get(key).Get()
		})
		if err == nil {
			if resultBytes, ok := result.([]byte); ok {
				data = resultBytes
			}
		}
	}

	if err != nil {
		return nil, fmt.Errorf("find entry %s: %w", fullpath, err)
	}

	if len(data) == 0 {
		return nil, filer_pb.ErrNotFound
	}

	entry = &filer.Entry{
		FullPath: fullpath,
	}

	err = entry.DecodeAttributesAndChunks(util.MaybeDecompressData(data))
	if err != nil {
		return entry, fmt.Errorf("decode %s : %w", entry.FullPath, err)
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
		return fmt.Errorf("deleting %s: %w", fullpath, err)
	}

	return nil
}

func (store *FoundationDBStore) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) error {
	// Construct tuple-aware range for all children in this directory
	prefixBytes := store.seaweedfsDir.Pack(tuple.Tuple{string(fullpath)})
	kr, err := fdb.PrefixRange(prefixBytes)
	if err != nil {
		return fmt.Errorf("creating prefix range for %s: %w", fullpath, err)
	}

	// Check if there's a transaction in context
	if tx, exists := store.getTransactionFromContext(ctx); exists {
		tx.ClearRange(kr)
		return nil
	}

	// Execute in a new transaction if not in an existing one
	_, err = store.database.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(kr)
		return nil, nil
	})

	if err != nil {
		return fmt.Errorf("deleting folder children %s: %w", fullpath, err)
	}

	return nil
}

func (store *FoundationDBStore) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return store.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, "", eachEntryFunc)
}

func (store *FoundationDBStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	// Cap limit at 1000 for optimal FoundationDB performance
	// Large batches can cause transaction timeouts and increase memory pressure
	if limit > 1000 {
		limit = 1000
	}

	// Determine the key range for the scan
	// Use FDB's range capabilities to only fetch keys matching the prefix
	var keyRange fdb.Range
	if prefix != "" {
		// Create a range for the prefix within the directory
		// This ensures FDB only returns keys starting with the prefix
		prefixTuple := tuple.Tuple{string(dirPath), prefix}
		keyRange, err = fdb.PrefixRange(store.seaweedfsDir.Pack(prefixTuple))
		if err != nil {
			return "", fmt.Errorf("creating prefix range for %s with prefix %s: %w", dirPath, prefix, err)
		}
	} else {
		// Create a range for the entire directory
		dirTuple := tuple.Tuple{string(dirPath)}
		keyRange, err = fdb.PrefixRange(store.seaweedfsDir.Pack(dirTuple))
		if err != nil {
			return "", fmt.Errorf("creating prefix range for %s: %w", dirPath, err)
		}
	}

	// Determine start key and selector based on startFileName
	var beginSelector fdb.KeySelector
	if startFileName != "" {
		// Start from the specified file
		startKey := store.seaweedfsDir.Pack(tuple.Tuple{string(dirPath), startFileName})
		if includeStartFile {
			beginSelector = fdb.FirstGreaterOrEqual(startKey)
		} else {
			beginSelector = fdb.FirstGreaterThan(startKey)
		}
		// Ensure beginSelector is within our desired range
		if bytes.Compare(beginSelector.Key, keyRange.Begin) < 0 {
			beginSelector = fdb.FirstGreaterOrEqual(keyRange.Begin)
		}
	} else {
		// Start from beginning of the range
		beginSelector = fdb.FirstGreaterOrEqual(keyRange.Begin)
	}

	// End selector is the end of our calculated range
	endSelector := fdb.FirstGreaterOrEqual(keyRange.End)

	var kvs []fdb.KeyValue
	var rangeErr error
	// Check if there's a transaction in context
	if tx, exists := store.getTransactionFromContext(ctx); exists {
		sr := fdb.SelectorRange{Begin: beginSelector, End: endSelector}
		kvs, rangeErr = tx.GetRange(sr, fdb.RangeOptions{Limit: int(limit)}).GetSliceWithError()
		if rangeErr != nil {
			return "", fmt.Errorf("scanning %s: %w", dirPath, rangeErr)
		}
	} else {
		result, err := store.database.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			sr := fdb.SelectorRange{Begin: beginSelector, End: endSelector}
			kvSlice, err := rtr.GetRange(sr, fdb.RangeOptions{Limit: int(limit)}).GetSliceWithError()
			if err != nil {
				return nil, err
			}
			return kvSlice, nil
		})
		if err != nil {
			return "", fmt.Errorf("scanning %s: %w", dirPath, err)
		}
		kvs = result.([]fdb.KeyValue)
	}

	for _, kv := range kvs {
		fileName, extractErr := store.extractFileName(kv.Key)
		if extractErr != nil {
			glog.Warningf("list %s: failed to extract fileName from key %v: %v", dirPath, kv.Key, extractErr)
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
		var result interface{}
		result, err = store.database.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
			return rtr.Get(fdbKey).Get()
		})
		if err == nil {
			if resultBytes, ok := result.([]byte); ok {
				data = resultBytes
			}
		}
	}

	if err != nil {
		return nil, fmt.Errorf("kv get %s: %w", string(key), err)
	}
	if len(data) == 0 {
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

func (store *FoundationDBStore) extractFileName(key fdb.Key) (string, error) {
	t, err := store.seaweedfsDir.Unpack(key)
	if err != nil {
		return "", fmt.Errorf("unpack key %v: %w", key, err)
	}
	if len(t) < 2 {
		return "", fmt.Errorf("tuple too short (len=%d) for key %v", len(t), key)
	}

	if fileName, ok := t[1].(string); ok {
		return fileName, nil
	}
	return "", fmt.Errorf("second element not a string (type=%T) for key %v", t[1], key)
}
