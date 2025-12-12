//go:build foundationdb
// +build foundationdb

// Package foundationdb provides a filer store implementation using FoundationDB as the backend.
//
// IMPORTANT DESIGN NOTE - DeleteFolderChildren and Transaction Limits:
//
// FoundationDB imposes strict transaction limits:
//   - Maximum transaction size: 10MB
//   - Maximum transaction duration: 5 seconds
//
// The DeleteFolderChildren operation always uses batched deletion with multiple small transactions
// to safely handle directories of any size. Even if called within an existing transaction context,
// it will create its own batch transactions to avoid exceeding FDB limits.
//
// This means DeleteFolderChildren is NOT atomic with respect to an outer transaction - it manages
// its own transaction boundaries for safety and reliability.

package foundationdb

import (
	"bytes"
	"context"
	"fmt"
	"sync"
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
	// Safe limit for batch size (leave margin for FDB overhead)
	FDB_BATCH_SIZE_LIMIT = 8 * 1024 * 1024
	// Maximum number of entries to return in a single directory listing
	// Large batches can cause transaction timeouts and increase memory pressure
	MAX_DIRECTORY_LIST_LIMIT = 1000

	// Write batching defaults
	DEFAULT_BATCH_SIZE     = 100
	DEFAULT_BATCH_INTERVAL = 5 * time.Millisecond
)

func init() {
	filer.Stores = append(filer.Stores, &FoundationDBStore{})
}

// writeOp represents a pending write operation
type writeOp struct {
	key   fdb.Key
	value []byte // nil for delete
	done  chan error
}

// opSize returns the approximate size of an operation in bytes
func (op *writeOp) size() int {
	return len(op.key) + len(op.value)
}

// writeBatcher batches multiple writes into single transactions
type writeBatcher struct {
	store    *FoundationDBStore
	ops      chan *writeOp
	stop     chan struct{}
	wg       sync.WaitGroup
	size     int
	interval time.Duration
}

func newWriteBatcher(store *FoundationDBStore, size int, interval time.Duration) *writeBatcher {
	b := &writeBatcher{
		store:    store,
		ops:      make(chan *writeOp, size*10),
		stop:     make(chan struct{}),
		size:     size,
		interval: interval,
	}
	b.wg.Add(1)
	go b.run()
	return b
}

func (b *writeBatcher) run() {
	defer b.wg.Done()
	batch := make([]*writeOp, 0, b.size)
	batchBytes := 0 // Track cumulative size of batch
	timer := time.NewTimer(b.interval)
	defer timer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		_, err := b.store.database.Transact(func(tr fdb.Transaction) (interface{}, error) {
			for _, op := range batch {
				if op.value != nil {
					tr.Set(op.key, op.value)
				} else {
					tr.Clear(op.key)
				}
			}
			return nil, nil
		})
		for _, op := range batch {
			if op.done != nil {
				op.done <- err
				close(op.done)
			}
		}
		batch = batch[:0]
		batchBytes = 0
	}

	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(b.interval)
	}

	for {
		select {
		case op := <-b.ops:
			batch = append(batch, op)
			batchBytes += op.size()
			// Flush when batch count or size limit is reached
			if len(batch) >= b.size || batchBytes >= FDB_BATCH_SIZE_LIMIT {
				flush()
				resetTimer()
			}
		case <-timer.C:
			flush()
			// Timer already fired, safe to reset directly
			timer.Reset(b.interval)
		case <-b.stop:
			for {
				select {
				case op := <-b.ops:
					batch = append(batch, op)
				default:
					flush()
					return
				}
			}
		}
	}
}

func (b *writeBatcher) submit(key fdb.Key, value []byte, wait bool) error {
	op := &writeOp{key: key, value: value}
	if wait {
		op.done = make(chan error, 1)
	}
	select {
	case b.ops <- op:
		if wait {
			return <-op.done
		}
		return nil
	case <-b.stop:
		return fmt.Errorf("batcher stopped")
	}
}

func (b *writeBatcher) shutdown() {
	close(b.stop)
	b.wg.Wait()
}

type FoundationDBStore struct {
	database        fdb.Database
	seaweedfsDir    directory.DirectorySubspace
	kvDir           directory.DirectorySubspace
	directoryPrefix string
	timeout         time.Duration
	maxRetryDelay   time.Duration
	// Write batching
	batcher       *writeBatcher
	batchSize     int
	batchInterval time.Duration
}

// Context key type for storing transactions
type contextKey string

const transactionKey contextKey = "fdb_transaction"

// Helper functions for context-scoped transactions
func (store *FoundationDBStore) getTransactionFromContext(ctx context.Context) (fdb.Transaction, bool) {
	val := ctx.Value(transactionKey)
	if val == nil {
		var emptyTx fdb.Transaction
		return emptyTx, false
	}
	if tx, ok := val.(fdb.Transaction); ok {
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
	configuration.SetDefault(prefix+"batch_size", DEFAULT_BATCH_SIZE)
	configuration.SetDefault(prefix+"batch_interval", DEFAULT_BATCH_INTERVAL.String())

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

	// Parse batch configuration
	store.batchSize = configuration.GetInt(prefix + "batch_size")
	if store.batchSize <= 0 {
		store.batchSize = DEFAULT_BATCH_SIZE
	}
	batchIntervalStr := configuration.GetString(prefix + "batch_interval")
	store.batchInterval, err = time.ParseDuration(batchIntervalStr)
	if err != nil {
		glog.Warningf("invalid %sbatch_interval duration %q, using default %v: %v", prefix, batchIntervalStr, DEFAULT_BATCH_INTERVAL, err)
		store.batchInterval = DEFAULT_BATCH_INTERVAL
	}

	return store.initialize(clusterFile, apiVersion)
}

func (store *FoundationDBStore) initialize(clusterFile string, apiVersion int) error {
	glog.V(0).Infof("FoundationDB: connecting to cluster file: %s, API version: %d", clusterFile, apiVersion)

	// Set FDB API version
	if err := fdb.APIVersion(apiVersion); err != nil {
		return fmt.Errorf("failed to set FoundationDB API version %d: %w", apiVersion, err)
	}

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

	// Start write batcher for improved throughput
	store.batcher = newWriteBatcher(store, store.batchSize, store.batchInterval)
	glog.V(0).Infof("FoundationDB: write batching enabled (batch_size=%d, batch_interval=%v)",
		store.batchSize, store.batchInterval)

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

	// Use write batcher for better throughput
	if store.batcher != nil {
		return store.batcher.submit(key, value, true)
	}

	// Fallback: execute in a new transaction
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

	if data == nil {
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

	// Use write batcher for better throughput (nil value = delete)
	if store.batcher != nil {
		return store.batcher.submit(key, nil, true)
	}

	// Fallback: execute in a new transaction
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
	// Recursively delete all entries in this directory and its subdirectories
	// We need recursion because our key structure is tuple{dirPath, fileName}
	// not tuple{dirPath, ...pathComponents}, so a simple prefix range won't catch subdirectories

	// ALWAYS use batched deletion to safely handle directories of any size.
	// This avoids FoundationDB's 10MB transaction size and 5s timeout limits.
	//
	// Note: Even if called within an existing transaction, we create our own batch transactions.
	// This means DeleteFolderChildren is NOT atomic with an outer transaction, but it ensures
	// reliability and prevents transaction limit violations.
	return store.deleteFolderChildrenInBatches(ctx, fullpath)
}

// deleteFolderChildrenInBatches deletes directory contents in multiple transactions
// to avoid hitting FoundationDB's transaction size (10MB) and time (5s) limits
func (store *FoundationDBStore) deleteFolderChildrenInBatches(ctx context.Context, fullpath util.FullPath) error {
	const BATCH_SIZE = 100 // Delete up to 100 entries per transaction

	// Ensure listing and recursion run outside of any ambient transaction
	// Store a sentinel nil value so getTransactionFromContext returns false
	ctxNoTxn := context.WithValue(ctx, transactionKey, (*struct{})(nil))

	for {
		// Collect one batch of entries
		var entriesToDelete []util.FullPath
		var subDirectories []util.FullPath

		// List entries - we'll process BATCH_SIZE at a time
		_, err := store.ListDirectoryEntries(ctxNoTxn, fullpath, "", true, int64(BATCH_SIZE), func(entry *filer.Entry) (bool, error) {
			entriesToDelete = append(entriesToDelete, entry.FullPath)
			if entry.IsDirectory() {
				subDirectories = append(subDirectories, entry.FullPath)
			}
			return true, nil
		})

		if err != nil {
			return fmt.Errorf("listing children of %s: %w", fullpath, err)
		}

		// If no entries found, we're done
		if len(entriesToDelete) == 0 {
			break
		}

		// Recursively delete subdirectories first (also in batches)
		for _, subDir := range subDirectories {
			if err := store.deleteFolderChildrenInBatches(ctxNoTxn, subDir); err != nil {
				return err
			}
		}

		// Delete this batch of entries in a single transaction
		_, err = store.database.Transact(func(tr fdb.Transaction) (interface{}, error) {
			txCtx := store.setTransactionInContext(context.Background(), tr)
			for _, entryPath := range entriesToDelete {
				if delErr := store.DeleteEntry(txCtx, entryPath); delErr != nil {
					return nil, fmt.Errorf("deleting entry %s: %w", entryPath, delErr)
				}
			}
			return nil, nil
		})

		if err != nil {
			return err
		}

		// If we got fewer entries than BATCH_SIZE, we're done with this directory
		if len(entriesToDelete) < BATCH_SIZE {
			break
		}
	}

	return nil
}

func (store *FoundationDBStore) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return store.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, "", eachEntryFunc)
}

func (store *FoundationDBStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	// Cap limit for optimal FoundationDB performance
	// Large batches can cause transaction timeouts and increase memory pressure
	if limit > MAX_DIRECTORY_LIST_LIMIT || limit <= 0 {
		limit = MAX_DIRECTORY_LIST_LIMIT
	}

	// Get the range for the entire directory first
	dirTuple := tuple.Tuple{string(dirPath)}
	dirRange, err := fdb.PrefixRange(store.seaweedfsDir.Pack(dirTuple))
	if err != nil {
		return "", fmt.Errorf("creating prefix range for %s: %w", dirPath, err)
	}

	// Determine the key range for the scan
	// Use FDB's range capabilities to only fetch keys matching the prefix
	var beginKey, endKey fdb.Key
	dirBeginConv, dirEndConv := dirRange.FDBRangeKeys()
	dirBegin := dirBeginConv.FDBKey()
	dirEnd := dirEndConv.FDBKey()

	if prefix != "" {
		// Build range by bracketing the filename component
		// Start at Pack(dirPath, prefix) and end at Pack(dirPath, nextPrefix)
		// where nextPrefix is the next lexicographic string
		beginKey = store.seaweedfsDir.Pack(tuple.Tuple{string(dirPath), prefix})
		endKey = dirEnd

		// Use Strinc to get the next string for proper prefix range
		if nextPrefix, strincErr := fdb.Strinc([]byte(prefix)); strincErr == nil {
			endKey = store.seaweedfsDir.Pack(tuple.Tuple{string(dirPath), string(nextPrefix)})
		}
	} else {
		// Use entire directory range
		beginKey = dirBegin
		endKey = dirEnd
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
		if bytes.Compare(beginSelector.Key.FDBKey(), beginKey.FDBKey()) < 0 {
			beginSelector = fdb.FirstGreaterOrEqual(beginKey)
		}
	} else {
		// Start from beginning of the range
		beginSelector = fdb.FirstGreaterOrEqual(beginKey)
	}

	// End selector is the end of our calculated range
	endSelector := fdb.FirstGreaterOrEqual(endKey)

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
		var ok bool
		kvs, ok = result.([]fdb.KeyValue)
		if !ok {
			return "", fmt.Errorf("unexpected type from ReadTransact: %T, expected []fdb.KeyValue", result)
		}
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

		resEachEntryFunc, resEachEntryFuncErr := eachEntryFunc(entry)
		if resEachEntryFuncErr != nil {
			glog.ErrorfCtx(ctx, "failed to process eachEntryFunc for entry %q: %v", fileName, resEachEntryFuncErr)
			return lastFileName, fmt.Errorf("failed to process eachEntryFunc for entry %q: %w", fileName, resEachEntryFuncErr)
		}
		if !resEachEntryFunc {
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
	if data == nil {
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
	// Stop write batcher
	if store.batcher != nil {
		store.batcher.shutdown()
		store.batcher = nil
	}
	// FoundationDB doesn't have an explicit close method for Database
	glog.V(0).Infof("FoundationDB store shutdown")
}

// tuplePool reduces allocations in genKey which is called on every FDB operation
var tuplePool = sync.Pool{
	New: func() interface{} {
		// Pre-allocate slice with capacity 2 for (dirPath, fileName)
		t := make(tuple.Tuple, 2)
		return &t
	},
}

// Helper functions
func (store *FoundationDBStore) genKey(dirPath, fileName string) fdb.Key {
	// Get a tuple from pool to avoid slice allocation
	tp := tuplePool.Get().(*tuple.Tuple)
	t := *tp
	t[0] = dirPath
	t[1] = fileName
	key := store.seaweedfsDir.Pack(t)
	// Clear references before returning to pool to avoid memory leaks
	t[0] = nil
	t[1] = nil
	tuplePool.Put(tp)
	return key
}

func (store *FoundationDBStore) extractFileName(key fdb.Key) (string, error) {
	t, err := store.seaweedfsDir.Unpack(key)
	if err != nil {
		return "", fmt.Errorf("unpack key %v: %w", key, err)
	}
	if len(t) != 2 {
		return "", fmt.Errorf("tuple unexpected length (len=%d, expected 2) for key %v", len(t), key)
	}

	if fileName, ok := t[1].(string); ok {
		return fileName, nil
	}
	return "", fmt.Errorf("second element not a string (type=%T) for key %v", t[1], key)
}
