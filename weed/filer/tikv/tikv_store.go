//go:build tikv
// +build tikv

package tikv

import (
	"bytes"
	"context"
	"crypto/sha1"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/txnkv"
)

const defaultBatchCommitSize = 10000

var (
	_ filer.FilerStore = ((*TikvStore)(nil))
)

func init() {
	filer.Stores = append(filer.Stores, &TikvStore{})
}

type TikvStore struct {
	client          *txnkv.Client
	onePC           bool
	batchCommitSize int
}

// Basic APIs
func (store *TikvStore) GetName() string {
	return "tikv"
}

func (store *TikvStore) Initialize(config util.Configuration, prefix string) error {
	ca := config.GetString(prefix + "ca_path")
	cert := config.GetString(prefix + "cert_path")
	key := config.GetString(prefix + "key_path")
	verify_cn := strings.Split(config.GetString(prefix+"verify_cn"), ",")
	pdAddrs := strings.Split(config.GetString(prefix+"pdaddrs"), ",")

	bdc := config.GetInt(prefix + "batchdelete_count")
	if bdc <= 0 {
		bdc = defaultBatchCommitSize
	}

	store.onePC = config.GetBool(prefix + "enable_1pc")
	store.batchCommitSize = bdc
	return store.initialize(ca, cert, key, verify_cn, pdAddrs)
}

func (store *TikvStore) initialize(ca, cert, key string, verify_cn, pdAddrs []string) error {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Security = config.NewSecurity(ca, cert, key, verify_cn)
	})
	client, err := txnkv.NewClient(pdAddrs)
	store.client = client
	return err
}

func (store *TikvStore) Shutdown() {
	err := store.client.Close()
	if err != nil {
		glog.V(0).Infof("Shutdown TiKV client got error: %v", err)
	}
}

// ~ Basic APIs

// Entry APIs
func (store *TikvStore) InsertEntry(ctx context.Context, entry *filer.Entry) error {
	dir, name := entry.DirAndName()
	key := generateKey(dir, name)

	value, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}
	txn, err := store.getTxn(ctx)
	if err != nil {
		return err
	}
	err = txn.RunInTxn(ctx, func(txn *txnkv.KVTxn) error {
		return txn.Set(key, value)
	})
	if err != nil {
		return fmt.Errorf("persisting %s : %v", entry.FullPath, err)
	}
	return nil
}

func (store *TikvStore) UpdateEntry(ctx context.Context, entry *filer.Entry) error {
	return store.InsertEntry(ctx, entry)
}

func (store *TikvStore) FindEntry(ctx context.Context, path util.FullPath) (*filer.Entry, error) {
	dir, name := path.DirAndName()
	key := generateKey(dir, name)

	txn, err := store.getTxn(ctx)
	if err != nil {
		return nil, err
	}
	var value []byte = nil
	err = txn.RunInTxn(ctx, func(txn *txnkv.KVTxn) error {
		val, err := txn.Get(ctx, key)
		if err == nil {
			value = val
		}
		return err
	})

	if isNotExists(err) || value == nil {
		return nil, filer_pb.ErrNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("get %s : %v", path, err)
	}

	entry := &filer.Entry{
		FullPath: path,
	}
	err = entry.DecodeAttributesAndChunks(value)
	if err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}
	return entry, nil
}

func (store *TikvStore) DeleteEntry(ctx context.Context, path util.FullPath) error {
	dir, name := path.DirAndName()
	key := generateKey(dir, name)

	txn, err := store.getTxn(ctx)
	if err != nil {
		return err
	}

	err = txn.RunInTxn(ctx, func(txn *txnkv.KVTxn) error {
		return txn.Delete(key)
	})
	if err != nil {
		return fmt.Errorf("delete %s : %v", path, err)
	}
	return nil
}

// ~ Entry APIs

// Directory APIs
func (store *TikvStore) DeleteFolderChildren(ctx context.Context, path util.FullPath) error {
	directoryPrefix := genDirectoryKeyPrefix(path, "")

	iterTxn, err := store.getTxn(ctx)
	if err != nil {
		return err
	}

	if !iterTxn.inContext {
		defer func() {
			_ = iterTxn.Rollback()
		}()
	}

	iter, err := iterTxn.Iter(directoryPrefix, nil)
	if err != nil {
		return err
	}
	defer iter.Close()

	var keys [][]byte

	flush := func() error {
		if len(keys) == 0 {
			return nil
		}
		if err := store.deleteBatch(ctx, keys); err != nil {
			return fmt.Errorf("delete batch in %s, error: %w", path, err)
		}
		keys = keys[:0]
		return nil
	}

	for iter.Valid() {
		key := iter.Key()
		if !bytes.HasPrefix(key, directoryPrefix) {
			break
		}

		keys = append(keys, append([]byte(nil), key...))

		if len(keys) >= store.batchCommitSize {
			if err := flush(); err != nil {
				return err
			}
		}

		if err := iter.Next(); err != nil {
			return err
		}
	}

	return flush()
}

func (store *TikvStore) deleteBatch(ctx context.Context, keys [][]byte) error {
	deleteTxn, err := store.getTxn(ctx)
	if err != nil {
		return err
	}

	if !deleteTxn.inContext {
		defer func() { _ = deleteTxn.Rollback() }()
	}

	for _, key := range keys {
		if err := deleteTxn.Delete(key); err != nil {
			return err
		}
	}

	if !deleteTxn.inContext {
		return deleteTxn.Commit(ctx)
	}

	return nil
}

func (store *TikvStore) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (string, error) {
	return store.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, "", eachEntryFunc)
}

func (store *TikvStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (string, error) {
	lastFileName := ""
	directoryPrefix := genDirectoryKeyPrefix(dirPath, prefix)
	lastFileStart := directoryPrefix
	if startFileName != "" {
		lastFileStart = genDirectoryKeyPrefix(dirPath, startFileName)
	}

	txn, err := store.getTxn(ctx)
	if err != nil {
		return lastFileName, err
	}
	var callbackErr error
	err = txn.RunInTxn(ctx, func(txn *txnkv.KVTxn) error {
		iter, err := txn.Iter(lastFileStart, nil)
		if err != nil {
			return err
		}
		defer iter.Close()
		i := int64(0)
		for iter.Valid() {
			key := iter.Key()
			if !bytes.HasPrefix(key, directoryPrefix) {
				break
			}
			fileName := getNameFromKey(key)
			if fileName == "" {
				if err := iter.Next(); err != nil {
					break
				}
				continue
			}
			if fileName == startFileName && !includeStartFile {
				if err := iter.Next(); err != nil {
					break
				}
				continue
			}

			// Check limit only before processing valid entries
			if limit > 0 && i >= limit {
				break
			}

			lastFileName = fileName
			entry := &filer.Entry{
				FullPath: util.NewFullPath(string(dirPath), fileName),
			}

			// println("list", entry.FullPath, "chunks", len(entry.GetChunks()))
			if decodeErr := entry.DecodeAttributesAndChunks(util.MaybeDecompressData(iter.Value())); decodeErr != nil {
				err = decodeErr
				glog.V(0).InfofCtx(ctx, "list %s : %v", entry.FullPath, err)
				break
			}

			// Check TTL expiration before calling eachEntryFunc (similar to Redis stores)
			if entry.TtlSec > 0 {
				if entry.Crtime.Add(time.Duration(entry.TtlSec) * time.Second).Before(time.Now()) {
					// Entry is expired, delete it and continue without counting toward limit
					if deleteErr := store.DeleteEntry(ctx, entry.FullPath); deleteErr != nil {
						glog.V(0).InfofCtx(ctx, "failed to delete expired entry %s: %v", entry.FullPath, deleteErr)
					}
					if err := iter.Next(); err != nil {
						break
					}
					continue
				}
			}

			// Only increment counter for non-expired entries
			i++

			resEachEntryFunc, resEachEntryFuncErr := eachEntryFunc(entry)
			if resEachEntryFuncErr != nil {
				glog.V(0).InfofCtx(ctx, "failed to process eachEntryFunc for entry %q: %v", fileName, resEachEntryFuncErr)
				callbackErr = resEachEntryFuncErr
				break
			}

			nextErr := iter.Next()
			if nextErr != nil {
				err = nextErr
				break
			}

			if !resEachEntryFunc {
				break
			}
		}
		return err
	})

	if callbackErr != nil {
		return lastFileName, fmt.Errorf(
			"failed to process eachEntryFunc for dir %q, entry %q: %w",
			dirPath, lastFileName, callbackErr,
		)
	}

	if err != nil {
		return lastFileName, fmt.Errorf("prefix list %s : %v", dirPath, err)
	}
	return lastFileName, nil
}

// ~ Directory APIs

// Transaction Related APIs
func (store *TikvStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	tx, err := store.client.Begin()
	if err != nil {
		return ctx, err
	}
	if store.onePC {
		tx.SetEnable1PC(store.onePC)
	}
	return context.WithValue(ctx, "tx", tx), nil
}

func (store *TikvStore) CommitTransaction(ctx context.Context) error {
	if tx, ok := ctx.Value("tx").(*txnkv.KVTxn); ok {
		return tx.Commit(context.Background())
	}
	return nil
}

func (store *TikvStore) RollbackTransaction(ctx context.Context) error {
	if tx, ok := ctx.Value("tx").(*txnkv.KVTxn); ok {
		return tx.Rollback()
	}
	return nil
}

// ~ Transaction Related APIs

// Transaction Wrapper
type TxnWrapper struct {
	*txnkv.KVTxn
	inContext bool
}

func (w *TxnWrapper) RunInTxn(ctx context.Context, f func(txn *txnkv.KVTxn) error) error {
	err := f(w.KVTxn)
	if !w.inContext {
		if err != nil {
			w.KVTxn.Rollback()
			return err
		}
		return w.KVTxn.Commit(ctx)
	}
	return err
}

func (store *TikvStore) getTxn(ctx context.Context) (*TxnWrapper, error) {
	if tx, ok := ctx.Value("tx").(*txnkv.KVTxn); ok {
		return &TxnWrapper{tx, true}, nil
	}
	txn, err := store.client.Begin()
	if err != nil {
		return nil, err
	}
	if store.onePC {
		txn.SetEnable1PC(store.onePC)
	}
	return &TxnWrapper{txn, false}, nil
}

// ~ Transaction Wrapper

// Encoding Functions
func hashToBytes(dir string) []byte {
	h := sha1.New()
	io.WriteString(h, dir)
	b := h.Sum(nil)
	return b
}

func generateKey(dirPath, fileName string) []byte {
	key := hashToBytes(dirPath)
	key = append(key, []byte(fileName)...)
	return key
}

func getNameFromKey(key []byte) string {
	return string(key[sha1.Size:])
}

func genDirectoryKeyPrefix(fullpath util.FullPath, startFileName string) (keyPrefix []byte) {
	keyPrefix = hashToBytes(string(fullpath))
	if len(startFileName) > 0 {
		keyPrefix = append(keyPrefix, []byte(startFileName)...)
	}
	return keyPrefix
}

func isNotExists(err error) bool {
	if err == nil {
		return false
	}
	if err.Error() == "not exist" {
		return true
	}
	return false
}

// ~ Encoding Functions
