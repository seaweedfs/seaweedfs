package badger

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/dgraph-io/badger/v3"
)

type BadgerStore struct {
	db *badger.DB
}

func init() {
	filer.Stores = append(filer.Stores, &BadgerStore{})
}

func (s *BadgerStore) GetName() string { return "badger" }

func (s *BadgerStore) BeginTransaction(ctx context.Context) (context.Context, error) { return ctx, nil }
func (s *BadgerStore) CommitTransaction(ctx context.Context) error                   { return nil }
func (s *BadgerStore) RollbackTransaction(ctx context.Context) error                 { return nil }

func (s *BadgerStore) Initialize(configuration util.Configuration, prefix string) error {
	opts := badger.DefaultOptions(prefix + "dir")
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

func (s *BadgerStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (s *BadgerStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		err = filer.ErrKvNotFound
	}
	return value, err
}

func (s *BadgerStore) KvDelete(ctx context.Context, key []byte) (err error) {
	err = s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		err = filer.ErrKvNotFound
	}
	return err
}

func generateKey(dirPath, fileName string) []byte {
	key := make([]byte, len(dirPath)+len(fileName)+1)
	copy(key, dirPath)
	if fileName != "" {
		copy(key[len(dirPath)+1:], fileName)
	}
	return key
}

func (s *BadgerStore) InsertEntry(ctx context.Context, entry *filer.Entry) error {
	dir, name := entry.DirAndName()
	key := generateKey(dir, name)

	value, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (s *BadgerStore) UpdateEntry(ctx context.Context, entry *filer.Entry) error {
	return s.InsertEntry(ctx, entry)
}

func (s *BadgerStore) FindEntry(ctx context.Context, path util.FullPath) (*filer.Entry, error) {
	dir, name := path.DirAndName()
	key := generateKey(dir, name)

	var value []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(value)
		return err
	})

	if errors.Is(err, badger.ErrKeyNotFound) || value == nil {
		return nil, filer_pb.ErrNotFound
	}

	if err != nil {
		return nil, err
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

func (s *BadgerStore) DeleteEntry(ctx context.Context, path util.FullPath) error {
	dir, name := path.DirAndName()
	key := generateKey(dir, name)

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (s *BadgerStore) DeleteFolderChildren(ctx context.Context, path util.FullPath) error {
	directoryPrefix := generateKey(string(path), "")

	return s.db.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = directoryPrefix
		opts.PrefetchValues = false
		iter := txn.NewIterator(opts)
		defer iter.Close()
		for iter.Seek(directoryPrefix); iter.ValidForPrefix(directoryPrefix); iter.Next() {
			if err := txn.Delete(iter.Item().KeyCopy(nil)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BadgerStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string,
	includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (string, error) {
	lastFileName := ""
	directoryPrefix := generateKey(string(dirPath), prefix)
	lastFileStart := directoryPrefix
	if startFileName != "" {
		lastFileStart = generateKey(string(dirPath), startFileName)
	}

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = directoryPrefix
		opts.PrefetchValues = false
		iter := txn.NewIterator(opts)
		defer iter.Close()
		i := int64(0)
		first := true
		for iter.Seek(lastFileStart); iter.ValidForPrefix(directoryPrefix); iter.Next() {
			if first {
				first = false
				if !includeStartFile && bytes.Equal(iter.Item().Key(), lastFileStart) {
					continue
				}
			}
			if limit > 0 {
				i++
				if i > limit {
					break
				}
			}
			fileName := string(iter.Item().Key()[len(dirPath)+1:])
			if fileName != "" {
				value, err := iter.Item().ValueCopy(nil)
				if err != nil {
					return err
				}
				// Got file name, then generate the Entry
				entry := &filer.Entry{
					FullPath: util.NewFullPath(string(dirPath), fileName),
				}
				// Update lastFileName
				lastFileName = fileName
				// Check for decode value.
				if decodeErr := entry.DecodeAttributesAndChunks(value); decodeErr != nil {
					return decodeErr
				}
				// Run for each callback if return false just break the iteration
				if !eachEntryFunc(entry) {
					break
				}
			}
			// End process
		}
		return nil
	})

	if err != nil {
		return lastFileName, fmt.Errorf("prefix list %s : %v", dirPath, err)
	}
	return lastFileName, nil
}

func (s *BadgerStore) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (string, error) {
	return s.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, "", eachEntryFunc)
}

func (s *BadgerStore) Shutdown() {
	s.db.Close()
}
