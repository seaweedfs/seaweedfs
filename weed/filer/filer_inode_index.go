package filer

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"sort"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

const inodeIndexKeyPrefix = "filer.inode.path."
const InodeIndexInitialGeneration uint64 = 1

type inodeIndexEntry struct {
	path  util.FullPath
	inode uint64
}

type InodeIndexRecord struct {
	Generation uint64   `json:"generation,omitempty"`
	Paths      []string `json:"paths,omitempty"`
}

func InodeIndexKey(inode uint64) []byte {
	key := make([]byte, len(inodeIndexKeyPrefix)+8)
	copy(key, inodeIndexKeyPrefix)
	binary.BigEndian.PutUint64(key[len(inodeIndexKeyPrefix):], inode)
	return key
}

func DecodeInodeIndexRecord(value []byte) (*InodeIndexRecord, error) {
	if len(value) == 0 {
		return &InodeIndexRecord{}, nil
	}

	// The first foundation slice stored the current path as raw bytes. Keep that
	// format readable so existing records are transparently upgraded on write.
	if value[0] != '{' {
		record := &InodeIndexRecord{Generation: InodeIndexInitialGeneration}
		record.addPath(util.FullPath(value))
		return record, nil
	}

	record := &InodeIndexRecord{}
	if err := json.Unmarshal(value, record); err != nil {
		return nil, err
	}
	record.normalize()
	return record, nil
}

func (record *InodeIndexRecord) Encode() ([]byte, error) {
	record.normalize()
	return json.Marshal(record)
}

func (record *InodeIndexRecord) normalize() {
	if len(record.Paths) == 0 {
		return
	}
	if record.Generation == 0 {
		record.Generation = InodeIndexInitialGeneration
	}

	sanitized := make([]string, 0, len(record.Paths))
	for _, path := range record.Paths {
		if path == "" {
			continue
		}
		sanitized = append(sanitized, path)
	}
	if len(sanitized) == 0 {
		record.Paths = nil
		return
	}

	sort.Strings(sanitized)
	deduped := sanitized[:1]
	for _, path := range sanitized[1:] {
		if path == deduped[len(deduped)-1] {
			continue
		}
		deduped = append(deduped, path)
	}
	record.Paths = deduped
}

func (record *InodeIndexRecord) addPath(path util.FullPath) bool {
	if path == "" {
		return false
	}
	record.normalize()
	target := string(path)
	index := sort.SearchStrings(record.Paths, target)
	if index < len(record.Paths) && record.Paths[index] == target {
		return false
	}
	record.Paths = append(record.Paths, "")
	copy(record.Paths[index+1:], record.Paths[index:])
	record.Paths[index] = target
	return true
}

func (record *InodeIndexRecord) removePath(path util.FullPath) bool {
	if len(record.Paths) == 0 || path == "" {
		return false
	}
	record.normalize()
	target := string(path)
	index := sort.SearchStrings(record.Paths, target)
	if index >= len(record.Paths) || record.Paths[index] != target {
		return false
	}
	record.Paths = append(record.Paths[:index], record.Paths[index+1:]...)
	if len(record.Paths) == 0 {
		record.Paths = nil
	}
	return true
}

func (record *InodeIndexRecord) CanonicalPath() util.FullPath {
	record.normalize()
	if len(record.Paths) == 0 {
		return ""
	}
	return util.FullPath(record.Paths[0])
}

func (record *InodeIndexRecord) FullPaths() []util.FullPath {
	record.normalize()
	if len(record.Paths) == 0 {
		return nil
	}
	paths := make([]util.FullPath, 0, len(record.Paths))
	for _, path := range record.Paths {
		paths = append(paths, util.FullPath(path))
	}
	return paths
}

func (fsw *FilerStoreWrapper) lookupInodeIndex(ctx context.Context, inode uint64) (*InodeIndexRecord, error) {
	if inode == 0 {
		return nil, ErrKvNotFound
	}

	value, err := fsw.KvGet(ctx, InodeIndexKey(inode))
	if err != nil {
		return nil, err
	}

	return DecodeInodeIndexRecord(value)
}

func (fsw *FilerStoreWrapper) storeInodeIndex(ctx context.Context, path util.FullPath, inode uint64) error {
	if inode == 0 || path == "" {
		return nil
	}

	record, err := fsw.lookupInodeIndex(ctx, inode)
	if err != nil {
		if err != ErrKvNotFound {
			return err
		}
		record = &InodeIndexRecord{Generation: InodeIndexInitialGeneration}
	}
	record.addPath(path)

	value, err := record.Encode()
	if err != nil {
		return err
	}
	return fsw.KvPut(ctx, InodeIndexKey(inode), value)
}

func (fsw *FilerStoreWrapper) lookupInodePath(ctx context.Context, inode uint64) (util.FullPath, error) {
	record, err := fsw.lookupInodeIndex(ctx, inode)
	if err != nil {
		return "", err
	}

	path := record.CanonicalPath()
	if path == "" {
		return "", ErrKvNotFound
	}
	return path, nil
}

func (fsw *FilerStoreWrapper) lookupInodePaths(ctx context.Context, inode uint64) ([]util.FullPath, error) {
	record, err := fsw.lookupInodeIndex(ctx, inode)
	if err != nil {
		return nil, err
	}

	paths := record.FullPaths()
	if len(paths) == 0 {
		return nil, ErrKvNotFound
	}
	return paths, nil
}

func (fsw *FilerStoreWrapper) removePathFromInodeIndex(ctx context.Context, path util.FullPath, inode uint64) error {
	if inode == 0 || path == "" {
		return nil
	}

	record, err := fsw.lookupInodeIndex(ctx, inode)
	if err != nil {
		if err == ErrKvNotFound {
			return nil
		}
		return err
	}

	if !record.removePath(path) {
		return nil
	}
	if len(record.Paths) == 0 {
		return fsw.KvDelete(ctx, InodeIndexKey(inode))
	}

	value, err := record.Encode()
	if err != nil {
		return err
	}
	return fsw.KvPut(ctx, InodeIndexKey(inode), value)
}

func (fsw *FilerStoreWrapper) collectInodeIndexEntries(ctx context.Context, dirPath util.FullPath) ([]inodeIndexEntry, error) {
	ctx = context.WithoutCancel(ctx)

	var collected []inodeIndexEntry
	if err := fsw.collectInodeIndexEntriesRecursive(ctx, dirPath, &collected); err != nil {
		return nil, err
	}
	return collected, nil
}

func (fsw *FilerStoreWrapper) collectInodeIndexEntriesRecursive(ctx context.Context, dirPath util.FullPath, collected *[]inodeIndexEntry) error {
	actualStore := fsw.getActualStore(dirPath + "/")

	lastFileName := ""
	includeStartFile := false
	for {
		page := make([]*Entry, 0, PaginationSize)
		nextLastFileName, err := actualStore.ListDirectoryEntries(ctx, dirPath, lastFileName, includeStartFile, PaginationSize, func(entry *Entry) (bool, error) {
			page = append(page, entry)
			return true, nil
		})
		if err != nil {
			return err
		}

		for _, entry := range page {
			if entry.Attr.Inode != 0 {
				*collected = append(*collected, inodeIndexEntry{path: entry.FullPath, inode: entry.Attr.Inode})
			}
			if entry.IsDirectory() {
				if err := fsw.collectInodeIndexEntriesRecursive(ctx, entry.FullPath, collected); err != nil {
					return err
				}
			}
		}

		if len(page) < PaginationSize {
			return nil
		}
		lastFileName = nextLastFileName
		includeStartFile = false
	}
}
