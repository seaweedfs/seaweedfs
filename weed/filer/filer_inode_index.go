package filer

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"sort"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

const inodeIndexKeyPrefix = "filer.inode.path."

type inodeIndexEntry struct {
	path  util.FullPath
	inode uint64
}

type inodeIndexRecord struct {
	Paths []string `json:"paths,omitempty"`
}

func inodeIndexKey(inode uint64) []byte {
	key := make([]byte, len(inodeIndexKeyPrefix)+8)
	copy(key, inodeIndexKeyPrefix)
	binary.BigEndian.PutUint64(key[len(inodeIndexKeyPrefix):], inode)
	return key
}

func decodeInodeIndexRecord(value []byte) (*inodeIndexRecord, error) {
	if len(value) == 0 {
		return &inodeIndexRecord{}, nil
	}

	// The first foundation slice stored the current path as raw bytes. Keep that
	// format readable so existing records are transparently upgraded on write.
	if value[0] != '{' {
		record := &inodeIndexRecord{}
		record.addPath(util.FullPath(value))
		return record, nil
	}

	record := &inodeIndexRecord{}
	if err := json.Unmarshal(value, record); err != nil {
		return nil, err
	}
	record.normalize()
	return record, nil
}

func (record *inodeIndexRecord) encode() ([]byte, error) {
	record.normalize()
	return json.Marshal(record)
}

func (record *inodeIndexRecord) normalize() {
	if len(record.Paths) == 0 {
		return
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

func (record *inodeIndexRecord) addPath(path util.FullPath) bool {
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

func (record *inodeIndexRecord) removePath(path util.FullPath) bool {
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

func (record *inodeIndexRecord) canonicalPath() util.FullPath {
	record.normalize()
	if len(record.Paths) == 0 {
		return ""
	}
	return util.FullPath(record.Paths[0])
}

func (record *inodeIndexRecord) fullPaths() []util.FullPath {
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

func (fsw *FilerStoreWrapper) lookupInodeIndex(ctx context.Context, inode uint64) (*inodeIndexRecord, error) {
	if inode == 0 {
		return nil, ErrKvNotFound
	}

	value, err := fsw.KvGet(ctx, inodeIndexKey(inode))
	if err != nil {
		return nil, err
	}

	return decodeInodeIndexRecord(value)
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
		record = &inodeIndexRecord{}
	}
	record.addPath(path)

	value, err := record.encode()
	if err != nil {
		return err
	}
	return fsw.KvPut(ctx, inodeIndexKey(inode), value)
}

func (fsw *FilerStoreWrapper) lookupInodePath(ctx context.Context, inode uint64) (util.FullPath, error) {
	record, err := fsw.lookupInodeIndex(ctx, inode)
	if err != nil {
		return "", err
	}

	path := record.canonicalPath()
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

	paths := record.fullPaths()
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
		return fsw.KvDelete(ctx, inodeIndexKey(inode))
	}

	value, err := record.encode()
	if err != nil {
		return err
	}
	return fsw.KvPut(ctx, inodeIndexKey(inode), value)
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
