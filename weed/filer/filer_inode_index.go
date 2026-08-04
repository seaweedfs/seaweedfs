package filer

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"sort"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const inodeIndexKeyPrefix = "filer.inode.path."
const InodeIndexInitialGeneration uint64 = 1

// inodeIndexPrefixes scopes which filer paths get an inode→path reverse-lookup
// index row. It is populated by SetInodeIndexPrefixes (called from the filer
// command's -nfs.inodeIndexPrefixes flag).
//
// When the slice is empty (the default) NO index rows are ever written: every
// storeInodeIndex/recordInodeIndexWrite call returns before touching the KV
// store, so behaviour is identical to upstream 4.38 (which has no index at
// all). When non-empty, only entries whose full path starts with one of the
// configured prefixes are indexed — this is the opt-in mechanism that confines
// the index bloat to NFS-exported subtrees (see design doc §3.1.2 选项 A).
//
// Reads are intentionally NOT gated here: a lookup for an inode that was never
// indexed simply misses and the caller falls back to its normal handling.
var inodeIndexPrefixes []string

// SetInodeIndexPrefixes configures the global prefix filter for the inode→path
// index. Pass nil or an empty slice (or entries that normalize to empty) to
// disable indexing entirely. The slice is copied and sorted so callers cannot
// mutate it later and so pathInInodeIndexScope's prefix scan is deterministic.
// This function is safe to call once during filer startup before any store
// mutation runs; it is not designed for concurrent mutation at runtime.
func SetInodeIndexPrefixes(prefixes []string) {
	cleaned := make([]string, 0, len(prefixes))
	for _, p := range prefixes {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		cleaned = append(cleaned, p)
	}
	sort.Strings(cleaned)
	// Dedup adjacent equal entries after sorting.
	deduped := make([]string, 0, len(cleaned))
	for _, p := range cleaned {
		if n := len(deduped); n > 0 && deduped[n-1] == p {
			continue
		}
		deduped = append(deduped, p)
	}
	// Keep the disabled state a clean nil so inodeIndexEnabled() and any
	// external nil-check see the documented "not configured" default.
	if len(deduped) == 0 {
		inodeIndexPrefixes = nil
	} else {
		inodeIndexPrefixes = deduped
	}
}

// inodeIndexEnabled reports whether any index rows are written at all. When
// false the filer behaves exactly like upstream 4.38 (no reverse-lookup index),
// which is the default and the hard guarantee that the index tax is opt-in.
func inodeIndexEnabled() bool {
	return len(inodeIndexPrefixes) > 0
}

// pathInInodeIndexScope reports whether fullpath falls under one of the
// configured index prefixes. With an empty prefix set this is always false.
//
// Matching honours path boundaries: the prefix "/exports" matches "/exports"
// and "/exports/docs" but NOT "/exportsx" — a sibling that merely shares a
// stem. This stops an operator-configured subtree from accidentally indexing
// unrelated namespaces with the same leading characters.
func pathInInodeIndexScope(fullpath util.FullPath) bool {
	if !inodeIndexEnabled() {
		return false
	}
	s := string(fullpath)
	for _, prefix := range inodeIndexPrefixes {
		if pathHasDirPrefix(s, prefix) {
			return true
		}
	}
	return false
}

// pathHasDirPrefix reports whether path == prefix or path is a descendant of
// prefix, treating prefix as a directory boundary. A prefix that already ends
// in "/" (e.g. "/exports/") is matched as-is against that exact stem.
func pathHasDirPrefix(path, prefix string) bool {
	if path == prefix {
		return true
	}
	if prefix == "" {
		return false
	}
	boundary := prefix
	if boundary[len(boundary)-1] != '/' {
		boundary += "/"
	}
	return strings.HasPrefix(path, boundary)
}

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
	// Index writes are opt-in: with no configured prefix (the default) nothing
	// is written, so behaviour matches upstream 4.38 exactly. Even when
	// indexing is enabled, only in-scope paths get a row.
	if !pathInInodeIndexScope(path) {
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
	// Index housekeeping is opt-in: with indexing disabled (the default) there
	// is nothing to clean up, so skip the recursive walk entirely — this keeps
	// DeleteFolderChildren free of any index-related KV/store traffic and makes
	// the disabled case byte-for-byte equivalent to upstream 4.38.
	if !inodeIndexEnabled() {
		return nil, nil
	}
	// Honor caller cancellation during the walk: a DeleteFolderChildren on a
	// pathological directory could otherwise loop indefinitely gathering
	// entries even after the client has given up, turning into a DoS vector.
	// If the walk is aborted, the caller treats the index cleanup as
	// best-effort and drops the partial result.
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

// recordInodeIndexWrite updates the inode→path secondary index after the
// primary store mutation has already succeeded. The index is best-effort: a
// failure here must not surface as an operation error, because the caller
// would then observe a failed create/update even though the entry was
// persisted, and a retry cannot heal the index (DeleteEntry exits early once
// the entry is gone). We log and let later writes rebuild the record.
func (fsw *FilerStoreWrapper) recordInodeIndexWrite(ctx context.Context, op string, path util.FullPath, inode uint64) {
	if !inodeIndexEnabled() {
		return
	}
	if inode == 0 || path == "" {
		return
	}
	if err := fsw.storeInodeIndex(ctx, path, inode); err != nil {
		glog.WarningfCtx(ctx, "%s: update inode index for %s (inode %d): %v", op, path, inode, err)
	}
}

// recordInodeIndexRemoval mirrors recordInodeIndexWrite for removals.
func (fsw *FilerStoreWrapper) recordInodeIndexRemoval(ctx context.Context, op string, path util.FullPath, inode uint64) {
	if !inodeIndexEnabled() {
		return
	}
	if inode == 0 || path == "" {
		return
	}
	if err := fsw.removePathFromInodeIndex(ctx, path, inode); err != nil {
		glog.WarningfCtx(ctx, "%s: clear inode index for %s (inode %d): %v", op, path, inode, err)
	}
}
