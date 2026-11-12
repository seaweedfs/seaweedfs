package localsink

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/repl_util"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var sourceLookupTimeout = 30 * time.Second

type LocalSink struct {
	Dir           string
	filerSource   *source.FilerSource
	isIncremental bool
}

func init() {
	sink.Sinks = append(sink.Sinks, &LocalSink{})
}

func (localsink *LocalSink) SetSourceFiler(s *source.FilerSource) {
	localsink.filerSource = s
}

func (localsink *LocalSink) GetName() string {
	return "local"
}

func (localsink *LocalSink) isMultiPartEntry(key string) bool {
	return strings.HasSuffix(key, ".part") && strings.Contains(key, "/"+s3_constants.MultipartUploadsFolder+"/")
}

func (localsink *LocalSink) isInsideDir(key string) (string, bool) {
	rel, err := filepath.Rel(filepath.Clean(localsink.Dir), filepath.Clean(key))
	if err == nil && filepath.IsLocal(rel) {
		return rel, true
	}
	glog.Warningf("local sink: skip %s outside %s", key, localsink.Dir)
	return "", false
}

// resolveInsideDir resolves symlinks in the parent of rel while it stays inside the sink directory.
func (localsink *LocalSink) resolveInsideDir(rel string) string {
	base, err := filepath.EvalSymlinks(localsink.Dir)
	if err != nil {
		return rel
	}
	dir, rest := filepath.Dir(rel), filepath.Base(rel)
	for dir != "." {
		if resolved, err := filepath.EvalSymlinks(filepath.Join(base, dir)); err == nil {
			if inside, err := filepath.Rel(base, resolved); err == nil && filepath.IsLocal(inside) {
				return filepath.Join(inside, rest)
			}
			return rel
		}
		dir, rest = filepath.Dir(dir), filepath.Join(filepath.Base(dir), rest)
	}
	return rel
}

func (localsink *LocalSink) initialize(dir string, isIncremental bool) error {
	localsink.Dir = dir
	localsink.isIncremental = isIncremental
	return nil
}

func (localsink *LocalSink) Initialize(configuration util.Configuration, prefix string) error {
	dir := configuration.GetString(prefix + "directory")
	isIncremental := configuration.GetBool(prefix + "is_incremental")
	glog.V(4).Infof("sink.local.directory: %v", dir)
	return localsink.initialize(dir, isIncremental)
}

func (localsink *LocalSink) GetSinkToDirectory() string {
	return localsink.Dir
}

func (localsink *LocalSink) GetDestinationIdentity() string {
	return localsink.Dir
}

func (localsink *LocalSink) IsIncremental() bool {
	return localsink.isIncremental
}

func (localsink *LocalSink) DeleteEntry(key string, isDirectory, deleteIncludeChunks bool, signatures []int32) error {
	return localsink.deleteEntry(key, nil, 0)
}

func (localsink *LocalSink) DeleteEntryVersion(key string, oldEntry *filer_pb.Entry, tsNs int64, deleteIncludeChunks bool, signatures []int32) error {
	return localsink.deleteEntry(key, oldEntry, tsNs)
}

func (localsink *LocalSink) deleteEntry(sourceKey string, oldEntry *filer_pb.Entry, tsNs int64) error {
	key := sanitizeFsKey(sourceKey)
	if localsink.isMultiPartEntry(key) {
		return nil
	}
	rel, ok := localsink.isInsideDir(key)
	if !ok {
		return nil
	}
	glog.V(4).Infof("Delete Entry key: %s", key)
	root, err := os.OpenRoot(localsink.Dir)
	if err != nil {
		glog.V(0).Infof("remove entry key %s: %s", key, err)
		return nil
	}
	defer root.Close()
	rel = localsink.resolveInsideDir(rel)
	if oldEntry != nil {
		if _, statErr := root.Lstat(rel); errors.Is(statErr, fs.ErrNotExist) {
			return nil
		}
		recreated, lookupErr := localsink.sourceRecreated(sourceKey, oldEntry, tsNs)
		if lookupErr != nil {
			return lookupErr
		}
		if recreated {
			glog.V(1).Infof("keep %s: the source recreated it after the deleted version", key)
			return nil
		}
	}
	if err := root.Remove(rel); err != nil {
		glog.V(0).Infof("remove entry key %s: %s", key, err)
	}
	return nil
}

// sourceRecreated reports whether the live source holds an entry of the same
// kind at key's source path committed after the delete at tsNs, the order the
// source's own replay keeps; an unstamped live entry, written by a filer that
// predates commit stamps, counts from its mtime. Without tsNs the deleted
// entry's commit stamp is the bound; when the deleted entry is unstamped too,
// a stamped live entry was written after it, and mtimes decide otherwise. An
// older live entry means the source store lags the delete event or will apply
// it next.
func (localsink *LocalSink) sourceRecreated(key string, oldEntry *filer_pb.Entry, tsNs int64) (bool, error) {
	sourcePath, ok := localsink.sourcePath(key)
	if !ok {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), sourceLookupTimeout)
	defer cancel()
	live, _, _, err := filer_pb.GetEntry(ctx, localsink.filerSource, sourcePath)
	if errors.Is(err, filer_pb.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("look up source %s before deleting %s: %w", sourcePath, key, err)
	}
	if live == nil || live.IsDirectory != oldEntry.IsDirectory {
		return false, nil
	}
	if tsNs <= 0 {
		tsNs = oldEntry.GetCommitTsNs()
	}
	liveTsNs := live.GetCommitTsNs()
	if tsNs > 0 {
		if liveTsNs == 0 {
			liveTsNs = mtimeNs(live)
		}
		return liveTsNs > tsNs, nil
	}
	if liveTsNs > 0 {
		return true, nil
	}
	return mtimeNs(live) > mtimeNs(oldEntry), nil
}

// sourcePath maps a sink key back to the source path it replicates. Incremental
// keys carry a date directory and do not map back.
func (localsink *LocalSink) sourcePath(key string) (util.FullPath, bool) {
	if localsink.filerSource == nil || localsink.isIncremental {
		return "", false
	}
	rel, err := filepath.Rel(filepath.Clean(localsink.Dir), filepath.Clean(key))
	if err != nil || rel == "." || !filepath.IsLocal(rel) {
		return "", false
	}
	return util.FullPath(localsink.filerSource.Dir).Child(filepath.ToSlash(rel)), true
}

func mtimeNs(entry *filer_pb.Entry) int64 {
	return entry.GetAttributes().GetMtime()*int64(time.Second) + int64(entry.GetAttributes().GetMtimeNs())
}

func (localsink *LocalSink) CreateEntry(key string, entry *filer_pb.Entry, signatures []int32) error {
	key = sanitizeFsKey(key)
	if entry.IsDirectory || localsink.isMultiPartEntry(key) {
		return nil
	}
	rel, ok := localsink.isInsideDir(key)
	if !ok {
		return nil
	}
	glog.V(4).Infof("Create Entry key: %s", key)

	totalSize := filer.FileSize(entry)
	chunkViews, err := filer.ViewFromChunks(context.Background(), localsink.filerSource.LookupFileId, entry.GetChunks(), 0, int64(totalSize))
	if err != nil {
		return fmt.Errorf("resolve chunks of %s: %w", key, err)
	}

	if err := os.MkdirAll(localsink.Dir, 0755); err != nil {
		return err
	}
	root, err := os.OpenRoot(localsink.Dir)
	if err != nil {
		return err
	}
	defer root.Close()

	rel = localsink.resolveInsideDir(rel)
	dir := filepath.Dir(rel)
	if err := root.MkdirAll(dir, 0755); err != nil {
		return err
	}

	mode := os.FileMode(entry.Attributes.FileMode)
	shortFileName := util.ToShortFileName(rel)

	// Write to a temp file in the same directory, then atomically rename
	// on success. This prevents leaving a truncated/empty file if
	// decryption or chunk copy fails.
	tmpName := filepath.Join(dir, fmt.Sprintf(".seaweedfs-tmp-%d", rand.Uint64()))
	tmpFile, err := root.OpenFile(tmpName, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() {
		// Clean up temp file on any error (rename removes it on success)
		if tmpFile != nil {
			tmpFile.Close()
			root.Remove(tmpName)
		}
	}()

	if err := tmpFile.Chmod(mode); err != nil {
		return err
	}

	writeFunc := func(data []byte) error {
		_, writeErr := tmpFile.Write(data)
		return writeErr
	}

	if len(entry.Content) > 0 {
		content, err := repl_util.MaybeDecryptContent(entry.Content, entry)
		if err != nil {
			return fmt.Errorf("decrypt inline SSE content: %w", err)
		}
		if err := writeFunc(content); err != nil {
			return err
		}
	} else {
		if err := repl_util.CopyFromChunkViews(chunkViews, localsink.filerSource, writeFunc, entry); err != nil {
			return err
		}
	}

	// Close before rename so the data is flushed
	if err := tmpFile.Close(); err != nil {
		return err
	}
	tmpFile = nil // prevent deferred cleanup

	// Atomic rename into final destination
	if err := root.Rename(tmpName, shortFileName); err != nil {
		root.Remove(tmpName)
		return err
	}

	return nil
}

func (localsink *LocalSink) UpdateEntry(key string, oldEntry *filer_pb.Entry, newParentPath string, newEntry *filer_pb.Entry, deleteIncludeChunks bool, signatures []int32) (foundExistingEntry bool, err error) {
	key = sanitizeFsKey(key)
	if localsink.isMultiPartEntry(key) {
		return true, nil
	}
	if _, ok := localsink.isInsideDir(key); !ok {
		return true, nil
	}
	glog.V(4).Infof("Update Entry key: %s", key)
	// do delete and create
	foundExistingEntry = util.FileExists(key)
	err = localsink.CreateEntry(key, newEntry, signatures)
	return
}
