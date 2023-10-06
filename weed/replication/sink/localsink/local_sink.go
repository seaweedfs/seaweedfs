package localsink

import (
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/repl_util"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"os"
	"path/filepath"
	"strings"
)

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

func (localsink *LocalSink) IsIncremental() bool {
	return localsink.isIncremental
}

func (localsink *LocalSink) DeleteEntry(key string, isDirectory, deleteIncludeChunks bool, signatures []int32) error {
	if localsink.isMultiPartEntry(key) {
		return nil
	}
	glog.V(4).Infof("Delete Entry key: %s", key)
	if err := os.Remove(key); err != nil {
		glog.V(0).Infof("remove entry key %s: %s", key, err)
	}
	return nil
}

func (localsink *LocalSink) CreateEntry(key string, entry *filer_pb.Entry, signatures []int32) error {
	if entry.IsDirectory || localsink.isMultiPartEntry(key) {
		return nil
	}
	glog.V(4).Infof("Create Entry key: %s", key)

	totalSize := filer.FileSize(entry)
	chunkViews := filer.ViewFromChunks(localsink.filerSource.LookupFileId, entry.GetChunks(), 0, int64(totalSize))

	dir := filepath.Dir(key)

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		glog.V(4).Infof("Create Directory key: %s", dir)
		if err = os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	if entry.IsDirectory {
		return os.Mkdir(key, os.FileMode(entry.Attributes.FileMode))
	}

	mode := os.FileMode(entry.Attributes.FileMode)
	dstFile, err := os.OpenFile(util.ToShortFileName(key), os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	fi, err := dstFile.Stat()
	if err != nil {
		return err
	}
	if fi.Mode() != mode {
		glog.V(4).Infof("Modify file mode: %o -> %o", fi.Mode(), mode)
		if err := dstFile.Chmod(mode); err != nil {
			return err
		}
	}

	writeFunc := func(data []byte) error {
		_, writeErr := dstFile.Write(data)
		return writeErr
	}

	if len(entry.Content) > 0 {
		return writeFunc(entry.Content)
	}

	if err := repl_util.CopyFromChunkViews(chunkViews, localsink.filerSource, writeFunc); err != nil {
		return err
	}

	return nil
}

func (localsink *LocalSink) UpdateEntry(key string, oldEntry *filer_pb.Entry, newParentPath string, newEntry *filer_pb.Entry, deleteIncludeChunks bool, signatures []int32) (foundExistingEntry bool, err error) {
	if localsink.isMultiPartEntry(key) {
		return true, nil
	}
	glog.V(4).Infof("Update Entry key: %s", key)
	// do delete and create
	foundExistingEntry = util.FileExists(key)
	err = localsink.CreateEntry(key, newEntry, signatures)
	return
}
