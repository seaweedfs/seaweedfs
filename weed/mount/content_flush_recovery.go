package mount

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

// contentPublicationError keeps the exact failed request, before any later
// subscription event can change the live handle. Unwrap preserves refusal and
// transport classification.
type contentPublicationError struct {
	cause   error
	request *filer_pb.CreateEntryRequest
}

func (e *contentPublicationError) Error() string { return e.cause.Error() }
func (e *contentPublicationError) Unwrap() error { return e.cause }

// retainFailedContent saves the refused request and its original CAS base for
// operator recovery to a separate path. It never retries against a fresh base.
// failed-metadata is outside the per-mount cache directories that shutdown and
// restart remove. Records are private, immutable and never automatically replayed
// or removed: chunk references may include cipher keys. They do not prevent a
// volume.fsck run from collecting unreferenced chunks; recover before that sweep.
func (wfs *WFS) retainFailedContent(fh *FileHandle, dir, name string, cause error) (string, error) {
	root, err := wfs.failedContentDir()
	if err != nil {
		return "", err
	}
	if err = os.MkdirAll(root, 0700); err != nil {
		return "", err
	}
	var request *filer_pb.CreateEntryRequest
	var publication *contentPublicationError
	if errors.As(cause, &publication) {
		// Snapshot the request that failed, not a handle a later metadata event may
		// already have changed after the publication lock was released.
		request = proto.Clone(publication.request).(*filer_pb.CreateEntryRequest)
	} else {
		var entry *filer_pb.Entry
		fh.UpdateEntry(func(e *filer_pb.Entry) { entry = proto.Clone(e).(*filer_pb.Entry) })
		entry.Name = name
		wfs.mapPbIdFromLocalToFiler(entry)
		request = &filer_pb.CreateEntryRequest{Directory: dir, Entry: entry, Condition: wfs.failedContentCondition(fh), Signatures: []int32{wfs.signature}, SkipCheckParentDirectory: true}
	}
	record := struct {
		Path            string
		Error           string
		ClientSignature int32
		Request         *filer_pb.CreateEntryRequest
	}{
		Path: string(util.FullPath(request.Directory).Child(request.Entry.Name)), Error: cause.Error(), ClientSignature: wfs.signature, Request: request,
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return "", err
	}
	file, err := os.CreateTemp(root, "pending-*.tmp")
	if err != nil {
		return "", err
	}
	temporary := file.Name()
	path := temporary + ".json"
	saved := false
	defer func() {
		file.Close()
		if !saved {
			os.Remove(temporary)
			os.Remove(path)
		}
	}()
	if _, err = file.Write(append(raw, '\n')); err != nil {
		return "", err
	}
	if err = file.Sync(); err != nil {
		return "", err
	}
	if err = file.Close(); err != nil {
		return "", err
	}
	if err = os.Rename(temporary, path); err != nil {
		return "", err
	}
	if err = util.FsyncDir(root); err != nil {
		return "", err
	}
	// Persist creation of failed-metadata itself when this is its first record.
	if err = util.FsyncDir(filepath.Dir(root)); err != nil {
		return "", err
	}
	saved = true
	return path, nil
}

// failedContentDir is per user: the write cache root defaults to the shared
// temp directory, where another user's private directory would refuse every save.
func (wfs *WFS) failedContentDir() (string, error) {
	if wfs.option.CacheDirForWrite == "" {
		return "", fmt.Errorf("write cache root unavailable for failed metadata retention")
	}
	name := "failed-metadata"
	if uid := os.Getuid(); uid >= 0 {
		name = fmt.Sprintf("%s-%d", name, uid)
	}
	return filepath.Abs(filepath.Join(wfs.option.CacheDirForWrite, name))
}

// Used only when failure happened before a publication request existed.
func (wfs *WFS) failedContentCondition(fh *FileHandle) *filer_pb.WriteCondition {
	clause := &filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_NOT_EXISTS}
	if base := fh.baseEntry.Load(); base != nil {
		expected := proto.Clone(base).(*filer_pb.Entry)
		wfs.mapPbIdFromLocalToFiler(expected)
		clause = &filer_pb.WriteCondition_Clause{Kind: filer_pb.WriteCondition_IF_ENTRY_EQUAL, ExpectedEntry: expected}
	}
	return &filer_pb.WriteCondition{Clauses: []*filer_pb.WriteCondition_Clause{clause}}
}
