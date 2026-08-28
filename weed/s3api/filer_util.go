package s3api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s3a *S3ApiServer) mkdir(parentDirectoryPath string, dirName string, fn func(entry *filer_pb.Entry)) error {

	return filer_pb.Mkdir(context.Background(), s3a, parentDirectoryPath, dirName, fn)

}

func (s3a *S3ApiServer) mkFile(parentDirectoryPath string, fileName string, chunks []*filer_pb.FileChunk, fn func(entry *filer_pb.Entry)) error {

	err := filer_pb.MkFile(context.Background(), s3a, parentDirectoryPath, fileName, chunks, fn)
	if errors.Is(err, filer_pb.ErrExistingIsDirectory) && !isReservedDirectoryName(fileName) {
		// Other keys are nested under this one, so the object goes onto the directory
		// they live in - the same place a PutObject of this key writes it.
		err = filer_pb.MkFile(context.Background(), s3a, parentDirectoryPath, fileName, chunks, func(entry *filer_pb.Entry) {
			if fn != nil {
				fn(entry)
			}
			entry.MarkPrefixObject()
		})
	}
	return err

}

func (s3a *S3ApiServer) list(parentDirectoryPath, prefix, startFrom string, inclusive bool, limit uint32) (entries []*filer_pb.Entry, isLast bool, err error) {

	return listWithRetry(parentDirectoryPath, func() (entries []*filer_pb.Entry, isLast bool, err error) {
		err = filer_pb.List(context.Background(), s3a, parentDirectoryPath, prefix, func(entry *filer_pb.Entry, isLastEntry bool) error {
			entries = append(entries, entry)
			if isLastEntry {
				isLast = true
			}
			return nil
		}, startFrom, inclusive, limit)

		if len(entries) == 0 {
			isLast = true
		}

		return
	})

}

// A listing has no side effects and collects into a fresh slice per attempt, so
// a replay can neither duplicate nor drop entries; the bound caps a filer that
// is genuinely down at two extra attempts and 300ms of added wait.
const (
	listRetryAttempts       = 3
	listRetryInitialBackoff = 100 * time.Millisecond
)

// isRetryableListError defers to util.IsTransientError, which reads the status
// the filer sent rather than the message DoSeaweedListWithSnapshot builds around
// it out of the bucket and prefix the client chose. Not-found is authoritative
// and must reach the caller unchanged.
func isRetryableListError(err error) bool {
	return err != nil && !isFilerNotFound(err) && util.IsTransientError(err)
}

// listWithRetry replays doList while the filer answers with a transient error.
// Both failure points, the ListEntries call itself and the stream.Recv that
// follows it, surface as a plain error out of filer_pb.List, so a single retry
// point above it covers both.
func listWithRetry(parentDirectoryPath string, doList func() (entries []*filer_pb.Entry, isLast bool, err error)) (entries []*filer_pb.Entry, isLast bool, err error) {

	backoff := listRetryInitialBackoff
	for attempt := 1; ; attempt++ {
		entries, isLast, err = doList()
		if err == nil || attempt >= listRetryAttempts || !isRetryableListError(err) {
			return entries, isLast, err
		}
		glog.V(1).Infof("list %s attempt %d/%d hit a transient error, retrying in %v: %v", parentDirectoryPath, attempt, listRetryAttempts, backoff, err)
		time.Sleep(backoff)
		backoff *= 2
	}

}

func (s3a *S3ApiServer) rm(parentDirectoryPath, entryName string, isDeleteData, isRecursive bool) error {

	return s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		return doDeleteEntry(client, parentDirectoryPath, entryName, isDeleteData, isRecursive)
	})

}

func (s3a *S3ApiServer) rmObject(parentDirectoryPath, entryName string, isDeleteData, isRecursive bool) error {

	return s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		return deleteObjectEntry(client, parentDirectoryPath, entryName, isDeleteData, isRecursive)
	})

}

func deleteObjectEntry(client filer_pb.SeaweedFilerClient, parentDirectoryPath, entryName string, isDeleteData, isRecursive bool) error {
	err := doDeleteEntry(client, parentDirectoryPath, entryName, isDeleteData, isRecursive)
	if err == nil {
		return nil
	}
	if !errors.Is(err, filer.ErrNonEmptyFolder) {
		return err
	}

	return demoteDirectoryMarkerToImplicitDirectory(client, parentDirectoryPath, entryName)
}

func doDeleteEntry(client filer_pb.SeaweedFilerClient, parentDirectoryPath string, entryName string, isDeleteData bool, isRecursive bool) error {
	request := &filer_pb.DeleteEntryRequest{
		Directory:            parentDirectoryPath,
		Name:                 entryName,
		IsDeleteData:         isDeleteData,
		IsRecursive:          isRecursive,
		IgnoreRecursiveError: true,
	}

	glog.V(1).Infof("delete entry %v/%v: %v", parentDirectoryPath, entryName, request)
	if resp, err := client.DeleteEntry(context.Background(), request); err != nil {
		glog.V(1).Infof("delete entry %v: %v", request, err)
		return fmt.Errorf("delete entry %s/%s: %w", parentDirectoryPath, entryName, err)
	} else {
		if resp.Error != "" {
			// the path wrapped in here is the client's, so classify the filer's
			// text now, while it still stands alone
			return fmt.Errorf("delete entry %s/%s: %w", parentDirectoryPath, entryName, filer.DeleteEntryError(resp.Error))
		}
	}
	return nil
}

func demoteDirectoryMarkerToImplicitDirectory(client filer_pb.SeaweedFilerClient, parentDirectoryPath, entryName string) error {
	resp, err := filer_pb.LookupEntry(context.Background(), client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: parentDirectoryPath,
		Name:      entryName,
	})
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("lookup entry %s/%s: %w", parentDirectoryPath, entryName, err)
	}
	if resp.Entry == nil || !resp.Entry.IsDirectory {
		return nil
	}
	if !resp.Entry.IsDirectoryKeyObject() {
		return nil
	}

	clearDirectoryMarkerMetadata(resp.Entry)

	if err := filer_pb.UpdateEntry(context.Background(), client, &filer_pb.UpdateEntryRequest{
		Directory: parentDirectoryPath,
		Entry:     resp.Entry,
	}); err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) || status.Code(err) == codes.NotFound {
			return nil
		}
		return fmt.Errorf("update entry %s/%s: %w", parentDirectoryPath, entryName, err)
	}
	return nil
}

func clearDirectoryMarkerMetadata(entry *filer_pb.Entry) {
	if entry == nil {
		return
	}
	if entry.Attributes == nil {
		entry.Attributes = &filer_pb.FuseAttributes{}
	}

	entry.Attributes.Mime = ""
	entry.Attributes.Md5 = nil
	entry.Attributes.FileSize = 0
	entry.Content = nil
	entry.Chunks = nil

	if len(entry.Extended) == 0 {
		return
	}

	filtered := make(map[string][]byte)
	for k, v := range entry.Extended {
		lowerKey := strings.ToLower(k)
		if lowerKey == s3_constants.SeaweedFSPrefixObject {
			// The path is a plain directory again, not a key of its own.
			continue
		}
		if strings.HasPrefix(lowerKey, "xattr-") || strings.HasPrefix(lowerKey, s3_constants.SeaweedFSInternalPrefix) {
			filtered[k] = v
		}
	}

	if len(filtered) == 0 {
		entry.Extended = nil
		return
	}
	entry.Extended = filtered
}

func (s3a *S3ApiServer) exists(parentDirectoryPath string, entryName string, isDirectory bool) (exists bool, err error) {

	return filer_pb.Exists(context.Background(), s3a, parentDirectoryPath, entryName, isDirectory)

}

func (s3a *S3ApiServer) getEntry(parentDirectoryPath, entryName string) (entry *filer_pb.Entry, err error) {
	fullPath := util.NewFullPath(parentDirectoryPath, entryName)
	entry, _, _, err = filer_pb.GetEntry(context.Background(), s3a, fullPath)
	return entry, err
}

func (s3a *S3ApiServer) updateEntry(parentDirectoryPath string, newEntry *filer_pb.Entry) error {
	updateEntryRequest := &filer_pb.UpdateEntryRequest{
		Directory: parentDirectoryPath,
		Entry:     newEntry,
	}

	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		err := filer_pb.UpdateEntry(context.Background(), client, updateEntryRequest)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

func (s3a *S3ApiServer) getCollectionName(bucket string) string {
	if s3a.option.FilerGroup != "" {
		return fmt.Sprintf("%s_%s", s3a.option.FilerGroup, bucket)
	}
	return bucket
}

func objectKey(key *string) *string {
	if strings.HasPrefix(*key, "/") {
		t := (*key)[1:]
		return &t
	}
	return key
}
