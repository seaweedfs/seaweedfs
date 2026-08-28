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

// isRetryableListError classifies by message via util.IsTransientError because
// DoSeaweedListWithSnapshot wraps a failed ListEntries call with %v, dropping
// the gRPC status from the chain. Not-found is authoritative and must reach the
// caller unchanged.
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

		return doDeleteEntry(context.Background(), client, parentDirectoryPath, entryName, isDeleteData, isRecursive)
	})

}

func (s3a *S3ApiServer) rmObject(parentDirectoryPath, entryName string, isDeleteData, isRecursive bool) error {

	return s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		return deleteObjectEntry(client, parentDirectoryPath, entryName, isDeleteData, isRecursive)
	})

}

func deleteObjectEntry(client filer_pb.SeaweedFilerClient, parentDirectoryPath, entryName string, isDeleteData, isRecursive bool) error {
	err := doDeleteEntry(context.Background(), client, parentDirectoryPath, entryName, isDeleteData, isRecursive)
	if err == nil {
		return nil
	}
	if !strings.Contains(err.Error(), filer.MsgFailDelNonEmptyFolder) {
		return err
	}

	return demoteDirectoryMarkerToImplicitDirectory(client, parentDirectoryPath, entryName)
}

// A delete is idempotent at the filer, so replaying one whose reply was lost in
// transit cannot turn into a spurious failure: FilerServer.DeleteEntry leaves
// resp.Error empty when the entry is already gone. That rests on it comparing
// the sentinel by identity (err != filer_pb.ErrNotFound) rather than with
// errors.Is, so a future wrap of that error on the filer side would make the
// second attempt report a failure instead. Worth re-checking if this is ever
// revisited.
//
// The replay carries no backoff of its own. What clears a transient failure
// here is the gRPC channel reconnecting underneath the call, not the passage of
// time, and a sleep would be paid once per key in handlers that delete up to
// deleteMultipleObjectsLimit entries per request.
//
// That does not make the replay free. A pick that finds no ready subconn
// returns balancer.ErrNoSubConnAvailable, and the picker then waits for the
// balancer to publish a new one; WaitForReady(false) does not shorten that wait,
// and its only escape is the context. So while a channel is CONNECTING an
// attempt blocks rather than failing fast, and replaying would multiply that
// stall by deleteRetryAttempts. deleteReplayTimeout bounds it instead.
const deleteRetryAttempts = 3

// deleteReplayTimeout bounds every replay of one delete taken together, so this
// replay can add at most this much to a request however the channel behaves.
//
// The first attempt deliberately keeps the caller's own context: a recursive
// delete of a large bucket is legitimately slow, and putting a deadline on it
// here would fail deletes that succeed today. Only the attempts this change
// adds are bounded, which is what keeps it from making anything worse than it
// already is. Deriving from the caller's context rather than replacing it means
// a real request context, once one is threaded in, still cancels the replay and
// an earlier deadline of its own still wins.
const deleteReplayTimeout = 2 * time.Second

// isRetryableDeleteRPCError reports whether a failed DeleteEntry call is worth
// replaying. It classifies by gRPC status code, never by message text.
//
// Every failure it can see carries a code no client input can influence.
// FilerServer.DeleteEntry answers (resp, nil) in every case, its own failures
// included, so a non-nil error out of the call was produced by the gRPC stack
// itself. Context cancellation arrives as Canceled or DeadlineExceeded and is
// not replayed: the caller is already gone.
//
// The filer's own failures arrive in resp.Error instead and are never replayed.
// They are its considered answer about the tree, and they are free text with
// the deleted path formatted into them - and, for a recursive delete, the paths
// of the children it stopped on (weed/filer/filer_delete_entry.go builds
// "delete file %s: %v", "list folder %s: %v" and MsgFailDelNonEmptyFolder that
// way, and weed/server/filer_grpc_server.go copies the result into resp.Error
// verbatim). Substring-matching that text would let an object named
// "transport.log" make a permission denial look transient, and one named after
// filer_pb.ErrNotFound make a genuine connection reset look authoritative.
//
// The set matches what pb.shouldInvalidateConnection treats as a broken
// channel, less Aborted:
//
//   - Unavailable is the ordinary transport failure, and the reason this exists.
//   - Internal is what grpc-go reports when a server ends a stream without
//     trailers, which is how an L7 gRPC proxy truncating a reply arrives - the
//     "error while returning the response" shape issue #7204 describes.
//   - Aborted is excluded: it reports a concurrency conflict the filer decided
//     on, not a channel that broke, so replaying it would just lose the race
//     again.
//   - ResourceExhausted is included for symmetry with the connection check but
//     is unreachable today, as nothing filer-side sheds load with it. If a shed
//     is ever added, an immediate zero-backoff replay is the wrong answer to
//     backpressure and this line should become a delay instead.
//
// Context cancellation arrives as Canceled or DeadlineExceeded and is not
// replayed: the caller is already gone.
func isRetryableDeleteRPCError(err error) bool {
	if err == nil {
		return false
	}
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch s.Code() {
	case codes.Unavailable, codes.Internal, codes.ResourceExhausted:
		return true
	}
	return false
}

func doDeleteEntry(ctx context.Context, client filer_pb.SeaweedFilerClient, parentDirectoryPath string, entryName string, isDeleteData bool, isRecursive bool) error {
	request := &filer_pb.DeleteEntryRequest{
		Directory:            parentDirectoryPath,
		Name:                 entryName,
		IsDeleteData:         isDeleteData,
		IsRecursive:          isRecursive,
		IgnoreRecursiveError: true,
	}

	glog.V(1).Infof("delete entry %v/%v: %v", parentDirectoryPath, entryName, request)

	// Master logged every failed delete RPC at V(1); keep that, so a delete that
	// ends in failure still says so here whatever the replay did.
	fail := func(cause string) error {
		glog.V(1).Infof("delete entry %s/%s: %s", parentDirectoryPath, entryName, cause)
		return fmt.Errorf("delete entry %s/%s: %s", parentDirectoryPath, entryName, cause)
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return fail(ctxErr.Error())
	}

	// The first attempt is unconditional and runs on the caller's own context,
	// so no attempt count can turn a delete that was never issued into a
	// reported success, and a legitimately slow recursive delete keeps the
	// budget it has today.
	rpcErr, filerErr := deleteEntryAttempt(ctx, client, request)
	if rpcErr == nil {
		if filerErr == "" {
			return nil
		}
		return fail(filerErr)
	}
	if !isRetryableDeleteRPCError(rpcErr) {
		return fail(rpcErr.Error())
	}

	// Every replay shares this one bounded context, so the wait they add is
	// capped whether or not the caller supplied a deadline of its own.
	replayCtx, cancelReplays := context.WithTimeout(ctx, deleteReplayTimeout)
	defer cancelReplays()

	for attempt := 2; attempt <= deleteRetryAttempts; attempt++ {
		glog.V(2).Infof("delete entry %s/%s attempt %d/%d hit a transient error, replaying: %v", parentDirectoryPath, entryName, attempt-1, deleteRetryAttempts, rpcErr)
		if ctxErr := replayCtx.Err(); ctxErr != nil {
			return fail(ctxErr.Error())
		}

		rpcErr, filerErr = deleteEntryAttempt(replayCtx, client, request)
		if rpcErr == nil {
			if filerErr == "" {
				return nil
			}
			return fail(filerErr)
		}
		if !isRetryableDeleteRPCError(rpcErr) {
			break
		}
	}
	return fail(rpcErr.Error())
}

// deleteEntryAttempt makes one DeleteEntry call and keeps the two ways it can
// fail apart. Only the RPC error is safe to classify; the filer's own message
// is returned as a plain string so it cannot be mistaken for one, because
// FilerServer.DeleteEntry reports its failures there with the deleted path
// formatted in. See isRetryableDeleteRPCError.
func deleteEntryAttempt(ctx context.Context, client filer_pb.SeaweedFilerClient, request *filer_pb.DeleteEntryRequest) (rpcErr error, filerErr string) {
	resp, err := client.DeleteEntry(ctx, request)
	if err != nil {
		return err, ""
	}
	return nil, resp.Error
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
