package filersink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/schollz/progressbar/v3"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func (fs *FilerSink) replicateChunks(ctx context.Context, sourceChunks []*filer_pb.FileChunk, path string, sourceMtimeNs int64) (replicatedChunks []*filer_pb.FileChunk, err error) {
	if len(sourceChunks) == 0 {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// a simple progress bar. Not ideal. Fix me.
	var bar *progressbar.ProgressBar
	if len(sourceChunks) > 1 {
		name := filepath.Base(path)
		bar = progressbar.NewOptions64(int64(len(sourceChunks)),
			progressbar.OptionClearOnFinish(),
			progressbar.OptionOnCompletion(func() {
				fmt.Fprint(os.Stderr, "\n")
			}),
			progressbar.OptionFullWidth(),
			progressbar.OptionSetDescription(name),
		)
	}

	replicatedChunks = make([]*filer_pb.FileChunk, len(sourceChunks))
	var errLock sync.Mutex
	setError := func(e error) {
		if e == nil {
			return
		}
		errLock.Lock()
		if err == nil {
			err = e
		}
		errLock.Unlock()
	}
	hasError := func() bool {
		errLock.Lock()
		defer errLock.Unlock()
		return err != nil
	}

	var wg sync.WaitGroup
	for chunkIndex, sourceChunk := range sourceChunks {
		if hasError() {
			break
		}

		if sourceChunk.IsChunkManifest {
			replicatedChunk, replicateErr := fs.replicateOneManifestChunk(ctx, sourceChunk, path, sourceMtimeNs)
			if replicateErr != nil {
				setError(replicateErr)
				break
			}
			replicatedChunks[chunkIndex] = replicatedChunk
			if bar != nil {
				bar.Add(1)
			}
			continue
		}

		wg.Add(1)
		index, source := chunkIndex, sourceChunk
		fs.executor.Execute(func() {
			defer wg.Done()
			var replicatedChunk *filer_pb.FileChunk
			retryErr := util.Retry("replicate chunks", func() error {
				chunk, e := fs.replicateOneChunk(source, path, sourceMtimeNs)
				if e != nil {
					return e
				}
				replicatedChunk = chunk
				return nil
			})
			if retryErr != nil {
				setError(retryErr)
				return
			}

			replicatedChunks[index] = replicatedChunk
			if bar != nil {
				bar.Add(1)
			}
		})
	}
	wg.Wait()

	return
}

func (fs *FilerSink) replicateOneChunk(sourceChunk *filer_pb.FileChunk, path string, sourceMtimeNs int64) (*filer_pb.FileChunk, error) {

	fileId, err := fs.fetchAndWrite(sourceChunk, path, sourceMtimeNs)
	if err != nil {
		return nil, fmt.Errorf("copy %s: %w", sourceChunk.GetFileIdString(), err)
	}

	return &filer_pb.FileChunk{
		FileId:       fileId,
		Offset:       sourceChunk.Offset,
		Size:         sourceChunk.Size,
		ModifiedTsNs: sourceChunk.ModifiedTsNs,
		ETag:         sourceChunk.ETag,
		SourceFileId: sourceChunk.GetFileIdString(),
		CipherKey:    sourceChunk.CipherKey,
		IsCompressed: sourceChunk.IsCompressed,
		SseType:      sourceChunk.SseType,
		SseMetadata:  sourceChunk.SseMetadata,
	}, nil
}

func (fs *FilerSink) replicateOneManifestChunk(ctx context.Context, sourceChunk *filer_pb.FileChunk, path string, sourceMtimeNs int64) (*filer_pb.FileChunk, error) {
	// LookupFileId can transiently fail during a write burst (volume not yet
	// registered). Retry like the data-chunk path does; the gate stops on
	// supersession or, when that cannot be checked, after a few attempts.
	var resolvedChunks []*filer_pb.FileChunk
	resolveName := fmt.Sprintf("resolve manifest %s", sourceChunk.GetFileIdString())
	err := util.RetryUntil(resolveName, func() error {
		rc, e := filer.ResolveOneChunkManifest(ctx, fs.filerSource.LookupFileId, sourceChunk)
		if e != nil {
			return e
		}
		resolvedChunks = rc
		return nil
	}, fs.manifestResolveRetryGate(path, sourceMtimeNs, sourceChunk.GetFileIdString()))
	if err != nil {
		return nil, fmt.Errorf("resolve manifest %s: %w", sourceChunk.GetFileIdString(), err)
	}

	replicatedResolvedChunks, err := fs.replicateChunks(ctx, resolvedChunks, path, sourceMtimeNs)
	if err != nil {
		return nil, fmt.Errorf("replicate manifest data chunks %s: %w", sourceChunk.GetFileIdString(), err)
	}

	manifestDataChunks := make([]*filer_pb.FileChunk, len(replicatedResolvedChunks))
	for i, chunk := range replicatedResolvedChunks {
		copied := *chunk
		manifestDataChunks[i] = &copied
	}
	filer_pb.BeforeEntrySerialization(manifestDataChunks)
	manifestData, err := proto.Marshal(&filer_pb.FileChunkManifest{
		Chunks: manifestDataChunks,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal manifest %s: %w", sourceChunk.GetFileIdString(), err)
	}

	manifestFileId, err := fs.uploadManifestChunk(path, sourceMtimeNs, sourceChunk.GetFileIdString(), manifestData)
	if err != nil {
		return nil, err
	}

	return &filer_pb.FileChunk{
		FileId:          manifestFileId,
		Offset:          sourceChunk.Offset,
		Size:            sourceChunk.Size,
		ModifiedTsNs:    sourceChunk.ModifiedTsNs,
		ETag:            sourceChunk.ETag,
		SourceFileId:    sourceChunk.GetFileIdString(),
		IsChunkManifest: true,
	}, nil
}

// maxUnverifiableResolveAttempts bounds manifest-resolve retries when
// supersession cannot be checked for the target path.
const maxUnverifiableResolveAttempts = 3

// manifestResolveRetryGate decides whether a failing manifest resolve keeps
// retrying: stop when the source superseded the replayed version (the caller
// skips it as lossless), stop immediately on non-transient errors (e.g. corrupt
// manifest data) so the configured metadata error policy applies, and stop
// after a few attempts when supersession cannot be checked at all (incremental
// dated target keys don't map back to a source path) — propagating lets
// filer.backup decide with the event's real source key instead of spinning
// here forever.
func (fs *FilerSink) manifestResolveRetryGate(path string, sourceMtimeNs int64, chunkName string) func(error) bool {
	_, canCheckSupersession := fs.targetPathToSourcePath(path)
	attempts := 0
	return func(resolveErr error) (shouldContinue bool) {
		if fs.hasSourceNewerVersion(path, sourceMtimeNs) {
			glog.V(1).Infof("skip retrying stale source manifest %s for %s: %v", chunkName, path, resolveErr)
			return false
		}
		if !isTransientResolveError(resolveErr) {
			glog.V(0).Infof("resolve manifest %s for %s: non-transient error, propagating: %v", chunkName, path, resolveErr)
			return false
		}
		attempts++
		if !canCheckSupersession && attempts >= maxUnverifiableResolveAttempts {
			glog.V(0).Infof("resolve manifest %s for %s: supersession unverifiable, propagating after %d attempts: %v", chunkName, path, attempts, resolveErr)
			return false
		}
		glog.V(0).Infof("resolve manifest %s for %s: %v", chunkName, path, resolveErr)
		return true
	}
}

// isTransientResolveError reports whether a manifest resolve failure belongs to
// the classes that clear on their own during a write burst — volume-lookup
// races and network interruptions. Anything else (corrupt manifest data, bad
// file ids) is permanent and must propagate rather than retry forever.
func isTransientResolveError(err error) bool {
	if err == nil {
		return false
	}
	if isRetryableNetworkError(err) {
		return true
	}
	errStr := err.Error()
	return strings.Contains(errStr, "LookupFileId") ||
		(strings.Contains(errStr, "volume id") && strings.Contains(errStr, "not found"))
}

func (fs *FilerSink) uploadManifestChunk(path string, sourceMtimeNs int64, sourceFileId string, manifestData []byte) (fileId string, err error) {
	uploader, err := fs.getUploader()
	if err != nil {
		glog.V(0).Infof("upload manifest data %v: %v", sourceFileId, err)
		return "", fmt.Errorf("upload manifest data: %w", err)
	}

	retryName := fmt.Sprintf("replicate manifest chunk %s", sourceFileId)
	err = util.RetryUntil(retryName, func() error {
		uploadOption := &operation.UploadOption{
			Filename:          "",
			Cipher:            false,
			IsInputCompressed: false,
			MimeType:          "application/octet-stream",
			PairMap:           nil,
			RetryForever:      false,
		}
		if fs.writeChunkByFiler {
			uploadOption.GenUploadUrl = operation.GenUploadUrlProxy(fs.address)
		}
		currentFileId, uploadResult, uploadErr, _ := uploader.UploadWithRetry(
			fs,
			&filer_pb.AssignVolumeRequest{
				Count:       1,
				Replication: fs.replication,
				Collection:  fs.collection,
				TtlSec:      fs.ttlSec,
				DataCenter:  fs.dataCenter,
				DiskType:    fs.diskType,
				Path:        path,
			},
			uploadOption,
			bytes.NewReader(manifestData),
		)
		if uploadErr != nil {
			return fmt.Errorf("upload manifest data: %w", uploadErr)
		}
		if uploadResult.Error != "" {
			return fmt.Errorf("upload manifest result: %v", uploadResult.Error)
		}

		fileId = currentFileId
		return nil
	}, func(uploadErr error) (shouldContinue bool) {
		if fs.hasSourceNewerVersion(path, sourceMtimeNs) {
			glog.V(1).Infof("skip retrying stale source manifest %s for %s: %v", sourceFileId, path, uploadErr)
			return false
		}
		glog.V(0).Infof("replicate manifest %s for %s: %v", sourceFileId, path, uploadErr)
		return true
	})
	if err != nil {
		return "", err
	}

	return fileId, nil
}

func (fs *FilerSink) fetchAndWrite(sourceChunk *filer_pb.FileChunk, path string, sourceMtimeNs int64) (fileId string, err error) {
	uploader, err := fs.getUploader()
	if err != nil {
		glog.V(0).Infof("upload source data %v: %v", sourceChunk.GetFileIdString(), err)
		return "", fmt.Errorf("upload data: %w", err)
	}

	transferStatus := &ChunkTransferStatus{
		ChunkTransferSnapshot: ChunkTransferSnapshot{
			ChunkFileId: sourceChunk.GetFileIdString(),
			Path:        path,
			Status:      "downloading",
		},
	}
	fs.activeTransfers.Store(sourceChunk.GetFileIdString(), transferStatus)
	defer fs.activeTransfers.Delete(sourceChunk.GetFileIdString())

	transientBackoff := time.Duration(0)
	var partialData []byte
	var savedFilename string
	var savedHeader http.Header
	var savedSourceUrl string
	retryName := fmt.Sprintf("replicate chunk %s", sourceChunk.GetFileIdString())
	err = util.RetryUntil(retryName, func() error {
		filename, header, resp, readErr := fs.filerSource.ReadPart(sourceChunk.GetFileIdString(), int64(len(partialData)))
		if readErr != nil {
			return fmt.Errorf("read part %s: %w", sourceChunk.GetFileIdString(), readErr)
		}
		defer util_http.CloseResponse(resp)

		// Save metadata from first successful response
		if len(partialData) == 0 {
			savedFilename = filename
			savedHeader = header
			if resp.Request != nil && resp.Request.URL != nil {
				savedSourceUrl = resp.Request.URL.String()
			}
		}

		// Read the response body
		data, readBodyErr := io.ReadAll(resp.Body)
		if readBodyErr != nil {
			// Keep whatever bytes we received before the error
			partialData = append(partialData, data...)
			return fmt.Errorf("read body: %w", readBodyErr)
		}

		// Combine with previously accumulated partial data
		var fullData []byte
		if len(partialData) > 0 {
			fullData = append(partialData, data...)
			glog.V(0).Infof("resumed reading %s, got %d + %d = %d bytes",
				sourceChunk.GetFileIdString(), len(partialData), len(data), len(fullData))
			partialData = nil
		} else {
			fullData = data
		}

		if err := validateReplicatedReadSize(sourceChunk, len(fullData)); err != nil {
			return err
		}

		transferStatus.mu.Lock()
		transferStatus.BytesReceived = int64(len(fullData))
		transferStatus.Status = "uploading"
		transferStatus.mu.Unlock()

		uploadOption := &operation.UploadOption{
			Filename:          savedFilename,
			Cipher:            false,
			IsInputCompressed: "gzip" == savedHeader.Get("Content-Encoding"),
			MimeType:          savedHeader.Get("Content-Type"),
			PairMap:           nil,
			RetryForever:      false,
			SourceUrl:         savedSourceUrl,
		}
		if fs.writeChunkByFiler {
			uploadOption.GenUploadUrl = operation.GenUploadUrlProxy(fs.address)
		}
		currentFileId, uploadResult, uploadErr, _ := uploader.UploadWithRetry(
			fs,
			&filer_pb.AssignVolumeRequest{
				Count:       1,
				Replication: fs.replication,
				Collection:  fs.collection,
				TtlSec:      fs.ttlSec,
				DataCenter:  fs.dataCenter,
				DiskType:    fs.diskType,
				Path:        path,
			},
			uploadOption,
			util.NewBytesReader(fullData),
		)
		if uploadErr != nil {
			return fmt.Errorf("upload data: %w", uploadErr)
		}
		if uploadResult.Error != "" {
			return fmt.Errorf("upload result: %v", uploadResult.Error)
		}

		transientBackoff = 0
		fileId = currentFileId
		return nil
	}, func(retryErr error) (shouldContinue bool) {
		if errors.Is(retryErr, errChunkSizeMismatch) {
			glog.V(0).Infof("permanent size mismatch replicating %s for %s: %v",
				sourceChunk.GetFileIdString(), path, retryErr)
			transferStatus.mu.Lock()
			transferStatus.LastErr = retryErr.Error()
			transferStatus.mu.Unlock()
			return false
		}
		if fs.hasSourceNewerVersion(path, sourceMtimeNs) {
			glog.V(1).Infof("skip retrying stale source %s for %s: %v", sourceChunk.GetFileIdString(), path, retryErr)
			return false
		}
		transferStatus.mu.Lock()
		transferStatus.LastErr = retryErr.Error()
		transferStatus.mu.Unlock()
		if isRetryableNetworkError(retryErr) {
			transientBackoff = nextTransientBackoff(transientBackoff)
			transferStatus.mu.Lock()
			transferStatus.BytesReceived = int64(len(partialData))
			transferStatus.Status = fmt.Sprintf("waiting %v", transientBackoff)
			transferStatus.mu.Unlock()
			glog.V(0).Infof("connection interrupted while replicating %s for %s (%d bytes received so far), backing off %v: %v",
				sourceChunk.GetFileIdString(), path, len(partialData), transientBackoff, retryErr)
			time.Sleep(transientBackoff)
			transferStatus.mu.Lock()
			transferStatus.Status = "downloading"
			transferStatus.mu.Unlock()
		} else {
			glog.V(0).Infof("replicate %s for %s: %v", sourceChunk.GetFileIdString(), path, retryErr)
		}
		return true
	})
	if err != nil {
		return "", err
	}

	return fileId, nil
}

const maxTransientBackoff = 2 * time.Minute

// nextTransientBackoff escalates 10s -> doubling -> 2m cap so an overloaded
// destination can recover instead of being hammered by the RetryUntil loop.
func nextTransientBackoff(current time.Duration) time.Duration {
	if current < 10*time.Second {
		return 10 * time.Second
	}
	current *= 2
	if current > maxTransientBackoff {
		current = maxTransientBackoff
	}
	return current
}

func isEofError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF)
}

// isRetryableNetworkError reports whether err is a transient network failure worth
// a backoff-and-retry: EOF, timeout (e.g. the destination's idle deadline under
// load), or a reset/broken connection. The volume server returns the timeout as a
// JSON string, so match on text as well as the net.Error interface.
func isRetryableNetworkError(err error) bool {
	if err == nil {
		return false
	}
	if isEofError(err) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	// lower-cased to also catch capitalized variants
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe")
}

// errChunkSizeMismatch is a permanent (non-retriable) replication failure.
var errChunkSizeMismatch = errors.New("chunk size mismatch")

func validateReplicatedReadSize(sourceChunk *filer_pb.FileChunk, readSize int) error {
	if uint64(readSize) != sourceChunk.Size {
		return fmt.Errorf("%w: read %s got %d bytes, source metadata says %d",
			errChunkSizeMismatch, sourceChunk.GetFileIdString(),
			readSize, sourceChunk.Size)
	}
	return nil
}

// hasSourceNewerVersion reports whether the source's current entry for targetPath
// has moved past sourceMtimeNs — gone, or a strictly-newer mtime — meaning the
// version being replayed is stale. The lookup runs regardless of sourceMtimeNs so
// a deleted source is detected even when the replayed mtime is epoch/unset.
func (fs *FilerSink) hasSourceNewerVersion(targetPath string, sourceMtimeNs int64) bool {
	sourcePath, ok := fs.targetPathToSourcePath(targetPath)
	if !ok {
		return false
	}
	return SourceSupersedes(context.Background(), fs.filerSource, sourcePath, sourceMtimeNs)
}

// SourceSupersedes reports whether the live source's entry at sourcePath has
// moved past the replayed version (mtimeNs) — deleted or strictly newer — so
// skipping it is lossless. Exported for filer.backup's non-filer-sink decision.
func SourceSupersedes(ctx context.Context, filerSource *source.FilerSource, sourcePath util.FullPath, mtimeNs int64) bool {
	if filerSource == nil {
		return false
	}
	sourceEntry, err := filer_pb.GetEntry(ctx, filerSource, sourcePath)
	return sourceSupersedes(sourcePath, sourceEntry, err, mtimeNs)
}

// sourceSupersedes reports whether the replayed version (sourceMtimeNs) is stale:
// true if the source entry is gone or strictly newer, false on a same/older version
// or any non-"not found" lookup error (so a transient failure never skips a live
// file). GetEntry signals missing as ErrNotFound — possibly wrapped or a plain gRPC
// string, occasionally a nil entry — all treated as gone.
func sourceSupersedes(sourcePath util.FullPath, sourceEntry *filer_pb.Entry, lookupErr error, sourceMtimeNs int64) bool {
	if lookupErr != nil {
		if errors.Is(lookupErr, filer_pb.ErrNotFound) || strings.Contains(lookupErr.Error(), filer_pb.ErrNotFound.Error()) {
			glog.V(1).Infof("source entry %s no longer exists: %v", sourcePath, lookupErr)
			return true
		}
		glog.V(1).Infof("lookup source entry %s: %v", sourcePath, lookupErr)
		return false
	}
	if sourceEntry == nil {
		glog.V(1).Infof("source entry %s no longer exists", sourcePath)
		return true
	}

	// source still holds the entry; only a strictly-newer mtime proves staleness,
	// and only when there is a valid replayed mtime to compare against.
	if sourceMtimeNs <= 0 {
		return false
	}
	return getEntryMtimeNs(sourceEntry) > sourceMtimeNs
}

func (fs *FilerSink) targetPathToSourcePath(targetPath string) (util.FullPath, bool) {
	if fs.filerSource == nil {
		return "", false
	}

	// Incremental sink keys carry a date prefix (sinkDir/YYYY-MM-DD/relPath) that
	// can't be reversed to a source path; report unmappable rather than build a path
	// under a nonexistent dated dir, which would read as ErrNotFound and skip a live entry.
	if fs.isIncremental {
		return "", false
	}

	normalizePath := func(p string) string {
		p = strings.TrimSuffix(p, "/")
		if p == "" {
			return "/"
		}
		return p
	}

	sourceRoot := normalizePath(fs.filerSource.Dir)
	targetRoot := normalizePath(fs.dir)
	targetPath = normalizePath(targetPath)

	var relative string
	switch {
	case targetRoot == "/":
		relative = strings.TrimPrefix(targetPath, "/")
	case targetPath == targetRoot:
		relative = ""
	case strings.HasPrefix(targetPath, targetRoot+"/"):
		relative = targetPath[len(targetRoot)+1:]
	default:
		return "", false
	}

	if relative == "" {
		return util.FullPath(sourceRoot), true
	}
	return util.FullPath(sourceRoot).Child(relative), true
}

var _ = filer_pb.FilerClient(&FilerSink{})

func (fs *FilerSink) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {

	return pb.WithGrpcClient(context.Background(), streamingMode, fs.signature, func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, fs.grpcAddress, false, fs.grpcDialOption)

}

func (fs *FilerSink) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}

func (fs *FilerSink) GetDataCenter() string {
	return fs.dataCenter
}
