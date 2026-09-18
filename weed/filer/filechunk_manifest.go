package filer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/wdclient"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

var bytesBufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// Keep Manifest reads bounded across all recursion levels of one resolution.
const maxChunkManifestResolveWorkers = 4

// Size of the job queue buffer. Large enough that submission does not block
// under normal chunk counts, so a promptly-failing manifest is always queued
// and can cancel stalled sibling reads once a worker picks it up.
const chunkManifestResolveJobBufferSize = 128

func HasChunkManifest(chunks []*filer_pb.FileChunk) bool {
	for _, chunk := range chunks {
		if chunk.IsChunkManifest {
			return true
		}
	}
	return false
}

func SeparateManifestChunks(chunks []*filer_pb.FileChunk) (manifestChunks, nonManifestChunks []*filer_pb.FileChunk) {
	for _, c := range chunks {
		if c.IsChunkManifest {
			manifestChunks = append(manifestChunks, c)
		} else {
			nonManifestChunks = append(nonManifestChunks, c)
		}
	}
	return
}

func ResolveChunkManifest(ctx context.Context, lookupFileIdFn wdclient.LookupFileIdFunctionType, chunks []*filer_pb.FileChunk, startOffset, stopOffset int64, invalidator CacheInvalidator) (dataChunks, manifestChunks []*filer_pb.FileChunk, manifestResolveErr error) {
	resolver := newChunkManifestResolver(ctx, lookupFileIdFn, invalidator)
	defer resolver.close()
	return resolver.resolve(chunks, startOffset, stopOffset)
}

type chunkManifestResolveJob struct {
	chunk       *filer_pb.FileChunk
	result      *chunkManifestResolveResult
	done        *sync.WaitGroup
	batchCtx    context.Context
	batchCancel context.CancelFunc
	batchOnce   *sync.Once
}

type chunkManifestResolveResult struct {
	chunks         []*filer_pb.FileChunk
	err            error
	internalCancel bool
}

type chunkManifestResolver struct {
	ctx            context.Context
	parentCtx      context.Context
	cancel         context.CancelFunc
	lookupFileIdFn wdclient.LookupFileIdFunctionType
	invalidator    CacheInvalidator
	jobs           chan chunkManifestResolveJob
	overflowSem    chan struct{}
	workers        sync.WaitGroup
	startOnce      sync.Once
	started        bool
}

func newChunkManifestResolver(ctx context.Context, lookupFileIdFn wdclient.LookupFileIdFunctionType, invalidator CacheInvalidator) *chunkManifestResolver {
	workCtx, cancel := context.WithCancel(ctx)
	resolver := &chunkManifestResolver{
		ctx:            workCtx,
		parentCtx:      ctx,
		cancel:         cancel,
		lookupFileIdFn: lookupFileIdFn,
		invalidator:    invalidator,
		jobs:           make(chan chunkManifestResolveJob, chunkManifestResolveJobBufferSize),
		overflowSem:    make(chan struct{}, maxChunkManifestResolveWorkers),
	}
	return resolver
}

func (r *chunkManifestResolver) executeJob(job chunkManifestResolveJob) {
	job.result.chunks, job.result.err = ResolveOneChunkManifest(job.batchCtx, r.lookupFileIdFn, job.chunk, r.invalidator)
	if job.result.err != nil && r.parentCtx.Err() == nil {
		if job.batchCtx.Err() != nil && errors.Is(job.result.err, context.Canceled) {
			job.result.internalCancel = true
		} else if job.batchCtx.Err() == nil {
			job.batchOnce.Do(job.batchCancel)
		}
	}
	job.done.Done()
}

func (r *chunkManifestResolver) executeOverflowJob(job chunkManifestResolveJob) {
	select {
	case r.overflowSem <- struct{}{}:
		defer func() { <-r.overflowSem }()
		r.executeJob(job)
	case <-job.batchCtx.Done():
		if job.result.err == nil {
			job.result.err = job.batchCtx.Err()
			job.result.internalCancel = r.parentCtx.Err() == nil && job.result.err != nil
		}
		job.done.Done()
	case <-r.ctx.Done():
		if job.result.err == nil {
			job.result.err = r.ctx.Err()
			job.result.internalCancel = r.parentCtx.Err() == nil && job.result.err != nil
		}
		job.done.Done()
	}
}

func (r *chunkManifestResolver) worker() {
	defer r.workers.Done()
	for job := range r.jobs {
		r.executeJob(job)
	}
}

func (r *chunkManifestResolver) close() {
	r.cancel()
	close(r.jobs)
	if r.started {
		r.workers.Wait()
	}
}

func (r *chunkManifestResolver) submit(job chunkManifestResolveJob) bool {
	r.startOnce.Do(func() {
		r.workers.Add(maxChunkManifestResolveWorkers)
		for i := 0; i < maxChunkManifestResolveWorkers; i++ {
			go r.worker()
		}
		r.started = true
	})
	select {
	case r.jobs <- job:
		return true
	case <-r.ctx.Done():
		return false
	case <-job.batchCtx.Done():
		// Batch was cancelled by a sibling failure; don't queue this job.
		// Set the result so the caller doesn't overwrite it; the caller
		// owns the WaitGroup decrement.
		job.result.err = job.batchCtx.Err()
		job.result.internalCancel = r.parentCtx.Err() == nil && job.result.err != nil
		return false
	default:
		// Buffer is full (exceedingly rare: more than chunkManifestResolveJobBufferSize
		// in-range manifests at one level). Run the job in a bounded overflow goroutine
		// so a promptly-failing manifest can still cancel the batch without waiting for
		// a worker slot, while keeping total concurrency bounded.
		go r.executeOverflowJob(job)
		return true
	}
}

func (r *chunkManifestResolver) resolve(chunks []*filer_pb.FileChunk, startOffset, stopOffset int64) (dataChunks, manifestChunks []*filer_pb.FileChunk, manifestResolveErr error) {
	type resolveSlot struct {
		chunk  *filer_pb.FileChunk
		result chunkManifestResolveResult
	}

	// Cancellation is scoped to this parallel read batch so a failure in a
	// later manifest does not cancel recursive work for an earlier manifest
	// that already completed. Recursion creates its own batch context derived
	// from the still-alive resolver context.
	batchCtx, batchCancel := context.WithCancel(r.ctx)
	defer batchCancel()
	var batchOnce sync.Once

	slots := make([]resolveSlot, len(chunks))
	var reads sync.WaitGroup
	for i, chunk := range chunks {
		if max(chunk.Offset, startOffset) >= min(chunk.Offset+int64(chunk.Size), stopOffset) {
			continue
		}

		slots[i].chunk = chunk
		if !chunk.IsChunkManifest {
			continue
		}

		reads.Add(1)
		if !r.submit(chunkManifestResolveJob{
			chunk:       chunk,
			result:      &slots[i].result,
			done:        &reads,
			batchCtx:    batchCtx,
			batchCancel: batchCancel,
			batchOnce:   &batchOnce,
		}) {
			// submit may have already set the result (batch cancellation).
			// Only set it here for the resolver-cancellation case.
			if slots[i].result.err == nil {
				slots[i].result.err = r.ctx.Err()
				slots[i].result.internalCancel = r.parentCtx.Err() == nil && slots[i].result.err != nil
			}
			reads.Done()
		}
	}
	reads.Wait()
	if err := r.parentCtx.Err(); err != nil {
		return dataChunks, nil, err
	}

	// Recurse only after this level's reads release their worker slots. A
	// worker must never wait for a child manifest while holding a slot.
	//
	// Pre-scan for the first real (non-internal-cancel) error before
	// recursing. If a later manifest already failed, return its error
	// promptly with data chunks that are already in hand, instead of
	// blocking on recursive reads of earlier manifests' children.
	for i, slot := range slots {
		if slot.chunk == nil {
			continue
		}
		if slot.result.err != nil {
			if slot.result.internalCancel {
				continue
			}
			for j := 0; j < i; j++ {
				if slots[j].chunk == nil {
					continue
				}
				if !slots[j].chunk.IsChunkManifest {
					dataChunks = append(dataChunks, slots[j].chunk)
					continue
				}
				for _, c := range slots[j].result.chunks {
					if !c.IsChunkManifest && max(c.Offset, startOffset) < min(c.Offset+int64(c.Size), stopOffset) {
						dataChunks = append(dataChunks, c)
					}
				}
			}
			return dataChunks, nil, slot.result.err
		}
	}

	for _, slot := range slots {
		if slot.chunk == nil {
			continue
		}
		if slot.result.err != nil {
			if slot.result.internalCancel {
				continue
			}
			return dataChunks, nil, slot.result.err
		}
		if !slot.chunk.IsChunkManifest {
			dataChunks = append(dataChunks, slot.chunk)
			continue
		}

		manifestChunks = append(manifestChunks, slot.chunk)
		subDataChunks, subManifestChunks, subErr := r.resolve(slot.result.chunks, startOffset, stopOffset)
		if subErr != nil {
			return dataChunks, nil, subErr
		}
		dataChunks = append(dataChunks, subDataChunks...)
		manifestChunks = append(manifestChunks, subManifestChunks...)
	}
	return
}

// ResolveOneChunkManifest fetches and decodes a single manifest chunk. It is
// the uncached, exported path used by every non-Mount caller; the Mount path
// routes through resolveOneChunkManifest so it can share a per-mount cache.
// Keeping this signature stable preserves the existing four-argument contract
// for external callers.
func ResolveOneChunkManifest(ctx context.Context, lookupFileIdFn wdclient.LookupFileIdFunctionType, chunk *filer_pb.FileChunk, invalidator CacheInvalidator) (dataChunks []*filer_pb.FileChunk, manifestResolveErr error) {
	return resolveOneChunkManifest(ctx, lookupFileIdFn, chunk, invalidator, nil)
}

// resolveOneChunkManifest is the cache-aware implementation. cache may be nil,
// in which case the manifest is fetched and validated on every call, matching
// the historical uncached behavior. A non-nil cache is owned by a single mount
// (WFS) and coalesces concurrent cold misses via singleflight.
func resolveOneChunkManifest(ctx context.Context, lookupFileIdFn wdclient.LookupFileIdFunctionType, chunk *filer_pb.FileChunk, invalidator CacheInvalidator, cache *ChunkManifestCache) (dataChunks []*filer_pb.FileChunk, manifestResolveErr error) {
	if !chunk.IsChunkManifest {
		return
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	key := chunkManifestCacheKey{
		fileID:       chunk.GetFileIdString(),
		cipherKey:    string(chunk.CipherKey),
		isCompressed: chunk.IsCompressed,
	}

	fetch := func() ([]byte, error) {
		bytesBuffer := bytesBufferPool.Get().(*bytes.Buffer)
		bytesBuffer.Reset()
		defer bytesBufferPool.Put(bytesBuffer)
		if err := fetchWholeChunk(ctx, bytesBuffer, lookupFileIdFn, key.fileID, chunk.CipherKey, chunk.IsCompressed, invalidator); err != nil {
			return nil, fmt.Errorf("fail to read manifest %s: %w", key.fileID, err)
		}
		// Copy before the buffer returns to the pool so concurrent callers
		// cannot overwrite the slice before it is cached.
		data := append([]byte(nil), bytesBuffer.Bytes()...)
		// Validate before returning so fetchOrLoad only caches well-formed
		// manifests. A malformed manifest must not poison the cache.
		if err := proto.Unmarshal(data, &filer_pb.FileChunkManifest{}); err != nil {
			return nil, fmt.Errorf("fail to unmarshal manifest %s: %w", key.fileID, err)
		}
		return data, nil
	}

	var manifestBytes []byte
	if cache != nil {
		data, err := cache.fetchOrLoad(ctx, key, fetch)
		if err != nil {
			return nil, err
		}
		manifestBytes = data
	} else {
		data, err := fetch()
		if err != nil {
			return nil, err
		}
		manifestBytes = data
	}

	m := &filer_pb.FileChunkManifest{}
	if err := proto.Unmarshal(manifestBytes, m); err != nil {
		return nil, fmt.Errorf("fail to unmarshal manifest %s: %w", key.fileID, err)
	}

	// recursive
	filer_pb.AfterEntryDeserialization(m.Chunks)
	return m.Chunks, nil
}

func fetchWholeChunk(ctx context.Context, bytesBuffer *bytes.Buffer, lookupFileIdFn wdclient.LookupFileIdFunctionType, fileId string, cipherKey []byte, isGzipped bool, invalidator CacheInvalidator) error {
	urlStrings, err := lookupFileIdFn(ctx, fileId)
	if err != nil {
		glog.ErrorfCtx(ctx, "operation LookupFileId %s failed, err: %v", fileId, err)
		return err
	}
	jwt := ChunkReadJwt(urlStrings, fileId)
	if _, err = retriedStreamFetchChunkData(ctx, bytesBuffer, urlStrings, jwt, cipherKey, isGzipped, true, 0, 0, refreshUrls(ctx, invalidator, lookupFileIdFn, fileId)); err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		// a cancelled read says nothing about where the volume lives, and the
		// stream error it provoked is a symptom, not the cause
		return ctxErr
	}
	return retryFetchWithFreshLocations(ctx, invalidator, lookupFileIdFn, fileId, urlStrings, err, func(newUrls []string) error {
		// the failed attempt may have streamed a partial prefix into the buffer
		bytesBuffer.Reset()
		_, retryErr := retriedStreamFetchChunkData(ctx, bytesBuffer, newUrls, jwt, cipherKey, isGzipped, true, 0, 0, nil)
		return retryErr
	})
}

func fetchChunkRange(ctx context.Context, buffer []byte, lookupFileIdFn wdclient.LookupFileIdFunctionType, fileId string, cipherKey []byte, isGzipped bool, offset int64, refreshUrls util_http.RefreshUrlsFunc) (int, error) {
	urlStrings, err := lookupFileIdFn(ctx, fileId)
	if err != nil {
		glog.ErrorfCtx(ctx, "operation LookupFileId %s failed, err: %v", fileId, err)
		return 0, err
	}
	return util_http.RetriedFetchChunkData(ctx, buffer, urlStrings, cipherKey, isGzipped, false, offset, fileId, refreshUrls)
}

// retriedStreamFetchChunkData streams a chunk from the first location that
// answers. refreshUrls may be nil; when a location failed and a later one
// answered, it is called so the reads that follow start from a fresh list.
func retriedStreamFetchChunkData(ctx context.Context, writer io.Writer, urlStrings []string, jwt string, cipherKey []byte, isGzipped bool, isFullChunk bool, offset int64, size int, refreshUrls util_http.RefreshUrlsFunc) (written int64, err error) {

	var shouldRetry bool
	var totalWritten int

	for waitTime := time.Second; waitTime < util.RetryWaitTime; waitTime += waitTime / 2 {
		// Check for context cancellation before starting retry loop
		select {
		case <-ctx.Done():
			return int64(totalWritten), ctx.Err()
		default:
		}

		retriedCnt := 0
		var failed bool
		for _, urlString := range util_http.ReachableFirst(urlStrings) {
			// Check for context cancellation before each volume server request
			select {
			case <-ctx.Done():
				return int64(totalWritten), ctx.Err()
			default:
			}

			retriedCnt++
			var localProcessed int
			var writeErr error
			shouldRetry, err = util_http.ReadUrlAsStream(ctx, util_http.AppendQueryParameter(urlString, "readDeleted", "true"), jwt, cipherKey, isGzipped, isFullChunk, offset, size, func(data []byte) {
				// Check for context cancellation during data processing
				select {
				case <-ctx.Done():
					writeErr = ctx.Err()
					return
				default:
				}

				if totalWritten > localProcessed {
					toBeSkipped := totalWritten - localProcessed
					if len(data) <= toBeSkipped {
						localProcessed += len(data)
						return // skip if already processed
					}
					data = data[toBeSkipped:]
					localProcessed += toBeSkipped
				}
				var writtenCount int
				writtenCount, writeErr = writer.Write(data)
				localProcessed += writtenCount
				totalWritten += writtenCount
			})
			if !shouldRetry {
				break
			}
			if writeErr != nil {
				err = writeErr
				break
			}
			if err != nil {
				failed = true
				glog.V(0).InfofCtx(ctx, "read %s failed, err: %v", urlString, err)
			} else {
				break
			}
		}
		if err == nil && failed && refreshUrls != nil {
			refreshUrls()
		}
		// all nodes have tried it
		if retriedCnt == len(urlStrings) {
			break
		}
		if err != nil && shouldRetry {
			glog.V(0).InfofCtx(ctx, "retry reading in %v", waitTime)
			// Sleep with proper context cancellation and timer cleanup
			timer := time.NewTimer(waitTime)
			select {
			case <-ctx.Done():
				timer.Stop()
				return int64(totalWritten), ctx.Err()
			case <-timer.C:
				// Continue with retry
			}
		} else {
			break
		}
	}

	return int64(totalWritten), err

}

// MaybeManifestize folds a flat chunk list into manifest chunks once it passes
// ManifestBatch, so an entry's chunk list stays within what the metadata store
// will hold. A fold that fails partway returns inputChunks unchanged -- never
// the half-folded list, which drops the manifests the caller came in with --
// and hands the blobs it had already saved to deleteChunks, since the flat list
// it returns references none of them. deleteChunks may be nil where the caller
// has no deleter to offer; then the blobs are only named in the log.
func MaybeManifestize(saveFunc SaveDataAsChunkFunctionType, deleteChunks func([]*filer_pb.FileChunk), inputChunks []*filer_pb.FileChunk) (chunks []*filer_pb.FileChunk, err error) {
	var saved []*filer_pb.FileChunk
	record := func(reader io.Reader, name string, offset int64, tsNs int64, expectedDataSize uint64) (*filer_pb.FileChunk, error) {
		chunk, saveErr := saveFunc(reader, name, offset, tsNs, expectedDataSize)
		if saveErr == nil {
			saved = append(saved, chunk)
		}
		return chunk, saveErr
	}

	chunks, err = doMaybeManifestize(record, inputChunks, ManifestBatch, mergeIntoManifest)
	if err == nil {
		return chunks, nil
	}
	if len(saved) > 0 {
		if deleteChunks != nil {
			deleteChunks(saved)
		} else {
			glog.V(0).Infof("manifestize failed, %d manifest blobs left unreferenced: %v", len(saved), err)
		}
	}
	return inputChunks, err
}

func doMaybeManifestize(saveFunc SaveDataAsChunkFunctionType, inputChunks []*filer_pb.FileChunk, mergeFactor int, mergefn func(saveFunc SaveDataAsChunkFunctionType, dataChunks []*filer_pb.FileChunk) (manifestChunk *filer_pb.FileChunk, err error)) (chunks []*filer_pb.FileChunk, err error) {

	var dataChunks []*filer_pb.FileChunk
	for _, chunk := range inputChunks {
		if !chunk.IsChunkManifest {
			dataChunks = append(dataChunks, chunk)
		} else {
			chunks = append(chunks, chunk)
		}
	}

	remaining := len(dataChunks)
	for i := 0; i+mergeFactor <= len(dataChunks); i += mergeFactor {
		chunk, err := mergefn(saveFunc, dataChunks[i:i+mergeFactor])
		if err != nil {
			// dataChunks is what is left after the manifests the caller
			// already had were separated out; returning it would drop them
			return inputChunks, err
		}
		chunks = append(chunks, chunk)
		remaining -= mergeFactor
	}
	// remaining
	for i := len(dataChunks) - remaining; i < len(dataChunks); i++ {
		chunks = append(chunks, dataChunks[i])
	}
	return
}

func mergeIntoManifest(saveFunc SaveDataAsChunkFunctionType, dataChunks []*filer_pb.FileChunk) (manifestChunk *filer_pb.FileChunk, err error) {

	filer_pb.BeforeEntrySerialization(dataChunks)

	// create and serialize the manifest
	data, serErr := proto.Marshal(&filer_pb.FileChunkManifest{
		Chunks: dataChunks,
	})
	if serErr != nil {
		return nil, fmt.Errorf("serializing manifest: %w", serErr)
	}

	minOffset, maxOffset := int64(math.MaxInt64), int64(math.MinInt64)
	for _, chunk := range dataChunks {
		if minOffset > int64(chunk.Offset) {
			minOffset = chunk.Offset
		}
		if maxOffset < int64(chunk.Size)+chunk.Offset {
			maxOffset = int64(chunk.Size) + chunk.Offset
		}
	}

	manifestChunk, err = saveFunc(bytes.NewReader(data), "", 0, 0, uint64(len(data)))
	if err != nil {
		return nil, err
	}
	manifestChunk.IsChunkManifest = true
	manifestChunk.Offset = minOffset
	manifestChunk.Size = uint64(maxOffset - minOffset)

	return
}

type SaveDataAsChunkFunctionType func(reader io.Reader, name string, offset int64, tsNs int64, expectedDataSize uint64) (chunk *filer_pb.FileChunk, err error)
