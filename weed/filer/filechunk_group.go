package filer

import (
	"context"
	"io"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

type ChunkGroup struct {
	lookupFn          wdclient.LookupFileIdFunctionType
	sections          map[SectionIndex]*FileChunkSection
	sectionsLock      sync.RWMutex
	readerCache       *ReaderCache
	concurrentReaders int
	// cacheInvalidator lets manifest resolution drop stale volume locations, as ReaderCache does for chunk reads
	cacheInvalidator CacheInvalidator
	// resolveErr is set when chunk manifest resolution failed, guarded by
	// sectionsLock. Reads must fail with this error instead of silently
	// zero-filling the unresolved sections as if they were sparse holes.
	resolveErr error
}

// NewChunkGroup creates a ChunkGroup with configurable concurrency.
// concurrentReaders controls:
// - Maximum parallel chunk fetches during read operations
// - Read-ahead prefetch parallelism
// - Number of concurrent section reads for large files
// If concurrentReaders <= 0, defaults to 16.
func NewChunkGroup(lookupFn wdclient.LookupFileIdFunctionType, chunkCache chunk_cache.ChunkCache, chunks []*filer_pb.FileChunk, concurrentReaders int, cacheInvalidator CacheInvalidator, budgets ...*ReaderCacheBudget) (*ChunkGroup, error) {
	if concurrentReaders <= 0 {
		concurrentReaders = 16
	}
	if concurrentReaders > 128 {
		concurrentReaders = 128 // Cap to prevent excessive goroutine fan-out
	}
	// ReaderCache limit should be at least concurrentReaders to allow parallel prefetching
	readerCacheLimit := concurrentReaders * 2
	if readerCacheLimit < 32 {
		readerCacheLimit = 32
	}
	group := &ChunkGroup{
		lookupFn:          lookupFn,
		sections:          make(map[SectionIndex]*FileChunkSection),
		readerCache:       NewReaderCache(readerCacheLimit, chunkCache, lookupFn, cacheInvalidator, budgets...),
		concurrentReaders: concurrentReaders,
		cacheInvalidator:  cacheInvalidator,
	}

	err := group.SetChunks(chunks)
	return group, err
}

// GetPrefetchCount returns the number of chunks to prefetch ahead during sequential reads.
// This is derived from concurrentReaders to keep the network pipeline full.
func (group *ChunkGroup) GetPrefetchCount() int {
	// Prefetch at least 1, and scale with concurrency (roughly 1/4 of concurrent readers)
	prefetch := group.concurrentReaders / 4
	if prefetch < 1 {
		prefetch = 1
	}
	if prefetch > 8 {
		prefetch = 8 // Cap at 8 to avoid excessive memory usage
	}
	return prefetch
}

func (group *ChunkGroup) AddChunk(chunk *filer_pb.FileChunk) error {

	group.sectionsLock.Lock()
	defer group.sectionsLock.Unlock()

	sectionIndexStart, sectionIndexStop := SectionIndex(chunk.Offset/SectionSize), SectionIndex((chunk.Offset+int64(chunk.Size))/SectionSize)
	for si := sectionIndexStart; si < sectionIndexStop+1; si++ {
		section, found := group.sections[si]
		if !found {
			section = NewFileChunkSection(si)
			group.sections[si] = section
		}
		section.addChunk(chunk)
	}
	return nil
}

func (group *ChunkGroup) ReadDataAt(ctx context.Context, fileSize int64, buff []byte, offset int64) (n int, tsNs int64, err error) {
	if offset >= fileSize {
		return 0, 0, io.EOF
	}

	group.sectionsLock.RLock()
	defer group.sectionsLock.RUnlock()

	// Fail fast when chunk manifest resolution failed: the sections map is
	// empty or partial, and zero-filling it would silently return all-zero
	// data as if the file were one big sparse hole.
	if group.resolveErr != nil {
		return 0, 0, group.resolveErr
	}

	sectionIndexStart, sectionIndexStop := SectionIndex(offset/SectionSize), SectionIndex((offset+int64(len(buff)))/SectionSize)
	numSections := int(sectionIndexStop - sectionIndexStart + 1)

	// For single section or when concurrency is disabled, use sequential reading
	if numSections <= 1 || group.concurrentReaders <= 1 {
		return group.readDataAtSequential(ctx, fileSize, buff, offset, sectionIndexStart, sectionIndexStop)
	}

	// For multiple sections, use parallel reading
	return group.readDataAtParallel(ctx, fileSize, buff, offset, sectionIndexStart, sectionIndexStop)
}

// readDataAtSequential reads sections sequentially (original behavior)
func (group *ChunkGroup) readDataAtSequential(ctx context.Context, fileSize int64, buff []byte, offset int64, sectionIndexStart, sectionIndexStop SectionIndex) (n int, tsNs int64, err error) {
	for si := sectionIndexStart; si < sectionIndexStop+1; si++ {
		section, found := group.sections[si]
		rangeStart, rangeStop := max(offset, int64(si*SectionSize)), min(offset+int64(len(buff)), int64((si+1)*SectionSize))
		if rangeStart >= rangeStop {
			continue
		}
		if !found {
			rangeStop = min(rangeStop, fileSize)
			for i := rangeStart; i < rangeStop; i++ {
				buff[i-offset] = 0
			}
			n = int(int64(n) + rangeStop - rangeStart)
			continue
		}
		xn, xTsNs, xErr := section.readDataAt(ctx, group, fileSize, buff[rangeStart-offset:rangeStop-offset], rangeStart)
		if xErr != nil {
			return n + xn, max(tsNs, xTsNs), xErr
		}
		n += xn
		tsNs = max(tsNs, xTsNs)
	}
	return
}

// sectionReadResult holds the result of a section read operation
type sectionReadResult struct {
	sectionIndex SectionIndex
	n            int
	tsNs         int64
	err          error
}

// readDataAtParallel reads multiple sections in parallel for better throughput
func (group *ChunkGroup) readDataAtParallel(ctx context.Context, fileSize int64, buff []byte, offset int64, sectionIndexStart, sectionIndexStop SectionIndex) (n int, tsNs int64, err error) {
	numSections := int(sectionIndexStop - sectionIndexStart + 1)

	// Limit concurrency to the smaller of concurrentReaders and numSections
	maxConcurrent := group.concurrentReaders
	if numSections < maxConcurrent {
		maxConcurrent = numSections
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrent)

	results := make([]sectionReadResult, numSections)

	for i := 0; i < numSections; i++ {
		si := sectionIndexStart + SectionIndex(i)
		idx := i

		section, found := group.sections[si]
		rangeStart, rangeStop := max(offset, int64(si*SectionSize)), min(offset+int64(len(buff)), int64((si+1)*SectionSize))
		if rangeStart >= rangeStop {
			continue
		}

		if !found {
			// Zero-fill missing sections synchronously
			rangeStop = min(rangeStop, fileSize)
			for j := rangeStart; j < rangeStop; j++ {
				buff[j-offset] = 0
			}
			results[idx] = sectionReadResult{
				sectionIndex: si,
				n:            int(rangeStop - rangeStart),
				tsNs:         0,
				err:          nil,
			}
			continue
		}

		// Capture variables for closure
		sectionCopy := section
		buffSlice := buff[rangeStart-offset : rangeStop-offset]
		rangeStartCopy := rangeStart

		g.Go(func() error {
			xn, xTsNs, xErr := sectionCopy.readDataAt(gCtx, group, fileSize, buffSlice, rangeStartCopy)
			results[idx] = sectionReadResult{
				sectionIndex: si,
				n:            xn,
				tsNs:         xTsNs,
				err:          xErr,
			}
			if xErr != nil && xErr != io.EOF {
				return xErr
			}
			return nil
		})
	}

	// Wait for all goroutines to complete
	groupErr := g.Wait()

	// Aggregate results
	for _, result := range results {
		n += result.n
		tsNs = max(tsNs, result.tsNs)
		// Collect first non-EOF error from results as fallback
		if result.err != nil && result.err != io.EOF && err == nil {
			err = result.err
		}
	}

	// Prioritize errgroup error (first error that cancelled context)
	if groupErr != nil {
		err = groupErr
	}

	return
}

func (group *ChunkGroup) SetChunks(chunks []*filer_pb.FileChunk) error {
	group.sectionsLock.Lock()
	defer group.sectionsLock.Unlock()

	var dataChunks []*filer_pb.FileChunk
	for _, chunk := range chunks {

		if !chunk.IsChunkManifest {
			dataChunks = append(dataChunks, chunk)
			continue
		}

		resolvedChunks, err := ResolveOneChunkManifest(context.Background(), group.lookupFn, chunk, group.cacheInvalidator)
		if err != nil {
			// remember the failure so ReadDataAt returns an error instead of
			// treating the unresolved sections as sparse holes
			group.resolveErr = err
			return err
		}

		dataChunks = append(dataChunks, resolvedChunks...)
	}

	sections := make(map[SectionIndex]*FileChunkSection)

	for _, chunk := range dataChunks {
		sectionIndexStart, sectionIndexStop := SectionIndex(chunk.Offset/SectionSize), SectionIndex((chunk.Offset+int64(chunk.Size))/SectionSize)
		for si := sectionIndexStart; si < sectionIndexStop+1; si++ {
			section, found := sections[si]
			if !found {
				section = NewFileChunkSection(si)
				sections[si] = section
			}
			section.chunks = append(section.chunks, chunk)
		}
	}

	group.sections = sections
	group.resolveErr = nil
	return nil
}

const (
	// see weedfs_file_lseek.go
	SEEK_DATA uint32 = 3 // seek to next data after the offset
	// SEEK_HOLE uint32 = 4 // seek to next hole after the offset
)

func (group *ChunkGroup) SearchChunks(ctx context.Context, offset, fileSize int64, whence uint32) (found bool, out int64, err error) {
	group.sectionsLock.RLock()
	defer group.sectionsLock.RUnlock()

	// the section map is unreliable after a failed manifest resolution
	if group.resolveErr != nil {
		return false, 0, group.resolveErr
	}

	found, out = group.doSearchChunks(ctx, offset, fileSize, whence)
	return found, out, nil
}

func (group *ChunkGroup) doSearchChunks(ctx context.Context, offset, fileSize int64, whence uint32) (found bool, out int64) {

	sectionIndex, maxSectionIndex := SectionIndex(offset/SectionSize), SectionIndex(fileSize/SectionSize)
	for si := sectionIndex; si <= maxSectionIndex; si++ {
		sectionStart, sectionStop := sectionBounds(si, fileSize)
		sectionStart = max(offset, sectionStart)
		if sectionStart >= sectionStop {
			continue
		}

		section, foundSection := group.sections[si]
		if whence == SEEK_DATA {
			if !foundSection {
				continue
			}
			dataStart := section.DataStartOffset(ctx, group, sectionStart, fileSize)
			if dataStart >= sectionStart && dataStart < sectionStop {
				return true, dataStart
			}
			continue
		}

		// whence == SEEK_HOLE
		if !foundSection {
			return true, sectionStart
		}
		holeStart := section.NextStopOffset(ctx, group, sectionStart, fileSize)
		if holeStart < sectionStop {
			return true, holeStart
		}
	}

	if whence == SEEK_DATA {
		return false, 0
	}
	return true, fileSize
}

func (group *ChunkGroup) Close() error {
	group.readerCache.destroy()
	return nil
}
