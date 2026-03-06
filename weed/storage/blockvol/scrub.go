package blockvol

import (
	"hash/crc32"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// ScrubStats contains scrub pass statistics.
type ScrubStats struct {
	PassCount      int64
	ErrorCount     int64
	LastPassTime   int64 // unix seconds
	SegmentsTotal  int64
	SegmentsClean  int64
	SegmentsDirty  int64 // skipped (recently written)
	SegmentsErrors int64
}

// Scrubber performs periodic patrol-reads to detect silent data corruption.
// Uses segment-level CRC (1 MiB segments = 256 LBAs with 4K blocks) to reduce
// memory usage. Written LBAs are tracked via NotifyWrite to avoid false positives.
type Scrubber struct {
	vol       *BlockVol
	interval  time.Duration
	throttle  time.Duration // delay between segments
	stopCh    chan struct{}
	triggerCh chan struct{}
	stopped   atomic.Bool

	// Segment CRC map: segment_index → CRC32 of concatenated block data.
	mu          sync.Mutex
	segmentSize uint64            // LBAs per segment (default 256 = 1 MiB)
	segmentCRCs map[uint64]uint32 // segment_index → CRC32
	stats       ScrubStats

	// Write tracking: LBAs written since last scrub pass.
	writtenMu   sync.Mutex
	writtenSegs map[uint64]struct{} // segment indices with writes since last pass
}

// NewScrubber creates a scrubber for the given volume. metrics is optional.
func NewScrubber(vol *BlockVol, interval time.Duration) *Scrubber {
	blockSize := uint64(vol.super.BlockSize)
	segmentSize := uint64(256) // default: 256 blocks per segment (1 MiB with 4K blocks)
	if blockSize == 0 {
		blockSize = 4096
	}
	_ = blockSize // used to document the 256 choice

	return &Scrubber{
		vol:         vol,
		interval:    interval,
		throttle:    100 * time.Microsecond,
		stopCh:      make(chan struct{}),
		triggerCh:   make(chan struct{}, 1),
		segmentSize: segmentSize,
		segmentCRCs: make(map[uint64]uint32),
		writtenSegs: make(map[uint64]struct{}),
	}
}

// Start begins the background scrub loop.
func (s *Scrubber) Start() {
	go s.loop()
}

// Stop stops the scrub loop.
func (s *Scrubber) Stop() {
	if s.stopped.Swap(true) {
		return
	}
	close(s.stopCh)
}

// TriggerNow requests an immediate scrub pass.
func (s *Scrubber) TriggerNow() {
	select {
	case s.triggerCh <- struct{}{}:
	default:
	}
}

// Stats returns the current scrub statistics.
func (s *Scrubber) Stats() ScrubStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

// NotifyWrite records that LBAs were written, invalidating their segment CRCs.
// Called from appendWithRetry after WAL append.
func (s *Scrubber) NotifyWrite(lba uint64, blockCount uint32) {
	s.writtenMu.Lock()
	defer s.writtenMu.Unlock()
	for i := uint32(0); i < blockCount; i++ {
		segIdx := (lba + uint64(i)) / s.segmentSize
		s.writtenSegs[segIdx] = struct{}{}
	}
}

func (s *Scrubber) loop() {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.runPass()
		case <-s.triggerCh:
			s.runPass()
		}
	}
}

func (s *Scrubber) runPass() {
	if s.stopped.Load() {
		return
	}
	passStart := time.Now()

	blockSize := uint64(s.vol.super.BlockSize)
	if blockSize == 0 {
		return
	}
	totalBlocks := s.vol.super.VolumeSize / blockSize
	totalSegments := (totalBlocks + s.segmentSize - 1) / s.segmentSize

	var segClean, segDirty, segErrors int64

	for segIdx := uint64(0); segIdx < totalSegments; segIdx++ {
		if s.stopped.Load() {
			return
		}

		// Check written segments under lock — live check, not snapshot.
		// This avoids false positives from writes that arrive during the pass.
		s.writtenMu.Lock()
		_, wasWritten := s.writtenSegs[segIdx]
		if wasWritten {
			delete(s.writtenSegs, segIdx) // consumed: clear this segment entry
		}
		s.writtenMu.Unlock()

		if wasWritten {
			// Invalidate old CRC for this segment.
			s.mu.Lock()
			delete(s.segmentCRCs, segIdx)
			s.mu.Unlock()
			segDirty++
			continue
		}

		// Skip segments with dirty map entries.
		startLBA := segIdx * s.segmentSize
		if s.segmentHasDirtyBlocks(startLBA) {
			s.mu.Lock()
			delete(s.segmentCRCs, segIdx)
			s.mu.Unlock()
			segDirty++
			continue
		}

		// Read and checksum the segment.
		segCRC, err := s.checksumSegment(segIdx, totalBlocks)
		if err != nil {
			log.Printf("scrub: error reading segment %d: %v", segIdx, err)
			segErrors++
			s.vol.healthScore.RecordScrubError()
			continue
		}

		// Post-read double-check: if a write arrived during the read, skip comparison.
		// This closes the race between the pre-read writtenSegs check and the actual I/O.
		s.writtenMu.Lock()
		_, writtenDuringRead := s.writtenSegs[segIdx]
		if writtenDuringRead {
			delete(s.writtenSegs, segIdx)
		}
		s.writtenMu.Unlock()
		if writtenDuringRead {
			s.mu.Lock()
			delete(s.segmentCRCs, segIdx)
			s.mu.Unlock()
			segDirty++
			continue
		}

		// Compare with previous CRC.
		s.mu.Lock()
		prevCRC, hasPrev := s.segmentCRCs[segIdx]
		s.segmentCRCs[segIdx] = segCRC
		s.mu.Unlock()

		if hasPrev && prevCRC != segCRC {
			log.Printf("scrub: CORRUPTION detected in segment %d (LBAs %d-%d): CRC %08x != %08x",
				segIdx, startLBA, startLBA+s.segmentSize-1, segCRC, prevCRC)
			segErrors++
			s.vol.healthScore.RecordScrubError()
		} else {
			segClean++
		}

		// Throttle I/O.
		if s.throttle > 0 {
			time.Sleep(s.throttle)
		}
	}

	s.vol.healthScore.RecordScrubComplete()

	s.mu.Lock()
	s.stats.PassCount++
	s.stats.ErrorCount += segErrors
	s.stats.LastPassTime = time.Now().Unix()
	s.stats.SegmentsTotal = int64(totalSegments)
	s.stats.SegmentsClean = segClean
	s.stats.SegmentsDirty = segDirty
	s.stats.SegmentsErrors = segErrors
	s.mu.Unlock()

	// Record metrics.
	if s.vol.Metrics != nil {
		s.vol.Metrics.RecordScrubPass(time.Since(passStart), segErrors)
	}
}

// segmentHasDirtyBlocks checks if any LBA in the segment has a dirty map entry.
func (s *Scrubber) segmentHasDirtyBlocks(startLBA uint64) bool {
	for i := uint64(0); i < s.segmentSize; i++ {
		lba := startLBA + i
		if lba*uint64(s.vol.super.BlockSize) >= s.vol.super.VolumeSize {
			break
		}
		if _, _, _, ok := s.vol.dirtyMap.Get(lba); ok {
			return true
		}
	}
	return false
}

// checksumSegment reads all blocks in a segment from the extent and computes CRC32.
func (s *Scrubber) checksumSegment(segIdx, totalBlocks uint64) (uint32, error) {
	startLBA := segIdx * s.segmentSize
	blockSize := uint64(s.vol.super.BlockSize)
	extentStart := s.vol.super.WALOffset + s.vol.super.WALSize

	h := crc32.NewIEEE()
	for i := uint64(0); i < s.segmentSize; i++ {
		lba := startLBA + i
		if lba >= totalBlocks {
			break
		}
		buf := make([]byte, blockSize)
		off := int64(extentStart + lba*blockSize)
		if _, err := s.vol.fd.ReadAt(buf, off); err != nil {
			return 0, err
		}
		h.Write(buf)
	}
	return h.Sum32(), nil
}
