package mount

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// FileIdEntry holds a pre-allocated file ID from the filer/master, ready for
// immediate use by an upload worker without an AssignVolume round-trip.
type FileIdEntry struct {
	FileId string
	Host   string // volume server address (already adjusted for access mode)
	Auth   security.EncodedJwt
	Time   time.Time
}

// FileIdPool pre-allocates file IDs in batches so that chunk uploads can grab
// one instantly instead of blocking on an AssignVolume RPC per chunk.
//
// The pool is refilled in the background when it drops below a low-water mark.
// All IDs are allocated with the mount's global (replication, collection, ttl,
// diskType, dataCenter) parameters. Path-based storage rules (filer.conf) are
// NOT applied to pooled IDs since the pool allocates ahead of any specific file
// path. This is an intentional tradeoff for writeback cache performance.
type FileIdPool struct {
	wfs *WFS

	mu       sync.Mutex
	cond     *sync.Cond
	entries  []FileIdEntry // available pre-allocated IDs
	filling  bool          // true when a background refill is in progress

	poolSize  int // target pool capacity
	batchSize int // how many IDs to request per Assign RPC
	lowWater  int // refill trigger threshold
	maxAge    time.Duration
}

func NewFileIdPool(wfs *WFS) *FileIdPool {
	concurrency := wfs.option.ConcurrentWriters
	if concurrency <= 0 {
		concurrency = 128 // match default async flush worker count
	}
	pool := &FileIdPool{
		wfs:       wfs,
		poolSize:  concurrency * 2,
		batchSize: concurrency,
		lowWater:  concurrency,
		maxAge:    25 * time.Second, // conservative; JWT TTL is typically 30s+
	}
	pool.cond = sync.NewCond(&pool.mu)
	return pool
}

// Get returns a pre-allocated file ID entry. If the pool is empty and a refill
// is in progress, callers wait for it to complete rather than failing. Returns
// an error only if the Assign RPC fails.
func (p *FileIdPool) Get() (FileIdEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for {
		p.evictExpired()

		if len(p.entries) > 0 {
			entry := p.entries[0]
			p.entries = p.entries[1:]
			if len(p.entries) < p.lowWater && !p.filling {
				p.filling = true
				go p.doRefill()
			}
			return entry, nil
		}

		// Pool empty.
		if p.filling {
			// Wait for the in-flight refill to complete.
			p.cond.Wait()
			continue
		}

		// No refill in progress — start one synchronously.
		p.filling = true
		p.mu.Unlock()
		entries, err := p.assignBatch(p.batchSize)
		p.mu.Lock()
		p.filling = false
		p.cond.Broadcast()

		if err != nil {
			return FileIdEntry{}, fmt.Errorf("fileIdPool: %w", err)
		}
		p.entries = append(p.entries, entries...)
		// Loop back to pop from entries.
	}
}

func (p *FileIdPool) evictExpired() {
	cutoff := time.Now().Add(-p.maxAge)
	i := 0
	for i < len(p.entries) && p.entries[i].Time.Before(cutoff) {
		i++
	}
	if i > 0 {
		p.entries = p.entries[i:]
	}
}

// doRefill runs in a background goroutine to refill the pool.
func (p *FileIdPool) doRefill() {
	entries, err := p.assignBatch(p.batchSize)
	if err != nil {
		glog.V(1).Infof("fileIdPool refill: %v", err)
	}

	p.mu.Lock()
	if err == nil {
		p.entries = append(p.entries, entries...)
	}
	p.filling = false
	p.cond.Broadcast()
	p.mu.Unlock()
}

// assignBatch requests `count` file IDs from the filer in a single RPC.
// The master allocates `count` sequential needle keys on the same volume.
// We parse the base file ID and generate the full sequence.
//
// Note: the AssignVolumeRequest intentionally omits the Path field. Pooled IDs
// use the mount's global storage parameters, not per-path rules from filer.conf
// (detectStorageOption / MatchStorageRule). This is a writeback cache tradeoff.
func (p *FileIdPool) assignBatch(count int) ([]FileIdEntry, error) {
	var entries []FileIdEntry
	err := p.wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, assignErr := client.AssignVolume(context.Background(), &filer_pb.AssignVolumeRequest{
			Count:            int32(count),
			Replication:      p.wfs.option.Replication,
			Collection:       p.wfs.option.Collection,
			TtlSec:           p.wfs.option.TtlSec,
			DiskType:         string(p.wfs.option.DiskType),
			DataCenter:       p.wfs.option.DataCenter,
			ExpectedDataSize: uint64(p.wfs.option.ChunkSizeLimit),
		})
		if assignErr != nil {
			return assignErr
		}
		if resp.Error != "" {
			return fmt.Errorf("assign: %s", resp.Error)
		}

		now := time.Now()
		host := p.wfs.AdjustedUrl(resp.Location)
		auth := security.EncodedJwt(resp.Auth)
		allocated := int(resp.Count)
		if allocated <= 0 {
			allocated = 1
		}

		if allocated == 1 {
			entries = append(entries, FileIdEntry{
				FileId: resp.FileId,
				Host:   host,
				Auth:   auth,
				Time:   now,
			})
			return nil
		}

		// Parse the base file ID to generate sequential IDs.
		// Format: "volumeId,needleKeyHexCookieHex"
		// Sequential IDs increment the needle key by 1 each, same volume+cookie.
		baseFid, parseErr := needle.ParseFileIdFromString(resp.FileId)
		if parseErr != nil {
			// Fallback: can't parse, just use the single base ID.
			entries = append(entries, FileIdEntry{
				FileId: resp.FileId,
				Host:   host,
				Auth:   auth,
				Time:   now,
			})
			return nil
		}

		baseKey := uint64(baseFid.Key)
		for i := 0; i < allocated; i++ {
			fid := needle.NewFileId(baseFid.VolumeId, baseKey+uint64(i), uint32(baseFid.Cookie))
			entries = append(entries, FileIdEntry{
				FileId: fid.String(),
				Host:   host,
				Auth:   auth,
				Time:   now,
			})
		}
		return nil
	})
	return entries, err
}
