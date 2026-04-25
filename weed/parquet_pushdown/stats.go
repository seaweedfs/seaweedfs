package parquet_pushdown

import (
	"time"

	pb "github.com/seaweedfs/seaweedfs/weed/pb/parquet_pushdown_pb"
)

// statsRecorder accumulates per-request stats and converts to the
// wire form when the handler returns. Counts here are scoped to a
// single Pushdown call, not process-wide.
type statsRecorder struct {
	start             time.Time
	trustMode         TrustMode
	footerCacheHits   int64
	footerCacheMisses int64
	indexesUsed       []string
	indexesMissing    []string
}

func newStats(trustMode TrustMode) *statsRecorder {
	return &statsRecorder{start: time.Now(), trustMode: trustMode}
}

func (r *statsRecorder) recordFooterCacheHit()  { r.footerCacheHits++ }
func (r *statsRecorder) recordFooterCacheMiss() { r.footerCacheMisses++ }

func (r *statsRecorder) markIndexUsed(kind string) {
	r.indexesUsed = append(r.indexesUsed, kind)
}

func (r *statsRecorder) markIndexMissing(kind string) {
	r.indexesMissing = append(r.indexesMissing, kind)
}

func (r *statsRecorder) toProto() *pb.PushdownStats {
	return &pb.PushdownStats{
		TrustMode:         string(r.trustMode),
		ServerTimeMicros:  time.Since(r.start).Microseconds(),
		FooterCacheHits:   r.footerCacheHits,
		FooterCacheMisses: r.footerCacheMisses,
		IndexesUsed:       r.indexesUsed,
		IndexesMissing:    r.indexesMissing,
	}
}
