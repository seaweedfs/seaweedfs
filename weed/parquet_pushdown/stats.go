package parquet_pushdown

import (
	"time"

	pb "github.com/seaweedfs/seaweedfs/weed/pb/parquet_pushdown_pb"
)

// statsRecorder accumulates per-request stats and converts to the
// wire form when the handler returns. M0 only records trust mode and
// elapsed server time; later milestones extend it with cache hits,
// pruning counts, and indexes-used / indexes-missing labels.
type statsRecorder struct {
	start          time.Time
	trustMode      TrustMode
	indexesUsed    []string
	indexesMissing []string
}

func newStats(trustMode TrustMode) *statsRecorder {
	return &statsRecorder{start: time.Now(), trustMode: trustMode}
}

func (r *statsRecorder) markIndexUsed(kind string) {
	r.indexesUsed = append(r.indexesUsed, kind)
}

func (r *statsRecorder) markIndexMissing(kind string) {
	r.indexesMissing = append(r.indexesMissing, kind)
}

func (r *statsRecorder) toProto() *pb.PushdownStats {
	return &pb.PushdownStats{
		TrustMode:        string(r.trustMode),
		ServerTimeMicros: time.Since(r.start).Microseconds(),
		IndexesUsed:      r.indexesUsed,
		IndexesMissing:   r.indexesMissing,
	}
}
