package dailyrun

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/stats"
)

// Bounds what a summary carries per pass; the counts stay exact.
const (
	maxReportedBucketFailures = 10
	maxReportedErrorLen       = 256
)

const (
	PhaseWalk   = "walk"
	PhaseReplay = "replay"
)

type bucketState uint8

const (
	bucketSkipped bucketState = iota + 1
	bucketWalked
	bucketFailed
)

type BucketFailure struct {
	Bucket string
	Error  string
}

// RunSummary is a copy of a RunStats at one point in time. A bucket walked
// more than once in a pass counts once, as failed if any walk failed.
type RunSummary struct {
	Phase          string
	Bucket         string
	BucketsWalked  int
	BucketsFailed  int
	BucketsSkipped int
	// The first maxReportedBucketFailures failed buckets.
	BucketFailures []BucketFailure
	// Every listed entry, versions and delete markers included.
	EntriesVisited int64
	// Distinct object keys among the listed entries.
	ObjectsVisited int64
	EventsScanned  int64
	// LifecycleDelete outcomes keyed by the dispatch_total outcome label.
	// They count RPC results, not reclaimed space.
	Outcomes    map[string]int64
	ShardErrors int
	// Shards whose replay stopped on an unresolved delete; their due
	// events wait for a later pass.
	ShardsHalted int
	// Shards whose walk-only rules waited for WalkerInterval.
	WalksDeferred int
}

// RunStats collects one pass's work. Safe for concurrent use; a nil
// *RunStats ignores every call.
type RunStats struct {
	entries atomic.Int64
	objects atomic.Int64
	events  atomic.Int64

	mu           sync.Mutex
	phase        string
	bucket       string
	buckets      map[string]bucketState
	failures     []BucketFailure
	outcomes     map[string]int64
	shardErrors  int
	shardsHalted int
	deferred     int
}

func (s *RunStats) startBucket(bucket string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.phase, s.bucket = PhaseWalk, bucket
	s.mu.Unlock()
}

func (s *RunStats) finishBucket(bucket string, state bucketState, err error) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.buckets == nil {
		s.buckets = map[string]bucketState{}
	}
	prev := s.buckets[bucket]
	if state <= prev {
		return
	}
	s.buckets[bucket] = state
	if state == bucketFailed && len(s.failures) < maxReportedBucketFailures {
		s.failures = append(s.failures, BucketFailure{Bucket: bucket, Error: truncateError(err)})
	}
}

func (s *RunStats) startReplay() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.phase, s.bucket = PhaseReplay, ""
	s.mu.Unlock()
}

func (s *RunStats) visitEntry(newObject bool) {
	if s == nil {
		return
	}
	s.entries.Add(1)
	if newObject {
		s.objects.Add(1)
	}
}

func (s *RunStats) scanEvent() {
	if s == nil {
		return
	}
	s.events.Add(1)
}

func (s *RunStats) recordOutcome(outcome string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.outcomes == nil {
		s.outcomes = map[string]int64{}
	}
	s.outcomes[outcome]++
	s.mu.Unlock()
}

func (s *RunStats) setShardErrors(n int) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.shardErrors = n
	s.mu.Unlock()
}

func (s *RunStats) shardHalted() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.shardsHalted++
	s.mu.Unlock()
}

func (s *RunStats) walkDeferred() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.deferred++
	s.mu.Unlock()
}

func (s *RunStats) Summary() RunSummary {
	if s == nil {
		return RunSummary{}
	}
	out := RunSummary{
		EntriesVisited: s.entries.Load(),
		ObjectsVisited: s.objects.Load(),
		EventsScanned:  s.events.Load(),
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out.Phase, out.Bucket = s.phase, s.bucket
	for _, state := range s.buckets {
		switch state {
		case bucketWalked:
			out.BucketsWalked++
		case bucketFailed:
			out.BucketsFailed++
		case bucketSkipped:
			out.BucketsSkipped++
		}
	}
	out.BucketFailures = append([]BucketFailure(nil), s.failures...)
	if len(s.outcomes) > 0 {
		out.Outcomes = make(map[string]int64, len(s.outcomes))
		for k, v := range s.outcomes {
			out.Outcomes[k] = v
		}
	}
	out.ShardErrors = s.shardErrors
	out.ShardsHalted = s.shardsHalted
	out.WalksDeferred = s.deferred
	return out
}

func countDispatch(s *RunStats, bucket, kind, outcome string) {
	stats.S3LifecycleDispatchCounter.WithLabelValues(bucket, kind, outcome).Inc()
	s.recordOutcome(outcome)
}

func truncateError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if len(msg) <= maxReportedErrorLen {
		return msg
	}
	return strings.ToValidUTF8(msg[:maxReportedErrorLen], "") + "..."
}
