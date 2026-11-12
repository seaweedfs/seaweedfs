package s3api

import (
	"context"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// filerSizeFunc sizes one bucket from its filer listing (bucketsize.Size).
type filerSizeFunc func(ctx context.Context, client filer_pb.FilerClient, bucketDir util.FullPath) (uint64, error)

const (
	// quotaSizeMinAge is the floor of how long a walked size stays usable
	// and of how long one walk may run.
	quotaSizeMinAge = 10 * time.Minute
	// quotaWalkMinBackoff is the first pause before a failed bucket is
	// walked again; it doubles up to the interval.
	quotaWalkMinBackoff = 10 * time.Second
)

// filerQuotaSizer walks remote-mounted buckets in the background, one at a
// time, least recently walked first, so each is re-walked about every
// interval, and publishes the sizes for the quota passes to read. A walk
// that takes long delays only the refresh of the buckets behind it, never a
// quota pass. The lock guards the maps only, never a walk.
type filerQuotaSizer struct {
	client      filer_pb.FilerClient
	walk        filerSizeFunc
	interval    time.Duration
	walkTimeout time.Duration    // one walk's deadline
	now         func() time.Time // the clock for ages and backoff
	wake        chan struct{}

	mu       sync.Mutex
	targets  map[string]quotaTarget
	nextGen  uint64
	sizes    map[string]filerQuotaSize
	inFlight *quotaWalk
}

// quotaTarget is a bucket to walk; gen changes each time the bucket becomes
// a target again, so a walk started before that is not published.
type quotaTarget struct {
	dir util.FullPath
	gen uint64
}

type quotaWalk struct {
	name   string
	gen    uint64
	cancel context.CancelFunc
}

type filerQuotaSize struct {
	size        uint64
	walkedAt    time.Time // when the last successful walk finished; zero if none
	failed      bool      // the last walk failed
	attemptedAt time.Time // when the last walk started
	fails       int       // consecutive failures
	retryAt     time.Time // a failed bucket is not walked again before this
}

func newFilerQuotaSizer(client filer_pb.FilerClient, walk filerSizeFunc, interval time.Duration) *filerQuotaSizer {
	return &filerQuotaSizer{
		client:      client,
		walk:        walk,
		interval:    interval,
		walkTimeout: max(interval, quotaSizeMinAge),
		now:         time.Now,
		wake:        make(chan struct{}, 1),
		targets:     make(map[string]quotaTarget),
		sizes:       make(map[string]filerQuotaSize),
	}
}

// maxAge is how long a walked size is used: twice the interval, at least
// quotaSizeMinAge. An older size is unknown.
func (s *filerQuotaSizer) maxAge() time.Duration {
	return max(2*s.interval, quotaSizeMinAge)
}

// setTargets replaces the buckets to walk. A bucket that stops being a
// target loses its size and its walk in progress is cancelled; a new target
// wakes the sizer.
func (s *filerQuotaSizer) setTargets(dirs map[string]util.FullPath) {
	if s == nil {
		return
	}
	s.mu.Lock()
	added := false
	targets := make(map[string]quotaTarget, len(dirs))
	for name, dir := range dirs {
		if old, found := s.targets[name]; found && old.dir == dir {
			targets[name] = old
			continue
		}
		s.nextGen++
		targets[name] = quotaTarget{dir: dir, gen: s.nextGen}
		delete(s.sizes, name)
		added = true
	}
	for name := range s.sizes {
		if _, keep := targets[name]; !keep {
			delete(s.sizes, name)
		}
	}
	if w := s.inFlight; w != nil && targets[w.name].gen != w.gen {
		w.cancel()
	}
	s.targets = targets
	s.mu.Unlock()
	if added {
		s.signalWake()
	}
}

func (s *filerQuotaSizer) signalWake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// size is the bucket's last walked size, whether it is fresh (walked
// successfully within maxAge), and whether the last walk succeeded.
func (s *filerQuotaSizer) size(bucket string) (size uint64, fresh, lastOK bool) {
	if s == nil {
		return 0, false, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v, found := s.sizes[bucket]
	if !found || v.walkedAt.IsZero() {
		return 0, false, false
	}
	return v.size, s.now().Sub(v.walkedAt) <= s.maxAge(), !v.failed
}

// next picks the target least recently walked (never walked first, ties by
// name) among those not backing off; when none is due it returns the
// earliest time one will be.
func (s *filerQuotaSizer) next(now time.Time) (name string, target quotaTarget, targets int, retryAt time.Time, found bool) {
	var oldest time.Time
	for n, t := range s.targets {
		v := s.sizes[n]
		if v.retryAt.After(now) {
			if retryAt.IsZero() || v.retryAt.Before(retryAt) {
				retryAt = v.retryAt
			}
			continue
		}
		if !found || v.attemptedAt.Before(oldest) || (v.attemptedAt.Equal(oldest) && n < name) {
			name, target, oldest, found = n, t, v.attemptedAt, true
		}
	}
	return name, target, len(s.targets), retryAt, found
}

// walkOne walks the least recently walked due target under its own deadline
// and publishes the result if the bucket is still the same target. It
// reports how many targets there were, whether it walked, and otherwise when
// a backing-off target is due.
func (s *filerQuotaSizer) walkOne(ctx context.Context) (targets int, walked bool, retryAt time.Time) {
	s.mu.Lock()
	started := s.now()
	name, target, targets, retryAt, found := s.next(started)
	if !found {
		s.mu.Unlock()
		return targets, false, retryAt
	}
	walkCtx, cancel := context.WithTimeout(ctx, s.walkTimeout)
	defer cancel()
	s.inFlight = &quotaWalk{name: name, gen: target.gen, cancel: cancel}
	s.mu.Unlock()

	size, err := s.walk(walkCtx, s.client, target.dir)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.inFlight = nil
	if current, still := s.targets[name]; !still || current.gen != target.gen {
		return targets, true, time.Time{}
	}
	v := s.sizes[name]
	v.attemptedAt = started
	if err != nil {
		v.failed = true
		v.fails++
		v.retryAt = s.now().Add(s.backoff(v.fails))
		glog.Warningf("bucket %s quota enforcement: size from filer: %v (read-only state left unchanged)", name, err)
	} else {
		v.size, v.walkedAt, v.failed, v.fails, v.retryAt = size, s.now(), false, 0, time.Time{}
	}
	s.sizes[name] = v
	return targets, true, time.Time{}
}

// backoff is the pause after the n-th consecutive failure: 10 s doubling,
// up to the interval (but never under 10 s, even with interval 0).
func (s *filerQuotaSizer) backoff(fails int) time.Duration {
	limit := max(s.interval, quotaWalkMinBackoff)
	d := quotaWalkMinBackoff
	for i := 1; i < fails && d < limit; i++ {
		d *= 2
	}
	return min(d, limit)
}

// run walks until ctx ends, spreading the walks so each target comes round
// about every interval (0: back to back); a new target ends any pause.
func (s *filerQuotaSizer) run(ctx context.Context) {
	for ctx.Err() == nil {
		// this pick sees every target set so far: a wake from before it is spent
		select {
		case <-s.wake:
		default:
		}
		started := time.Now()
		targets, walked, retryAt := s.walkOne(ctx)
		switch {
		case targets == 0:
			s.pause(ctx, -1)
		case !walked:
			s.pause(ctx, max(retryAt.Sub(s.now()), time.Millisecond))
		default:
			s.pause(ctx, paceAfterWalk(s.interval, targets, time.Since(started)))
		}
	}
}

// pause waits d, or until woken or ctx ends; a negative d waits for a wake.
func (s *filerQuotaSizer) pause(ctx context.Context, d time.Duration) {
	if d == 0 {
		return
	}
	var timer <-chan time.Time
	if d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		timer = t.C
	}
	select {
	case <-ctx.Done():
	case <-s.wake:
	case <-timer:
	}
}

// paceAfterWalk is the pause after a walk that took took, so that walking
// targets buckets one after another comes round about every interval.
func paceAfterWalk(interval time.Duration, targets int, took time.Duration) time.Duration {
	if interval <= 0 || targets <= 0 {
		return 0
	}
	if gap := interval / time.Duration(targets); took < gap {
		return gap - took
	}
	return 0
}
