package http

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"
)

// A chunk read whose location goes quiet is asked of another location as
// well. A volume server sends a chunk without pauses once it has answered, so
// a body that stops arriving for chunkReadStallDelay is waiting for a lost TCP
// segment: the retransmission timeout is at least 200 ms and doubles with
// every further loss, so on a lossy path one chunk can hold up a sequential
// reader for seconds, while a fresh request takes milliseconds. Half the
// minimum retransmission timeout still gets ahead of every such wait, and
// lets the shorter pauses that recover on their own go by without a hedge.
// Before the response headers the server is still reading the needle, which
// can take a while on a busy disk, so a location gets chunkReadAnswerDelay to
// answer; one silent for that long is on a hung or dead host, or its answer
// was lost. Tests shorten both.
var (
	chunkReadAnswerDelay = 200 * time.Millisecond
	chunkReadStallDelay  = 100 * time.Millisecond
)

// maxHedgesPerRead caps the extra requests a single chunk read can make.
const maxHedgesPerRead = 2

// errNoAnswer marks a location that had not even sent its response headers by
// the time another location delivered the chunk.
var errNoAnswer = errors.New("no response headers before another location delivered the chunk")

// Hedging is budgeted so that a cluster that is slow everywhere is not asked
// for every chunk twice: every chunk read earns a tenth of a token, every
// hedge spends a whole one, and at most hedgeTokenBurst tokens are saved up.
// Hedges therefore stay below a tenth of the reads, apart from a burst of ten.
const (
	hedgeTokenScale = 1000 // tokens are counted in thousandths
	hedgeTokenEarn  = hedgeTokenScale / 10
	hedgeTokenBurst = 10 * hedgeTokenScale
)

var hedgeTokens = func() *atomic.Int64 {
	var tokens atomic.Int64
	tokens.Store(hedgeTokenBurst)
	return &tokens
}()

func earnHedgeToken() {
	for {
		tokens := hedgeTokens.Load()
		if tokens >= hedgeTokenBurst || hedgeTokens.CompareAndSwap(tokens, min(tokens+hedgeTokenEarn, hedgeTokenBurst)) {
			return
		}
	}
}

func spendHedgeToken() bool {
	for {
		tokens := hedgeTokens.Load()
		if tokens < hedgeTokenScale {
			return false
		}
		if hedgeTokens.CompareAndSwap(tokens, tokens-hedgeTokenScale) {
			return true
		}
	}
}

// chunkProgress is what a chunk read reports while it runs: when something
// last arrived, and whether the response headers did. A nil one ignores it.
type chunkProgress struct {
	lastArrival atomic.Int64 // unix nanoseconds
	answered    atomic.Bool
}

func (p *chunkProgress) arrived() {
	if p != nil {
		p.lastArrival.Store(time.Now().UnixNano())
	}
}

func (p *chunkProgress) answer() {
	if p != nil {
		p.answered.Store(true)
		p.arrived()
	}
}

// progressReader reports every piece of a response body that arrives.
type progressReader struct {
	reader   io.Reader
	progress *chunkProgress
}

func (r progressReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.progress.arrived()
	}
	return n, err
}

// chunkAttempt reads a chunk from one location into dst and returns how many
// bytes it read. It reports to progress what arrives, and stops when ctx ends.
// A failure comes with whether another location is worth asking.
type chunkAttempt func(ctx context.Context, url string, dst []byte, progress *chunkProgress) (n int, retryable bool, err error)

type hedgedAttempt struct {
	url       string
	dst       []byte
	scratch   bool // dst is not the caller's buffer and is freed when the attempt ends
	cancel    context.CancelFunc
	started   time.Time
	progress  chunkProgress
	finished  bool
	abandoned bool
}

type attemptResult struct {
	attempt   *hedgedAttempt
	n         int
	retryable bool
	err       error
}

// hedgedChunkPass makes one pass over urls for a chunk read into buffer.
//
// Like a plain loop over the locations it asks them in order and moves on to
// the next one as soon as one fails. Unlike it, it does not wait out a location
// that goes quiet: once every request in flight has been quiet for longer than
// it may be (chunkReadAnswerDelay before its response headers arrive,
// chunkReadStallDelay after), the read is asked of the next location as well.
// Once every location has been asked, a location whose server answered and
// then went quiet is asked again over another connection: the server is up,
// and only that connection is stuck. A location that has not answered is left
// alone then, as it was before. Whichever request delivers the whole chunk
// first wins, and the others are cancelled.
//
// Only one request at a time reads into buffer; the others read into buffers
// of their own, and a winner's data is copied over once the request reading
// into buffer has ended. buffer is not written after hedgedChunkPass returns.
//
// It returns the number of bytes read, whether a location failed or did not
// answer at all on the way, which says the caller's location list may be
// stale, and the error of the pass with whether it is worth another pass. A
// failure that another location would not fix stops the asking, though a
// request already in flight may still deliver the chunk.
func hedgedChunkPass(ctx context.Context, urls []string, buffer []byte, attempt chunkAttempt) (n int, failed bool, retryable bool, err error) {
	if len(urls) == 0 {
		return 0, false, false, nil
	}
	earnHedgeToken()

	results := make(chan attemptResult, len(urls)+maxHedgesPerRead)
	var (
		attempts   []*hedgedAttempt
		owner      *hedgedAttempt // the request reading into buffer
		running    int
		next       int // the next location not asked yet
		hedges     int
		hedgeOff   bool // the budget ran out during this pass
		stopAsking bool
		finalErr   error
	)

	start := func(url string) {
		a := &hedgedAttempt{url: url, started: time.Now()}
		if owner == nil {
			a.dst, owner = buffer, a
		} else {
			a.dst, a.scratch = mem.Allocate(len(buffer)), true
		}
		attemptCtx, cancel := context.WithCancel(ctx)
		a.cancel = cancel
		a.progress.arrived()
		attempts = append(attempts, a)
		running++
		go func() {
			m, r, e := attempt(attemptCtx, url, a.dst, &a.progress)
			results <- attemptResult{attempt: a, n: m, retryable: r, err: e}
		}()
	}

	// settle takes in the result of a request that ended.
	settle := func(r attemptResult) {
		running--
		r.attempt.finished = true
		r.attempt.cancel()
		if r.attempt == owner {
			owner = nil
		}
	}

	// end cancels the requests still running and waits for the one reading
	// into buffer. The others release their buffers in the background.
	end := func() {
		for _, a := range attempts {
			if !a.finished {
				a.abandoned = true
				a.cancel()
			}
		}
		for owner != nil {
			r := <-results
			settle(r)
			if r.attempt.scratch {
				mem.Free(r.attempt.dst)
			}
		}
		if running > 0 {
			go func(left int) {
				for ; left > 0; left-- {
					if r := <-results; r.attempt.scratch {
						mem.Free(r.attempt.dst)
					}
				}
			}(running)
		}
	}

	// hedgeAt is when every request in flight will have been quiet for longer
	// than it may be, unless something arrives first.
	hedgeAt := func() time.Time {
		var at time.Time
		for _, a := range attempts {
			if a.finished {
				continue
			}
			quietLimit := chunkReadAnswerDelay
			if a.progress.answered.Load() {
				quietLimit = chunkReadStallDelay
			}
			if t := time.Unix(0, a.progress.lastArrival.Load()).Add(quietLimit); t.After(at) {
				at = t
			}
		}
		return at
	}

	// stuckAnswered is the request that answered and has been quiet the
	// longest, if any: its location is worth asking again.
	stuckAnswered := func() *hedgedAttempt {
		var pick *hedgedAttempt
		for _, a := range attempts {
			if !a.finished && a.progress.answered.Load() &&
				(pick == nil || a.progress.lastArrival.Load() < pick.progress.lastArrival.Load()) {
				pick = a
			}
		}
		return pick
	}

	start(urls[next])
	next++

	timer := time.NewTimer(chunkReadAnswerDelay)
	defer timer.Stop()

	for running > 0 {
		var hedge <-chan time.Time
		if !stopAsking && !hedgeOff && hedges < maxHedgesPerRead {
			// A request that answers meanwhile may be quiet only for the
			// shorter chunkReadStallDelay from then on: look again by then.
			wait := chunkReadStallDelay
			if next < len(urls) || stuckAnswered() != nil {
				wait = min(time.Until(hedgeAt()), wait)
			}
			timer.Reset(max(wait, time.Millisecond))
			hedge = timer.C
		}

		select {
		case <-ctx.Done():
			end()
			return 0, failed, false, ctx.Err()

		case <-hedge:
			if time.Now().Before(hedgeAt()) {
				continue // something arrived meanwhile
			}
			// a location not asked yet, or again one whose answer got stuck
			var url string
			unasked := next < len(urls)
			if unasked {
				url = urls[next]
			} else if stuck := stuckAnswered(); stuck != nil {
				url = stuck.url
			} else {
				continue // nowhere worth asking yet
			}
			if !spendHedgeToken() {
				hedgeOff = true
				continue
			}
			if unasked {
				next++
			}
			hedges++
			glog.V(1).InfofCtx(ctx, "chunk read went quiet, asking %s as well", url)
			start(url)

		case r := <-results:
			settle(r)
			a := r.attempt
			if a.abandoned {
				if a.scratch {
					mem.Free(a.dst)
				}
				continue
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				if a.scratch {
					mem.Free(a.dst)
				}
				end()
				return 0, failed, false, ctxErr
			}
			if r.err == nil {
				// The chunk is here. A location that has not even answered by
				// now is tried last for a while, like one that cannot be
				// reached, and the location list it came from may be stale.
				for _, other := range attempts {
					if !other.finished && !other.progress.answered.Load() && time.Since(other.started) >= chunkReadAnswerDelay {
						recordUnreachable(hostOf(other.url), errNoAnswer)
						failed = true
					}
				}
				end()
				if a.scratch {
					copy(buffer, a.dst[:r.n])
					mem.Free(a.dst)
				}
				return r.n, failed, false, nil
			}
			if a.scratch {
				mem.Free(a.dst)
			}
			if !r.retryable {
				if finalErr == nil {
					finalErr = r.err
				}
				stopAsking = true
				continue
			}
			failed = true
			err, retryable = r.err, true
			glog.V(0).InfofCtx(ctx, "read %s failed, err: %v", a.url, r.err)
			if !stopAsking && next < len(urls) {
				start(urls[next])
				next++
			}
		}
	}

	if finalErr != nil {
		return 0, failed, false, finalErr
	}
	return 0, failed, retryable, err
}
