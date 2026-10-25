package oidc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/providers"
)

const (
	// clockSkewTolerance matches the typical JWT library clock skew allowance (RFC 7519)
	clockSkewTolerance = 5 * time.Minute
	// defaultJTICacheSize is the default maximum number of JTI entries to cache
	defaultJTICacheSize = 10000
	// defaultCleanupInterval is how often to scan for expired JTI entries
	defaultCleanupInterval = 5 * time.Minute
	// eagerCleanupMinInterval rate limits opportunistic sweeps near capacity
	eagerCleanupMinInterval = 5 * time.Second
	// tokenFingerprintPrefix marks replay-cache keys derived from the token's
	// own fingerprint, used when the token carries no usable jti claim
	tokenFingerprintPrefix = "fp:"
)

// jtiEntry represents a cached JTI with metadata
type jtiEntry struct {
	subject   string
	expiresAt time.Time
	// uncounted marks an entry merged in past the capacity bound: it keeps
	// rejecting replays but does not charge the count, so merged excess can
	// neither reject new exchanges nor be double-counted when swept.
	uncounted bool
}

// jtiReplayCache wraps the consumed-token store that backs JTI replay
// protection. Each consumed token identifier is held until the token's exp
// plus clock skew, and a second presentation of the same identifier is
// rejected with ErrProviderTokenReplayed.
//
// The cache is bounded by maxSize and uses automatic TTL-based expiration:
// a background worker sweeps expired entries, an eager sweep runs when the
// cache nears capacity, and the consume path reclaims expired slots before
// rejecting a fresh token for capacity.
//
// Thread Safety:
// All methods are safe for concurrent use. The consume path uses atomic
// LoadOrStore to prevent TOCTOU races.
type jtiReplayCache struct {
	name string // owning provider name, for log messages

	enabled   atomic.Bool
	maxSize   atomic.Int64
	lastSweep atomic.Int64 // unix nano of the last sweep; rate limits eager cleanups

	// storeMu guards the store/count pair: replay-state handoffs (adoptFrom,
	// mergeFrom) repoint them under the write lock while the consume path
	// holds a read lock, so a consume can never land in a store the handoff
	// already drained and abandoned.
	// It is always acquired before sweepMu, never while holding it.
	storeMu sync.RWMutex
	store   *sync.Map     // map[string]*jtiEntry - atomic storage for consumed token keys
	count   *atomic.Int64 // entries in store; shared with the store

	// mu guards the lifecycle of the cleanup worker: cleanupCancel and
	// cleanupDone. The worker itself only touches store/count, so holding
	// mu while waiting for it is safe.
	mu            sync.Mutex
	cleanupCancel context.CancelFunc
	cleanupDone   chan struct{} // Closed when cleanup goroutine exits

	// sweepMu serializes cache sweeps so at most one scan runs at a time.
	sweepMu sync.Mutex
}

func newJTIReplayCache(name string) *jtiReplayCache {
	return &jtiReplayCache{
		name:  name,
		store: &sync.Map{},
		count: &atomic.Int64{},
	}
}

// configure applies the replay-protection settings: a previous worker is
// always stopped, whether the new configuration enables protection or
// disables it. The consumed-token store survives re-configuration: resetting
// it would let already-seen tokens be replayed against the refreshed provider.
func (c *jtiReplayCache) configure(enabled bool, maxSize int64) {
	c.mu.Lock()
	c.stopWorkerLocked()
	if enabled {
		if maxSize <= 0 {
			maxSize = defaultJTICacheSize
		}
		// the bound must land before the flag: a consume that sees enabled
		// with a zero bound would reject the first token it registers
		c.maxSize.Store(maxSize)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		c.cleanupCancel = cancel
		c.cleanupDone = done
		c.enabled.Store(true)
		c.mu.Unlock()

		go c.cleanupLoop(ctx, done, defaultCleanupInterval)
		glog.V(2).Infof("OIDC provider %q: JTI replay protection enabled (max size: %d)", c.name, maxSize)
	} else {
		c.enabled.Store(false)
		c.mu.Unlock()
		glog.Warningf("OIDC provider %q: JTI replay protection is DISABLED", c.name)
	}
}

// stopWorkerLocked cancels the current cleanup goroutine and waits briefly
// for it to exit. Caller must hold mu.
func (c *jtiReplayCache) stopWorkerLocked() {
	if c.cleanupCancel == nil {
		return
	}
	c.cleanupCancel()
	select {
	case <-c.cleanupDone:
		glog.V(3).Infof("OIDC provider %q: JTI cleanup goroutine exited", c.name)
	case <-time.After(2 * time.Second):
		glog.Warningf("OIDC provider %q: Timeout waiting for JTI cleanup goroutine", c.name)
	}
	c.cleanupCancel = nil
	c.cleanupDone = nil
}

// consume marks a token as used and rejects a second presentation of it.
// jti is the token's jti claim (may be empty); tokenExpiresAt is its exp.
// A no-op while protection is disabled.
func (c *jtiReplayCache) consume(token, jti, subject string, tokenExpiresAt time.Time) error {
	if !c.enabled.Load() {
		return nil
	}

	key := jti
	if key == "" {
		// Providers that issue no jti cannot be tracked by claim, so the
		// signed input keys the entry: a repeated presentation of the same
		// claims is a replay even when the signature bytes differ (e.g.
		// ECDSA s/N-s malleability).
		signingInput := token
		if i := strings.LastIndexByte(token, '.'); i > 0 {
			signingInput = token[:i]
		}
		sum := sha256.Sum256([]byte(signingInput))
		key = tokenFingerprintPrefix + hex.EncodeToString(sum[:])
	}

	if tokenExpiresAt.IsZero() {
		// a cache slot is freed only by expiry, so a token without exp
		// would hold it forever and eventually exhaust the bound
		return fmt.Errorf("%w: missing exp claim", providers.ErrProviderMissingClaims)
	}
	expiresAt := tokenExpiresAt.Add(clockSkewTolerance)
	if !time.Now().Before(expiresAt) {
		return fmt.Errorf("%w: token expired", providers.ErrProviderTokenExpired)
	}

	entry := &jtiEntry{
		subject:   subject,
		expiresAt: expiresAt,
	}

	// The read lock pins the store/count pair for the whole test-and-set
	// so a replay-state handoff cannot repoint them mid-check and strand
	// this consume in an abandoned store.
	c.storeMu.RLock()
	store, count := c.store, c.count

	// Atomic test-and-set detects replays before any capacity work runs,
	// so a replayed token cannot trigger cleanup scans.
	if _, loaded := store.LoadOrStore(key, entry); loaded {
		c.storeMu.RUnlock()
		glog.Warningf("OIDC provider %q: Token replay detected (jti: %s)",
			c.name, jtiLogKey(key))
		return fmt.Errorf("%w: jti %s has already been used",
			providers.ErrProviderTokenReplayed, jtiLogKey(key))
	}

	// The count is incremented as part of the insert, so concurrent
	// validations cannot all slip under the limit: whoever pushes the
	// count over evicts their own entry and is rejected.
	newCount := count.Add(1)
	maxSize := c.maxSize.Load()
	if newCount > maxSize {
		// Over the bound: expired entries may still occupy slots, so
		// reclaim them with a synchronized sweep before rejecting.
		c.sweepStore(store, count)
		if count.Load() > maxSize {
			if _, loaded := store.LoadAndDelete(key); loaded {
				count.Add(-1)
			}
			c.storeMu.RUnlock()
			glog.Errorf("OIDC provider %q: JTI cache FULL (%d entries) - rejecting token. "+
				"This may indicate a DoS attack or insufficient cache size. Consider increasing JTICacheMaxSize.",
				c.name, maxSize)
			return fmt.Errorf("JTI cache capacity exceeded")
		}
	} else if newCount >= maxSize*9/10 {
		glog.Warningf("OIDC provider %q: JTI cache at %d%% capacity (%d/%d), triggering eager cleanup",
			c.name, (newCount*100)/maxSize, newCount, maxSize)
		go c.performEagerCleanup()
	}
	c.storeMu.RUnlock()

	glog.V(4).Infof("OIDC provider %q: Registered JTI %s (expires at %v, cache size: %d)",
		c.name, jtiLogKey(key), expiresAt, newCount)
	return nil
}

// evictExpired removes entries whose expiry has passed and returns how
// many it deleted. Caller must hold sweepMu.
func (c *jtiReplayCache) evictExpired(now time.Time, store *sync.Map, count *atomic.Int64) int {
	removed := 0
	store.Range(func(key, value interface{}) bool {
		expiresAt := value.(*jtiEntry).expiresAt
		if !expiresAt.IsZero() && now.After(expiresAt) {
			// LoadAndDelete reports whether this call actually removed the
			// entry, so concurrent deletes cannot decrement the count twice.
			if v, loaded := store.LoadAndDelete(key); loaded {
				if !v.(*jtiEntry).uncounted {
					count.Add(-1)
				}
				removed++
			}
		}
		return true
	})
	return removed
}

// state returns the cache's current store/count pair.
func (c *jtiReplayCache) state() (*sync.Map, *atomic.Int64) {
	c.storeMu.RLock()
	defer c.storeMu.RUnlock()
	return c.store, c.count
}

// sweepStore runs one serialized expiry scan over the given store. It only
// takes sweepMu, so callers already holding storeMu may use it.
func (c *jtiReplayCache) sweepStore(store *sync.Map, count *atomic.Int64) {
	c.sweepMu.Lock()
	defer c.sweepMu.Unlock()
	c.lastSweep.Store(time.Now().UnixNano())
	if removed := c.evictExpired(time.Now(), store, count); removed > 0 {
		glog.V(4).Infof("OIDC provider %q: Cleaned up %d expired JTI entries (remaining: %d)",
			c.name, removed, count.Load())
	}
}

// sweepExpired runs a scan that is guaranteed to complete before the
// caller proceeds; the periodic worker uses it so the scan always covers
// the store currently in effect, not one captured earlier.
func (c *jtiReplayCache) sweepExpired() {
	store, count := c.state()
	c.sweepStore(store, count)
}

// performEagerCleanup performs an opportunistic cleanup of expired JTI
// entries when the cache approaches capacity. At most one sweep runs at a
// time and no more often than eagerCleanupMinInterval, so a burst of
// requests near the limit cannot each turn into a full-cache scan.
func (c *jtiReplayCache) performEagerCleanup() {
	if !c.enabled.Load() {
		return
	}
	if time.Since(time.Unix(0, c.lastSweep.Load())) < eagerCleanupMinInterval {
		return
	}
	store, count := c.state()
	if !c.sweepMu.TryLock() {
		return
	}
	defer c.sweepMu.Unlock()
	c.lastSweep.Store(time.Now().UnixNano())

	beforeCount := count.Load()
	if removed := c.evictExpired(time.Now(), store, count); removed > 0 {
		glog.Infof("OIDC provider %q: Eager cleanup removed %d expired JTI entries (%d -> %d)",
			c.name, removed, beforeCount, count.Load())
	} else {
		glog.Warningf("OIDC provider %q: Eager cleanup found no expired entries to remove. "+
			"Cache may be filled with valid long-lived tokens. Consider increasing JTICacheMaxSize.",
			c.name)
	}
}

// cleanupLoop periodically removes expired JTI entries from the cache.
// This prevents memory growth by removing JTIs after their tokens have
// expired. Runs until ctx is cancelled; done is closed on exit.
func (c *jtiReplayCache) cleanupLoop(ctx context.Context, done chan struct{}, interval time.Duration) {
	defer close(done)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	glog.V(3).Infof("OIDC provider %q: JTI cleanup goroutine started (interval: %v)", c.name, interval)

	for {
		select {
		case <-ctx.Done():
			glog.V(3).Infof("OIDC provider %q: JTI cleanup goroutine stopping", c.name)
			return
		case <-ticker.C:
			c.sweepExpired()
		}
	}
}

// stop gracefully stops the cleanup worker.
// Should be called when the provider is being shut down to prevent goroutine
// leaks.
func (c *jtiReplayCache) stop(ctx context.Context) error {
	c.mu.Lock()
	cancel, done := c.cleanupCancel, c.cleanupDone
	c.mu.Unlock()

	if cancel == nil {
		return nil
	}
	cancel()

	select {
	case <-done:
		glog.V(2).Infof("OIDC provider %q: Cleanup goroutine shutdown complete", c.name)
	case <-ctx.Done():
		glog.Warningf("OIDC provider %q: Shutdown timeout waiting for cleanup goroutine: %v", c.name, ctx.Err())
		return ctx.Err()
	}

	c.mu.Lock()
	if c.cleanupDone == done {
		c.cleanupCancel = nil
		c.cleanupDone = nil
	}
	c.mu.Unlock()
	return nil
}

// adoptFrom takes over the consumed-token store of another cache, so tokens
// it already saw stay rejected across the rebuild. The caller must shut down
// the replaced provider; only one cleanup worker may sweep the shared store
// at a time. Call before the new provider serves validators; sweepMu
// excludes its own cleanup worker during the swap.
func (c *jtiReplayCache) adoptFrom(old *jtiReplayCache) {
	if old == nil || old == c {
		return
	}
	c.storeMu.Lock()
	c.sweepMu.Lock()
	c.store, c.count = old.store, old.count
	c.sweepMu.Unlock()
	c.storeMu.Unlock()
}

// mergeFrom hands this cache's store to another cache and then folds the
// other's consumed-token entries in, so rejections the other recorded keep
// holding when its records move to a shared provider. Repointing first means
// tokens the other still consumes — in-flight exchanges or ones routed to it
// before the provider map updates — land in the shared store instead of a
// store this merge already drained. Unlike adoptFrom it swaps nothing of its
// own, so it is safe on a cache that is already serving validators.
func (c *jtiReplayCache) mergeFrom(other *jtiReplayCache) {
	if other == nil || other == c {
		return
	}
	c.storeMu.RLock()
	defer c.storeMu.RUnlock()
	store, count := c.store, c.count

	other.storeMu.Lock()
	oldStore := other.store
	if oldStore == store {
		other.storeMu.Unlock()
		return
	}
	other.store, other.count = store, count
	other.storeMu.Unlock()

	c.sweepMu.Lock()
	defer c.sweepMu.Unlock()
	now := time.Now()
	oldStore.Range(func(key, value interface{}) bool {
		entry := value.(*jtiEntry)
		if expiresAt := entry.expiresAt; !expiresAt.IsZero() && now.After(expiresAt) {
			return true
		}
		// A consumed token is never dropped to fit the bound: merged entries
		// stay but do not charge the count, so a store filled past the bound
		// by the merge cannot reject new exchanges while it drains by expiry.
		store.LoadOrStore(key, &jtiEntry{subject: entry.subject, expiresAt: entry.expiresAt, uncounted: true})
		return true
	})
}

// jtiLogKey redacts a token identifier for logs and error strings: a stable
// fingerprint that still lets operators correlate repeated presentations.
func jtiLogKey(jti string) string {
	sum := sha256.Sum256([]byte(jti))
	return hex.EncodeToString(sum[:8])
}
