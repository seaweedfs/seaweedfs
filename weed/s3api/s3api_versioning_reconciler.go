package s3api

import (
	"context"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// Background reconciler for .versions/ directories whose latest-version
// pointer references a missing file. The inline delete path
// (deleteSpecificObjectVersion -> updateLatestVersionAfterDeletion) can
// leave this state behind if the second step (pointer update / dir rm)
// fails after the first (blob rm) succeeded. Read-path self-heal already
// recovers on the next GET, but that surfaces NoSuchKey to the client.
// The reconciler drains stranded entries proactively so a passing health
// check or an idle key gets repaired without waiting for a client read.

const (
	versionsHealQueueCapacity = 4096
	versionsHealPollInterval  = 5 * time.Second
	versionsHealMaxRetries    = 6
	versionsHealBaseBackoff   = 200 * time.Millisecond
	versionsHealMaxBackoff    = 30 * time.Second
)

type versionsHealCandidate struct {
	bucket    string
	object    string
	enqueued  time.Time
	attempts  int
	nextRetry time.Time
}

type versionsHealQueue struct {
	mu      sync.Mutex
	pending map[string]*versionsHealCandidate
}

func newVersionsHealQueue() *versionsHealQueue {
	return &versionsHealQueue{
		pending: make(map[string]*versionsHealCandidate),
	}
}

func versionsHealKey(bucket, object string) string {
	return bucket + "/" + object
}

// Enqueue records that bucket/object's .versions/ directory may be in a
// stranded state and needs the reconciler to verify and heal it. Bounded
// by versionsHealQueueCapacity so a stuck filer can't grow the map
// without limit; on overflow we drop the newest candidate (the heal will
// run on the next read or the next delete-failure for that key).
func (q *versionsHealQueue) Enqueue(bucket, object string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	key := versionsHealKey(bucket, object)
	if _, ok := q.pending[key]; ok {
		return
	}
	if len(q.pending) >= versionsHealQueueCapacity {
		glog.V(1).Infof("versionsHealQueue: capacity reached, dropping %s/%s", bucket, object)
		return
	}
	q.pending[key] = &versionsHealCandidate{
		bucket:    bucket,
		object:    object,
		enqueued:  time.Now(),
		nextRetry: time.Now(),
	}
}

// popReady returns candidates whose nextRetry is in the past, removing
// them from the queue. Callers reinsert via requeue() if the heal failed.
func (q *versionsHealQueue) popReady(now time.Time) []*versionsHealCandidate {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []*versionsHealCandidate
	for k, c := range q.pending {
		if !c.nextRetry.After(now) {
			out = append(out, c)
			delete(q.pending, k)
		}
	}
	return out
}

// requeue re-inserts a candidate with an updated retry schedule. Drops
// candidates that have hit the attempt cap so they don't loop forever
// against a deterministically broken state; the read-path heal remains
// as a last-resort safety net.
func (q *versionsHealQueue) requeue(c *versionsHealCandidate, backoff time.Duration) {
	if c.attempts >= versionsHealMaxRetries {
		glog.Warningf("versionsHealQueue: giving up on %s/%s after %d attempts (read-path heal will still recover)", c.bucket, c.object, c.attempts)
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	c.nextRetry = time.Now().Add(backoff)
	q.pending[versionsHealKey(c.bucket, c.object)] = c
}

// Len returns the current queued size. Used in tests and by metrics.
func (q *versionsHealQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}

// startVersioningReconciler launches the background worker. Returns a
// stop function that the server's Shutdown calls to cancel the loop.
func (s3a *S3ApiServer) startVersioningReconciler() (stop func()) {
	if s3a.versionsHealQueue == nil {
		s3a.versionsHealQueue = newVersionsHealQueue()
	}
	ctx, cancel := context.WithCancel(context.Background())
	go s3a.runVersioningReconciler(ctx)
	return cancel
}

func (s3a *S3ApiServer) runVersioningReconciler(ctx context.Context) {
	ticker := time.NewTicker(versionsHealPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s3a.drainVersionsHealQueue()
		}
	}
}

func (s3a *S3ApiServer) drainVersionsHealQueue() {
	q := s3a.versionsHealQueue
	if q == nil {
		return
	}
	ready := q.popReady(time.Now())
	for _, c := range ready {
		c.attempts++
		if err := s3a.healVersionsPointer(c.bucket, c.object); err != nil {
			backoff := versionsHealBaseBackoff << uint(c.attempts-1)
			if backoff > versionsHealMaxBackoff {
				backoff = versionsHealMaxBackoff
			}
			glog.V(1).Infof("versionsHealQueue: heal failed for %s/%s (attempt %d): %v; retry in %v", c.bucket, c.object, c.attempts, err, backoff)
			q.requeue(c, backoff)
			continue
		}
		glog.V(1).Infof("versionsHealQueue: healed %s/%s after %d attempt(s) (queued for %v)", c.bucket, c.object, c.attempts, time.Since(c.enqueued))
	}
}

// healVersionsPointer reads the .versions directory entry for the given
// object and, if its latest-version pointer references a file that does
// not exist, invokes the same self-heal path used by the read flow. A
// nil .versions directory or an already-consistent pointer is treated as
// success — the candidate either healed itself via another path or was
// never in trouble.
func (s3a *S3ApiServer) healVersionsPointer(bucket, object string) error {
	bucketDir := s3a.bucketDir(bucket)
	versionsObjectPath := object + s3_constants.VersionsFolder
	versionsEntry, err := s3a.getEntry(bucketDir, versionsObjectPath)
	if err != nil {
		// Directory is gone — no stranded state to heal.
		return nil
	}
	if versionsEntry == nil || versionsEntry.Extended == nil {
		return nil
	}
	fileBytes, hasFile := versionsEntry.Extended[s3_constants.ExtLatestVersionFileNameKey]
	if !hasFile || len(fileBytes) == 0 {
		// No pointer => no stranded state.
		return nil
	}
	latestFile := string(fileBytes)
	if _, err := s3a.getEntry(bucketDir+"/"+versionsObjectPath, latestFile); err == nil {
		// Pointer is consistent.
		return nil
	}
	// Reuse the read-path heal so behaviour stays identical: it will
	// repair the pointer to the newest remaining version, or clear it
	// when nothing valid remains.
	_, healErr := s3a.healStaleLatestVersionPointer(bucket, object, versionsEntry, latestFile)
	return healErr
}
