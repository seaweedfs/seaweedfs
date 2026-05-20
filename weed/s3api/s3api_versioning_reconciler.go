package s3api

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// versioningHealLogPrefix is the grep-able tag every line in the .versions/
// pointer recovery lifecycle carries. Operators correlating produced /
// surfaced / drained / gave-up events can pull a single stream across
// hosts with `grep '\[versioning-heal\]'`. Keep this constant — log
// aggregators (Loki, Splunk, journalctl filters) may match on it.
const versioningHealLogPrefix = "[versioning-heal]"

// versioningHealInfof / Warningf / Errorf emit a log line stamped with
// the grep prefix and an event tag. Use event names from a small fixed
// vocabulary so dashboards can aggregate by event:
//
//	event=produced  — stranded state was created (or would have been before this PR's fixes)
//	event=surfaced  — read-path heal triggered for an already-stranded key
//	event=healed    — pointer was successfully repaired or cleared
//	event=enqueue   — reconciler queued a candidate
//	event=drain     — reconciler successfully drained a candidate
//	event=retry     — reconciler attempt failed, will retry
//	event=gave_up   — reconciler exhausted retries on a candidate
//	event=anomaly   — listing-after-rm inconsistency detected
//	event=summary   — periodic heartbeat (counters + queue depth)
//
// All string/error arguments are sanitized so user-controlled bucket
// names, object keys, filenames, and error text can't split the line
// into multiple grep tokens or inject fake event=/bucket=/key= fields.
// Safe values pass through unchanged; anything containing whitespace,
// quotes, control chars, or backslashes is replaced with its
// strconv.Quote form so the field boundary stays one space.
func versioningHealInfof(event, format string, args ...interface{}) {
	glog.Infof("%s event=%s %s", versioningHealLogPrefix, event, fmt.Sprintf(format, sanitizeHealArgs(args)...))
}

func versioningHealWarningf(event, format string, args ...interface{}) {
	glog.Warningf("%s event=%s %s", versioningHealLogPrefix, event, fmt.Sprintf(format, sanitizeHealArgs(args)...))
}

func versioningHealErrorf(event, format string, args ...interface{}) {
	glog.Errorf("%s event=%s %s", versioningHealLogPrefix, event, fmt.Sprintf(format, sanitizeHealArgs(args)...))
}

// sanitizeHealArgs walks args and replaces string/error values that
// would break the single-space field separator (whitespace, newlines,
// control chars, quotes, backslashes) with their strconv.Quote form.
// Non-string args are returned untouched.
func sanitizeHealArgs(args []interface{}) []interface{} {
	out := make([]interface{}, len(args))
	for i, v := range args {
		out[i] = sanitizeHealArg(v)
	}
	return out
}

func sanitizeHealArg(v interface{}) interface{} {
	var s string
	switch t := v.(type) {
	case nil:
		return v
	case string:
		s = t
	case error:
		s = t.Error()
	default:
		return v
	}
	if !needsHealQuote(s) {
		return s
	}
	return strconv.Quote(s)
}

func needsHealQuote(s string) bool {
	for _, r := range s {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '"' || r == '\\' || r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}

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
		versioningHealWarningf("queue_full", "bucket=%s key=%s capacity=%d (candidate dropped; read-path heal still covers it)", bucket, object, versionsHealQueueCapacity)
		return
	}
	q.pending[key] = &versionsHealCandidate{
		bucket:    bucket,
		object:    object,
		enqueued:  time.Now(),
		nextRetry: time.Now(),
	}
	versioningHealInfof("enqueue", "bucket=%s key=%s queue_depth=%d", bucket, object, len(q.pending))
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
		versioningHealWarningf("gave_up", "bucket=%s key=%s attempts=%d (read-path heal will still recover)", c.bucket, c.object, c.attempts)
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
			versioningHealInfof("retry", "bucket=%s key=%s attempt=%d retry_in=%v err=%v", c.bucket, c.object, c.attempts, backoff, err)
			q.requeue(c, backoff)
			continue
		}
		versioningHealInfof("drain", "bucket=%s key=%s attempts=%d queued_for=%v", c.bucket, c.object, c.attempts, time.Since(c.enqueued))
	}
}

// healVersionsPointer reads the .versions directory entry for the given
// object and, if its latest-version pointer references a file that does
// not exist, invokes the same self-heal path used by the read flow. A
// genuinely-missing .versions directory or an already-consistent pointer
// is treated as success — the candidate either healed itself via another
// path or was never in trouble.
//
// Transient errors from the filer (timeout, brief unreachability) are
// returned so the reconciler retries with backoff rather than silently
// evicting the candidate. Swallowing them as "no stranded state" would
// defeat the reconciler for exactly the failure modes the PR targets.
func (s3a *S3ApiServer) healVersionsPointer(bucket, object string) error {
	bucketDir := s3a.bucketDir(bucket)
	versionsObjectPath := object + s3_constants.VersionsFolder
	versionsEntry, err := s3a.getEntry(bucketDir, versionsObjectPath)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) || status.Code(err) == codes.NotFound {
			// Directory is gone — no stranded state to heal.
			return nil
		}
		return fmt.Errorf("read .versions entry %s/%s: %w", bucket, object, err)
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
	if _, probeErr := s3a.getEntry(bucketDir+"/"+versionsObjectPath, latestFile); probeErr == nil {
		// Pointer is consistent.
		return nil
	} else if !errors.Is(probeErr, filer_pb.ErrNotFound) && status.Code(probeErr) != codes.NotFound {
		// Transient — surface so the reconciler retries with backoff
		// rather than invoking heal on a possibly-incomplete listing
		// (which could rewrite the pointer to an older version).
		return fmt.Errorf("probe latest version %s/%s/%s: %w", bucket, object, latestFile, probeErr)
	}
	// Reuse the read-path heal so behaviour stays identical: it will
	// repair the pointer to the newest remaining version, or clear it
	// when nothing valid remains.
	_, healErr := s3a.healStaleLatestVersionPointer(bucket, object, versionsEntry, latestFile)
	return healErr
}
