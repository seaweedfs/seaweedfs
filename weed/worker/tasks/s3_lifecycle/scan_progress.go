package s3_lifecycle

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/bootstrap"
)

const (
	defaultScanProgressBatchSize = 1000
	defaultScanProgressInterval  = 30 * time.Second
)

// scanProgressReporter tracks entries processed during S3 lifecycle
// bucket scans and periodically dispatches JobProgressUpdate messages
// to the admin. Without progress updates, healthy long scans on large
// buckets can hit the admin's 24-hour no-progress stale-job guard.
type scanProgressReporter struct {
	sender      pluginworker.ExecutionSender
	jobID       string
	batchSize   int64
	minInterval time.Duration
	now         func() time.Time

	mu           sync.Mutex
	totalScanned int64
	lastReport   time.Time
}

func newScanProgressReporter(sender pluginworker.ExecutionSender, request *plugin_pb.ExecuteJobRequest, batchSize int64, minInterval time.Duration) *scanProgressReporter {
	if batchSize <= 0 {
		batchSize = defaultScanProgressBatchSize
	}
	if minInterval <= 0 {
		minInterval = defaultScanProgressInterval
	}
	var jobID string
	if request != nil && request.Job != nil {
		jobID = request.Job.JobId
	}
	nowFn := func() time.Time { return time.Now().UTC() }
	return &scanProgressReporter{
		sender:      sender,
		jobID:       jobID,
		batchSize:   batchSize,
		minInterval: minInterval,
		now:         nowFn,
		lastReport:  nowFn(),
	}
}

// wrapListFunc wraps a bootstrap.ListFunc so each scanned entry increments
// the scanned entry count and periodically emits JobProgressUpdate messages
// when batchSize entries have been scanned or minInterval has elapsed.
func (r *scanProgressReporter) wrapListFunc(inner bootstrap.ListFunc) bootstrap.ListFunc {
	if r == nil || inner == nil {
		return inner
	}
	return func(ctx context.Context, bucket, start string, cb func(*bootstrap.Entry) error) error {
		var bucketScanned int64
		err := inner(ctx, bucket, start, func(entry *bootstrap.Entry) error {
			if entry == nil {
				return nil
			}
			bucketScanned++
			r.onEntryScanned(bucket, bucketScanned)
			return cb(entry)
		})
		r.onBucketCompleted(bucket, bucketScanned)
		return err
	}
}

func (r *scanProgressReporter) onEntryScanned(bucket string, bucketScanned int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.totalScanned++
	now := r.now()
	if r.totalScanned%r.batchSize == 0 || (r.minInterval > 0 && now.Sub(r.lastReport) >= r.minInterval) {
		r.lastReport = now
		total := r.totalScanned
		msg := fmt.Sprintf("scanning bucket %s: %d entries scanned (%d total)", bucket, bucketScanned, total)
		r.sendProgressLocked(msg, "scanning")
	}
}

func (r *scanProgressReporter) onBucketCompleted(bucket string, bucketScanned int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.now()
	if bucketScanned > 0 && (bucketScanned >= r.batchSize || (r.minInterval > 0 && now.Sub(r.lastReport) >= r.minInterval)) {
		r.lastReport = now
		total := r.totalScanned
		msg := fmt.Sprintf("scanning bucket %s completed: %d entries scanned (%d total)", bucket, bucketScanned, total)
		r.sendProgressLocked(msg, "scanning")
	}
}

func (r *scanProgressReporter) sendProgressLocked(msg string, stage string) {
	if r.sender == nil {
		return
	}
	_ = r.sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId:   r.jobID,
		JobType: jobType,
		State:   plugin_pb.JobState_JOB_STATE_RUNNING,
		Stage:   stage,
		Message: msg,
	})
}

// TotalScanned returns the total number of entries scanned across all buckets.
func (r *scanProgressReporter) TotalScanned() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.totalScanned
}
