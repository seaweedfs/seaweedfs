package lifecycle

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
)

type executionResult struct {
	objectsExpired     int64
	objectsScanned     int64
	deleteMarkersClean int64
	errors             int64
}

// executeLifecycleForBucket processes lifecycle rules for a single bucket:
// 1. Reads filer.conf to get TTL rules for the bucket's collection
// 2. Walks the bucket directory tree to find expired objects
// 3. Deletes expired objects (unless dry run)
func (h *Handler) executeLifecycleForBucket(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	config Config,
	bucket, bucketsPath string,
	sender pluginworker.ExecutionSender,
	jobID string,
) (*executionResult, error) {
	result := &executionResult{}

	// Load filer.conf to verify TTL rules still exist.
	fc, err := loadFilerConf(ctx, filerClient)
	if err != nil {
		return result, fmt.Errorf("load filer conf: %w", err)
	}

	collection := bucket
	ttlRules := fc.GetCollectionTtls(collection)
	if len(ttlRules) == 0 {
		glog.V(1).Infof("s3_lifecycle: bucket %s has no lifecycle rules, skipping", bucket)
		return result, nil
	}

	_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId:           jobID,
		JobType:         jobType,
		State:           plugin_pb.JobState_JOB_STATE_RUNNING,
		ProgressPercent: 10,
		Stage:           "scanning",
		Message:         fmt.Sprintf("scanning bucket %s for expired objects (%d rules)", bucket, len(ttlRules)),
	})

	// Find expired objects.
	expired, scanned, err := listExpiredObjects(ctx, filerClient, bucketsPath, bucket, config.MaxDeletesPerBucket)
	result.objectsScanned = scanned
	if err != nil {
		return result, fmt.Errorf("list expired objects: %w", err)
	}

	if len(expired) == 0 {
		glog.V(1).Infof("s3_lifecycle: bucket %s: scanned %d objects, none expired", bucket, scanned)
		return result, nil
	}

	glog.V(1).Infof("s3_lifecycle: bucket %s: found %d expired objects out of %d scanned", bucket, len(expired), scanned)

	if config.DryRun {
		result.objectsExpired = int64(len(expired))
		_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
			JobId:           jobID,
			JobType:         jobType,
			State:           plugin_pb.JobState_JOB_STATE_RUNNING,
			ProgressPercent: 100,
			Stage:           "dry_run",
			Message:         fmt.Sprintf("dry run: would delete %d expired objects", len(expired)),
		})
		return result, nil
	}

	// Delete expired objects in batches.
	_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId:           jobID,
		JobType:         jobType,
		State:           plugin_pb.JobState_JOB_STATE_RUNNING,
		ProgressPercent: 50,
		Stage:           "deleting",
		Message:         fmt.Sprintf("deleting %d expired objects", len(expired)),
	})

	var batchSize int
	if config.BatchSize <= 0 {
		batchSize = defaultBatchSize
	} else if config.BatchSize > math.MaxInt32 {
		batchSize = math.MaxInt32
	} else {
		batchSize = int(config.BatchSize)
	}

	for i := 0; i < len(expired); i += batchSize {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}

		end := i + batchSize
		if end > len(expired) {
			end = len(expired)
		}
		batch := expired[i:end]

		deleted, errs := deleteExpiredObjects(ctx, filerClient, batch)
		result.objectsExpired += int64(deleted)
		result.errors += int64(errs)

		progress := float64(end)/float64(len(expired))*50 + 50 // 50-100%
		_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
			JobId:           jobID,
			JobType:         jobType,
			State:           plugin_pb.JobState_JOB_STATE_RUNNING,
			ProgressPercent: progress,
			Stage:           "deleting",
			Message:         fmt.Sprintf("deleted %d/%d expired objects", result.objectsExpired, len(expired)),
		})
	}

	return result, nil
}

// deleteExpiredObjects deletes a batch of expired objects from the filer.
func deleteExpiredObjects(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	objects []expiredObject,
) (deleted, errors int) {
	for _, obj := range objects {
		select {
		case <-ctx.Done():
			return deleted, errors
		default:
		}

		err := filer_pb.DoRemove(ctx, client, obj.dir, obj.name, true, false, false, false, nil)
		if err != nil {
			glog.V(1).Infof("s3_lifecycle: failed to delete %s/%s: %v", obj.dir, obj.name, err)
			errors++
			continue
		}
		deleted++
	}
	return deleted, errors
}

// nowUnix returns the current time as a Unix timestamp.
func nowUnix() int64 {
	return time.Now().Unix()
}
