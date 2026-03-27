package lifecycle

import (
	"context"
	"fmt"
	"math"
	"path"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

type executionResult struct {
	objectsExpired     int64
	objectsScanned     int64
	deleteMarkersClean int64
	mpuAborted         int64
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
	} else if config.BatchSize > math.MaxInt {
		batchSize = math.MaxInt
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

		deleted, errs, batchErr := deleteExpiredObjects(ctx, filerClient, batch)
		result.objectsExpired += int64(deleted)
		result.errors += int64(errs)

		if batchErr != nil {
			return result, batchErr
		}

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

	// Delete marker cleanup.
	if config.DeleteMarkerCleanup {
		_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
			JobId: jobID, JobType: jobType,
			State: plugin_pb.JobState_JOB_STATE_RUNNING,
			Stage: "cleaning_delete_markers", Message: "cleaning expired delete markers",
		})
		cleaned, cleanErrs, cleanCtxErr := cleanupDeleteMarkers(ctx, filerClient, bucketsPath, bucket, config.MaxDeletesPerBucket)
		result.deleteMarkersClean = int64(cleaned)
		result.errors += int64(cleanErrs)
		if cleanCtxErr != nil {
			return result, cleanCtxErr
		}
	}

	// Abort incomplete multipart uploads.
	if config.AbortMPUDays > 0 {
		_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
			JobId: jobID, JobType: jobType,
			State: plugin_pb.JobState_JOB_STATE_RUNNING,
			Stage: "aborting_mpus", Message: fmt.Sprintf("aborting multipart uploads older than %d days", config.AbortMPUDays),
		})
		aborted, abortErrs := abortIncompleteMPUs(ctx, filerClient, bucketsPath, bucket, config.AbortMPUDays, config.MaxDeletesPerBucket)
		result.mpuAborted = int64(aborted)
		result.errors += int64(abortErrs)
	}

	return result, nil
}

// cleanupDeleteMarkers scans the bucket for entries marked as delete markers
// (via the S3 versioning extended attribute) and removes them.
func cleanupDeleteMarkers(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	bucketsPath, bucket string,
	limit int64,
) (cleaned, errors int, ctxErr error) {
	bucketPath := path.Join(bucketsPath, bucket)

	dirsToProcess := []string{bucketPath}
	for len(dirsToProcess) > 0 {
		if ctx.Err() != nil {
			return cleaned, errors, ctx.Err()
		}

		dir := dirsToProcess[0]
		dirsToProcess = dirsToProcess[1:]

		listErr := filer_pb.SeaweedList(ctx, client, dir, "", func(entry *filer_pb.Entry, isLast bool) error {
			if entry.IsDirectory {
				// Skip .uploads directories.
				if entry.Name != ".uploads" {
					dirsToProcess = append(dirsToProcess, path.Join(dir, entry.Name))
				}
				return nil
			}

			if isDeleteMarker(entry) {
				if err := filer_pb.DoRemove(ctx, client, dir, entry.Name, true, false, false, false, nil); err != nil {
					glog.V(1).Infof("s3_lifecycle: failed to remove delete marker %s/%s: %v", dir, entry.Name, err)
					errors++
				} else {
					cleaned++
				}
			}

			if limit > 0 && int64(cleaned+errors) >= limit {
				return fmt.Errorf("limit reached")
			}
			return nil
		}, "", false, 10000)

		if listErr != nil && !strings.Contains(listErr.Error(), "limit reached") {
			return cleaned, errors, fmt.Errorf("list %s: %w", dir, listErr)
		}

		if limit > 0 && int64(cleaned+errors) >= limit {
			break
		}
	}
	return cleaned, errors, nil
}

// isDeleteMarker checks if an entry is an S3 delete marker.
func isDeleteMarker(entry *filer_pb.Entry) bool {
	if entry == nil || entry.Extended == nil {
		return false
	}
	return string(entry.Extended[s3_constants.ExtDeleteMarkerKey]) == "true"
}

// abortIncompleteMPUs scans the .uploads directory under a bucket and
// removes multipart upload entries older than the specified number of days.
func abortIncompleteMPUs(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	bucketsPath, bucket string,
	olderThanDays, limit int64,
) (aborted, errors int) {
	uploadsDir := path.Join(bucketsPath, bucket, ".uploads")
	cutoff := time.Now().Add(-time.Duration(olderThanDays) * 24 * time.Hour)

	listErr := filer_pb.SeaweedList(ctx, client, uploadsDir, "", func(entry *filer_pb.Entry, isLast bool) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if !entry.IsDirectory {
			return nil
		}

		// Each subdirectory under .uploads is one multipart upload.
		// Check the directory creation time.
		if entry.Attributes != nil && entry.Attributes.Crtime > 0 {
			created := time.Unix(entry.Attributes.Crtime, 0)
			if created.Before(cutoff) {
				uploadPath := path.Join(uploadsDir, entry.Name)
				if err := filer_pb.DoRemove(ctx, client, uploadsDir, entry.Name, true, true, true, false, nil); err != nil {
					glog.V(1).Infof("s3_lifecycle: failed to abort MPU %s: %v", uploadPath, err)
					errors++
				} else {
					aborted++
				}
			}
		}

		if limit > 0 && int64(aborted+errors) >= limit {
			return fmt.Errorf("limit reached")
		}
		return nil
	}, "", false, 10000)

	if listErr != nil && !strings.Contains(listErr.Error(), "limit reached") {
		glog.Errorf("s3_lifecycle: failed to list uploads in %s: %v", uploadsDir, listErr)
	}

	return aborted, errors
}

// deleteExpiredObjects deletes a batch of expired objects from the filer.
// Returns a non-nil error when the context is canceled mid-batch.
func deleteExpiredObjects(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	objects []expiredObject,
) (deleted, errors int, ctxErr error) {
	for _, obj := range objects {
		if ctx.Err() != nil {
			return deleted, errors, ctx.Err()
		}

		err := filer_pb.DoRemove(ctx, client, obj.dir, obj.name, true, false, false, false, nil)
		if err != nil {
			glog.V(1).Infof("s3_lifecycle: failed to delete %s/%s: %v", obj.dir, obj.name, err)
			errors++
			continue
		}
		deleted++
	}
	return deleted, errors, nil
}

// nowUnix returns the current time as a Unix timestamp.
func nowUnix() int64 {
	return time.Now().Unix()
}
