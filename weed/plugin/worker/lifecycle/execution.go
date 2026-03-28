package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
)

var errLimitReached = errors.New("limit reached")

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

	// Try to load lifecycle rules from stored XML first (full rule evaluation).
	// Fall back to filer.conf TTL-only evaluation only if no XML is configured.
	// If XML exists but is malformed, fail closed (don't fall back to TTL,
	// which could apply broader rules and delete objects the XML rules would keep).
	// Transient filer errors fall back to TTL with a warning.
	lifecycleRules, xmlErr := loadLifecycleRulesFromBucket(ctx, filerClient, bucketsPath, bucket)
	if xmlErr != nil && errors.Is(xmlErr, errMalformedLifecycleXML) {
		glog.Errorf("s3_lifecycle: bucket %s: %v (skipping bucket)", bucket, xmlErr)
		return result, xmlErr
	}
	if xmlErr != nil {
		glog.V(1).Infof("s3_lifecycle: bucket %s: transient error loading lifecycle XML: %v, falling back to TTL", bucket, xmlErr)
	}
	// lifecycleRules is non-nil when XML was present (even if empty/all disabled).
	// Only fall back to TTL when XML was truly absent (nil).
	xmlPresent := xmlErr == nil && lifecycleRules != nil
	useRuleEval := xmlPresent && len(lifecycleRules) > 0

	if !useRuleEval && !xmlPresent {
		// Fall back to filer.conf TTL rules only when no lifecycle XML exists.
		// When XML is present but has no effective rules, skip TTL fallback.
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
	}

	_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
		JobId:           jobID,
		JobType:         jobType,
		State:           plugin_pb.JobState_JOB_STATE_RUNNING,
		ProgressPercent: 10,
		Stage:           "scanning",
		Message:         fmt.Sprintf("scanning bucket %s for expired objects", bucket),
	})

	// Shared budget across all phases so we don't exceed MaxDeletesPerBucket.
	remaining := config.MaxDeletesPerBucket

	// Find expired objects using rule-based evaluation or TTL fallback.
	var expired []expiredObject
	var scanned int64
	var err error
	if useRuleEval {
		expired, scanned, err = listExpiredObjectsByRules(ctx, filerClient, bucketsPath, bucket, lifecycleRules, remaining)
	} else if !xmlPresent {
		// TTL-only scan when no lifecycle XML exists.
		expired, scanned, err = listExpiredObjects(ctx, filerClient, bucketsPath, bucket, remaining)
	}
	// When xmlPresent but no effective rules (all disabled), skip object scanning.
	result.objectsScanned = scanned
	if err != nil {
		return result, fmt.Errorf("list expired objects: %w", err)
	}

	if len(expired) > 0 {
		glog.V(1).Infof("s3_lifecycle: bucket %s: found %d expired objects out of %d scanned", bucket, len(expired), scanned)
	} else {
		glog.V(1).Infof("s3_lifecycle: bucket %s: scanned %d objects, none expired", bucket, scanned)
	}

	if config.DryRun && len(expired) > 0 {
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
	if len(expired) > 0 {
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

		remaining -= result.objectsExpired + result.errors
		if remaining < 0 {
			remaining = 0
		}
	}

	// Delete marker cleanup.
	if config.DeleteMarkerCleanup && remaining > 0 {
		_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
			JobId: jobID, JobType: jobType,
			State: plugin_pb.JobState_JOB_STATE_RUNNING,
			Stage: "cleaning_delete_markers", Message: "cleaning expired delete markers",
		})
		cleaned, cleanErrs, cleanCtxErr := cleanupDeleteMarkers(ctx, filerClient, bucketsPath, bucket, lifecycleRules, remaining)
		result.deleteMarkersClean = int64(cleaned)
		result.errors += int64(cleanErrs)
		if cleanCtxErr != nil {
			return result, cleanCtxErr
		}
		remaining -= int64(cleaned + cleanErrs)
		if remaining < 0 {
			remaining = 0
		}
	}

	// Abort incomplete multipart uploads.
	if config.AbortMPUDays > 0 && remaining > 0 {
		_ = sender.SendProgress(&plugin_pb.JobProgressUpdate{
			JobId: jobID, JobType: jobType,
			State: plugin_pb.JobState_JOB_STATE_RUNNING,
			Stage: "aborting_mpus", Message: fmt.Sprintf("aborting multipart uploads older than %d days", config.AbortMPUDays),
		})
		aborted, abortErrs, abortCtxErr := abortIncompleteMPUs(ctx, filerClient, bucketsPath, bucket, config.AbortMPUDays, remaining)
		result.mpuAborted = int64(aborted)
		result.errors += int64(abortErrs)
		if abortCtxErr != nil {
			return result, abortCtxErr
		}
	}

	return result, nil
}

// cleanupDeleteMarkers scans versioned objects and removes delete markers
// that are the sole remaining version. This matches AWS S3
// ExpiredObjectDeleteMarker semantics: a delete marker is only removed when
// it is the only version of an object (no non-current versions behind it).
//
// This phase should run AFTER NoncurrentVersionExpiration (PR 4) so that
// non-current versions have already been cleaned up, potentially leaving
// delete markers as sole versions eligible for removal.
func cleanupDeleteMarkers(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	bucketsPath, bucket string,
	rules []s3lifecycle.Rule,
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
				if dir == bucketPath && entry.Name == s3_constants.MultipartUploadsFolder {
					return nil
				}
				if strings.HasSuffix(entry.Name, s3_constants.VersionsFolder) {
					versionsDir := path.Join(dir, entry.Name)
					// Check if the latest version is a delete marker.
					latestIsMarker := string(entry.Extended[s3_constants.ExtLatestVersionIsDeleteMarker]) == "true"
					if !latestIsMarker {
						return nil
					}
					// Count versions in the directory.
					versionCount := 0
					countErr := filer_pb.SeaweedList(ctx, client, versionsDir, "", func(ve *filer_pb.Entry, _ bool) error {
						if !ve.IsDirectory {
							versionCount++
						}
						return nil
					}, "", false, 10000)
					if countErr != nil {
						glog.V(1).Infof("s3_lifecycle: failed to count versions in %s: %v", versionsDir, countErr)
						errors++
						return nil
					}
					// Only remove if the delete marker is the sole version.
					if versionCount != 1 {
						return nil
					}
					// Check that a matching ExpiredObjectDeleteMarker rule exists.
					// The rule's prefix filter must match this object's key.
					relDir := strings.TrimPrefix(versionsDir, bucketPath+"/")
					objKey := strings.TrimSuffix(relDir, s3_constants.VersionsFolder)
					if len(rules) > 0 && !matchesDeleteMarkerRule(rules, objKey) {
						return nil
					}
					// Find and remove the sole delete marker entry.
					removeErr := filer_pb.SeaweedList(ctx, client, versionsDir, "", func(ve *filer_pb.Entry, _ bool) error {
						if !ve.IsDirectory && isDeleteMarker(ve) {
							if err := filer_pb.DoRemove(ctx, client, versionsDir, ve.Name, true, false, false, false, nil); err != nil {
								glog.V(1).Infof("s3_lifecycle: failed to remove delete marker %s/%s: %v", versionsDir, ve.Name, err)
								errors++
							} else {
								cleaned++
							}
						}
						return nil
					}, "", false, 10)
					if removeErr != nil {
						glog.V(1).Infof("s3_lifecycle: failed to scan for delete marker in %s: %v", versionsDir, removeErr)
					}
					// Also remove the now-empty .versions directory.
					if cleaned > 0 {
						_ = filer_pb.DoRemove(ctx, client, dir, entry.Name, true, true, true, false, nil)
					}
					return nil
				}
				dirsToProcess = append(dirsToProcess, path.Join(dir, entry.Name))
				return nil
			}

			// For non-versioned objects: only clean up if explicitly a delete marker
			// and a matching rule exists.
			relKey := strings.TrimPrefix(path.Join(dir, entry.Name), bucketPath+"/")
			if isDeleteMarker(entry) && matchesDeleteMarkerRule(rules, relKey) {
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

// matchesDeleteMarkerRule checks if any enabled ExpiredObjectDeleteMarker rule
// matches the given object key (respecting the rule's prefix filter).
// When no lifecycle rules are provided (nil/empty), falls back to legacy
// behavior (returns true to allow cleanup).
func matchesDeleteMarkerRule(rules []s3lifecycle.Rule, objKey string) bool {
	if len(rules) == 0 {
		return true // legacy fallback
	}
	for _, r := range rules {
		if r.Status == "Enabled" && r.ExpiredObjectDeleteMarker && strings.HasPrefix(objKey, r.Prefix) {
			return true
		}
	}
	return false
}

// abortIncompleteMPUs scans the .uploads directory under a bucket and
// removes multipart upload entries older than the specified number of days.
func abortIncompleteMPUs(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	bucketsPath, bucket string,
	olderThanDays, limit int64,
) (aborted, errors int, ctxErr error) {
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
		return aborted, errors, fmt.Errorf("list uploads in %s: %w", uploadsDir, listErr)
	}

	return aborted, errors, nil
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

// listExpiredObjectsByRules scans a bucket directory tree and evaluates
// lifecycle rules against each object using the s3lifecycle evaluator.
// This function handles non-versioned objects (IsLatest=true). Versioned
// objects in .versions directories are handled by processVersionsDirectory
// (added in a separate change for NoncurrentVersionExpiration support).
func listExpiredObjectsByRules(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	bucketsPath, bucket string,
	rules []s3lifecycle.Rule,
	limit int64,
) ([]expiredObject, int64, error) {
	var expired []expiredObject
	var scanned int64

	bucketPath := path.Join(bucketsPath, bucket)
	now := time.Now()
	needTags := s3lifecycle.HasTagRules(rules)

	dirsToProcess := []string{bucketPath}
	for len(dirsToProcess) > 0 {
		select {
		case <-ctx.Done():
			return expired, scanned, ctx.Err()
		default:
		}

		dir := dirsToProcess[0]
		dirsToProcess = dirsToProcess[1:]

		limitReached := false
		err := filer_pb.SeaweedList(ctx, client, dir, "", func(entry *filer_pb.Entry, isLast bool) error {
			if entry.IsDirectory {
				if dir == bucketPath && entry.Name == s3_constants.MultipartUploadsFolder {
					return nil // skip .uploads at bucket root only
				}
				if strings.HasSuffix(entry.Name, s3_constants.VersionsFolder) {
					// Process versioned object.
					versionsDir := path.Join(dir, entry.Name)
					vExpired, vScanned, vErr := processVersionsDirectory(ctx, client, versionsDir, bucketPath, rules, now, needTags, limit-int64(len(expired)))
					if vErr != nil {
						glog.V(1).Infof("s3_lifecycle: %v", vErr)
						return vErr
					}
					expired = append(expired, vExpired...)
					scanned += vScanned
					if limit > 0 && int64(len(expired)) >= limit {
						limitReached = true
						return errLimitReached
					}
					return nil
				}
				dirsToProcess = append(dirsToProcess, path.Join(dir, entry.Name))
				return nil
			}
			scanned++

			// Skip objects already handled by TTL fast path.
			if entry.Attributes != nil && entry.Attributes.TtlSec > 0 {
				expirationUnix := entry.Attributes.Crtime + int64(entry.Attributes.TtlSec)
				if expirationUnix > nowUnix() {
					return nil // will be expired by RocksDB compaction
				}
			}

			// Build ObjectInfo for the evaluator.
			relKey := strings.TrimPrefix(path.Join(dir, entry.Name), bucketPath+"/")
			objInfo := s3lifecycle.ObjectInfo{
				Key:      relKey,
				IsLatest: true, // non-versioned objects are always "latest"
			}
			if entry.Attributes != nil {
				objInfo.Size = int64(entry.Attributes.GetFileSize())
				if entry.Attributes.Mtime > 0 {
					objInfo.ModTime = time.Unix(entry.Attributes.Mtime, 0)
				} else if entry.Attributes.Crtime > 0 {
					objInfo.ModTime = time.Unix(entry.Attributes.Crtime, 0)
				}
			}
			if needTags {
				objInfo.Tags = s3lifecycle.ExtractTags(entry.Extended)
			}

			result := s3lifecycle.Evaluate(rules, objInfo, now)
			if result.Action == s3lifecycle.ActionDeleteObject {
				expired = append(expired, expiredObject{dir: dir, name: entry.Name})
			}

			if limit > 0 && int64(len(expired)) >= limit {
				limitReached = true
				return errLimitReached
			}
			return nil
		}, "", false, 10000)

		if err != nil && !errors.Is(err, errLimitReached) {
			return expired, scanned, fmt.Errorf("list %s: %w", dir, err)
		}

		if limitReached || (limit > 0 && int64(len(expired)) >= limit) {
			break
		}
	}

	return expired, scanned, nil
}

// processVersionsDirectory evaluates NoncurrentVersionExpiration rules
// against all versions in a .versions directory.
func processVersionsDirectory(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	versionsDir, bucketPath string,
	rules []s3lifecycle.Rule,
	now time.Time,
	needTags bool,
	limit int64,
) ([]expiredObject, int64, error) {
	var expired []expiredObject
	var scanned int64

	// Check if any rule has NoncurrentVersionExpiration.
	hasNoncurrentRules := false
	for _, r := range rules {
		if r.Status == "Enabled" && r.NoncurrentVersionExpirationDays > 0 {
			hasNoncurrentRules = true
			break
		}
	}
	if !hasNoncurrentRules {
		return nil, 0, nil
	}

	// List all versions in this directory.
	var versions []*filer_pb.Entry
	listErr := filer_pb.SeaweedList(ctx, client, versionsDir, "", func(entry *filer_pb.Entry, isLast bool) error {
		if !entry.IsDirectory {
			versions = append(versions, entry)
		}
		return nil
	}, "", false, 10000)
	if listErr != nil {
		return nil, 0, fmt.Errorf("list versions in %s: %w", versionsDir, listErr)
	}
	if len(versions) <= 1 {
		return nil, 0, nil // only one version (the latest), nothing to expire
	}

	// Sort by version timestamp, newest first.
	sortVersionsByVersionId(versions)

	// Derive the object key from the .versions directory path.
	// e.g., /buckets/mybucket/path/to/key.versions -> path/to/key
	relDir := strings.TrimPrefix(versionsDir, bucketPath+"/")
	objKey := strings.TrimSuffix(relDir, s3_constants.VersionsFolder)

	// Walk versions: first is latest, rest are non-current.
	noncurrentIndex := 0
	for i := 1; i < len(versions); i++ {
		entry := versions[i]
		scanned++

		// Skip delete markers from expiration evaluation, but count
		// them toward NewerNoncurrentVersions so data versions get
		// the correct noncurrent index.
		if isDeleteMarker(entry) {
			noncurrentIndex++
			continue
		}

		// Determine successor's timestamp (the version that replaced this one).
		successorEntry := versions[i-1]
		successorVersionId := strings.TrimPrefix(successorEntry.Name, "v_")
		successorTime := s3lifecycle.GetVersionTimestamp(successorVersionId)
		if successorTime.IsZero() && successorEntry.Attributes != nil && successorEntry.Attributes.Mtime > 0 {
			successorTime = time.Unix(successorEntry.Attributes.Mtime, 0)
		}

		objInfo := s3lifecycle.ObjectInfo{
			Key:              objKey,
			IsLatest:         false,
			SuccessorModTime: successorTime,
			NumVersions:      len(versions),
			NoncurrentIndex:  noncurrentIndex,
		}
		if entry.Attributes != nil {
			objInfo.Size = int64(entry.Attributes.GetFileSize())
			if entry.Attributes.Mtime > 0 {
				objInfo.ModTime = time.Unix(entry.Attributes.Mtime, 0)
			}
		}
		if needTags {
			objInfo.Tags = s3lifecycle.ExtractTags(entry.Extended)
		}

		// Evaluate using the detailed ShouldExpireNoncurrentVersion which
		// handles NewerNoncurrentVersions.
		for _, rule := range rules {
			if s3lifecycle.ShouldExpireNoncurrentVersion(rule, objInfo, noncurrentIndex, now) {
				expired = append(expired, expiredObject{dir: versionsDir, name: entry.Name})
				break
			}
		}

		noncurrentIndex++

		if limit > 0 && int64(len(expired)) >= limit {
			break
		}
	}

	return expired, scanned, nil
}

// sortVersionsByVersionId sorts version entries newest-first using full
// version ID comparison (matching compareVersionIds in s3api_version_id.go).
// This uses the complete version ID string, not just the decoded timestamp,
// so entries with the same timestamp prefix are correctly ordered by their
// random suffix.
func sortVersionsByVersionId(versions []*filer_pb.Entry) {
	sort.Slice(versions, func(i, j int) bool {
		vidI := strings.TrimPrefix(versions[i].Name, "v_")
		vidJ := strings.TrimPrefix(versions[j].Name, "v_")
		return s3lifecycle.CompareVersionIds(vidI, vidJ) < 0
	})
}
