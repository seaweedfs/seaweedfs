package lifecycle

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
)

// detectBucketsWithLifecycleRules scans all S3 buckets to find those
// with lifecycle (TTL) rules configured in filer.conf.
func (h *Handler) detectBucketsWithLifecycleRules(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	config Config,
	bucketFilter string,
	maxResults int,
) ([]*plugin_pb.JobProposal, error) {
	// Load filer configuration to find TTL rules.
	fc, err := loadFilerConf(ctx, filerClient)
	if err != nil {
		return nil, fmt.Errorf("load filer conf: %w", err)
	}

	bucketsPath := defaultBucketsPath
	bucketMatchers := wildcard.CompileWildcardMatchers(bucketFilter)

	// List all buckets.
	bucketEntries, err := listFilerEntries(ctx, filerClient, bucketsPath, "")
	if err != nil {
		return nil, fmt.Errorf("list buckets at %s: %w", bucketsPath, err)
	}

	var proposals []*plugin_pb.JobProposal
	for _, entry := range bucketEntries {
		select {
		case <-ctx.Done():
			return proposals, ctx.Err()
		default:
		}

		if !entry.IsDirectory {
			continue
		}
		bucketName := entry.Name
		if !wildcard.MatchesAnyWildcard(bucketMatchers, bucketName) {
			continue
		}

		// Derive the collection name for this bucket.
		collection := bucketName
		ttls := fc.GetCollectionTtls(collection)
		if len(ttls) == 0 {
			continue
		}

		glog.V(2).Infof("s3_lifecycle: bucket %s has %d lifecycle rule(s)", bucketName, len(ttls))

		proposal := &plugin_pb.JobProposal{
			ProposalId: fmt.Sprintf("s3_lifecycle:%s", bucketName),
			JobType:    jobType,
			Summary:    fmt.Sprintf("Lifecycle management for bucket %s (%d rules)", bucketName, len(ttls)),
			DedupeKey:  fmt.Sprintf("s3_lifecycle:%s", bucketName),
			Parameters: map[string]*plugin_pb.ConfigValue{
				"bucket":       {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: bucketName}},
				"buckets_path": {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: bucketsPath}},
				"collection":   {Kind: &plugin_pb.ConfigValue_StringValue{StringValue: collection}},
				"rule_count":   {Kind: &plugin_pb.ConfigValue_Int64Value{Int64Value: int64(len(ttls))}},
			},
			Labels: map[string]string{
				"bucket": bucketName,
			},
		}

		proposals = append(proposals, proposal)
		if maxResults > 0 && len(proposals) >= maxResults {
			break
		}
	}

	return proposals, nil
}

const defaultBucketsPath = "/buckets"

// loadFilerConf reads the filer configuration from the filer.
func loadFilerConf(ctx context.Context, client filer_pb.SeaweedFilerClient) (*filer.FilerConf, error) {
	fc := filer.NewFilerConf()

	content, err := filer.ReadInsideFiler(ctx, client, filer.DirectoryEtcSeaweedFS, filer.FilerConfName)
	if err != nil {
		// filer.conf may not exist yet - return empty config.
		glog.V(1).Infof("s3_lifecycle: filer.conf not found or unreadable: %v (using empty config)", err)
		return fc, nil
	}
	if err := fc.LoadFromBytes(content); err != nil {
		return nil, fmt.Errorf("parse filer.conf: %w", err)
	}

	return fc, nil
}

// listFilerEntries lists directory entries from the filer.
func listFilerEntries(ctx context.Context, client filer_pb.SeaweedFilerClient, dir, startFrom string) ([]*filer_pb.Entry, error) {
	var entries []*filer_pb.Entry
	err := filer_pb.SeaweedList(ctx, client, dir, "", func(entry *filer_pb.Entry, isLast bool) error {
		entries = append(entries, entry)
		return nil
	}, startFrom, false, 10000)
	return entries, err
}

type expiredObject struct {
	dir  string
	name string
}

// listExpiredObjects scans a bucket directory tree for objects whose TTL
// has expired based on their TtlSec attribute set by PutBucketLifecycle.
func listExpiredObjects(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	bucketsPath, bucket string,
	limit int64,
) ([]expiredObject, int64, error) {
	var expired []expiredObject
	var scanned int64

	bucketPath := path.Join(bucketsPath, bucket)

	// Walk the bucket directory tree using breadth-first traversal.
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
				dirsToProcess = append(dirsToProcess, path.Join(dir, entry.Name))
				return nil
			}
			scanned++

			if isExpiredByTTL(entry) {
				expired = append(expired, expiredObject{
					dir:  dir,
					name: entry.Name,
				})
			}

			if limit > 0 && int64(len(expired)) >= limit {
				limitReached = true
				return fmt.Errorf("limit reached")
			}
			return nil
		}, "", false, 10000)

		if err != nil && !strings.Contains(err.Error(), "limit reached") {
			glog.V(1).Infof("s3_lifecycle: error listing %s: %v", dir, err)
		}

		if limitReached || (limit > 0 && int64(len(expired)) >= limit) {
			break
		}
	}

	return expired, scanned, nil
}

// isExpiredByTTL checks if an entry is expired based on its TTL attribute.
// SeaweedFS sets TtlSec on entries when lifecycle rules are applied via
// PutBucketLifecycleConfiguration. An entry is expired when
// creation_time + TTL < now.
func isExpiredByTTL(entry *filer_pb.Entry) bool {
	if entry == nil || entry.Attributes == nil {
		return false
	}

	ttlSec := entry.Attributes.TtlSec
	if ttlSec <= 0 {
		return false
	}

	crTime := entry.Attributes.Crtime
	if crTime <= 0 {
		return false
	}

	expirationUnix := crTime + int64(ttlSec)
	return expirationUnix < nowUnix()
}
