package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type planningIndex struct {
	SnapshotID        int64                          `json:"snapshotId"`
	ManifestList      string                         `json:"manifestList,omitempty"`
	UpdatedAtMs       int64                          `json:"updatedAtMs"`
	DataManifestCount int64                          `json:"dataManifestCount,omitempty"`
	Compaction        *planningIndexCompaction       `json:"compaction,omitempty"`
	RewriteManifests  *planningIndexRewriteManifests `json:"rewriteManifests,omitempty"`
}

type planningIndexCompaction struct {
	ConfigHash string `json:"configHash"`
	Eligible   bool   `json:"eligible"`
}

type planningIndexRewriteManifests struct {
	Threshold int64 `json:"threshold"`
	Eligible  bool  `json:"eligible"`
}

type tableMetadataEnvelope struct {
	MetadataVersion  int    `json:"metadataVersion"`
	MetadataLocation string `json:"metadataLocation,omitempty"`
	Metadata         *struct {
		FullMetadata json.RawMessage `json:"fullMetadata,omitempty"`
	} `json:"metadata,omitempty"`
	PlanningIndex json.RawMessage `json:"planningIndex,omitempty"`
}

func parseTableMetadataEnvelope(metadataBytes []byte) (table.Metadata, string, *planningIndex, error) {
	var envelope tableMetadataEnvelope
	if err := json.Unmarshal(metadataBytes, &envelope); err != nil {
		return nil, "", nil, fmt.Errorf("parse metadata xattr: %w", err)
	}
	if envelope.Metadata == nil || len(envelope.Metadata.FullMetadata) == 0 {
		return nil, "", nil, fmt.Errorf("no fullMetadata in table xattr")
	}

	meta, err := table.ParseMetadataBytes(envelope.Metadata.FullMetadata)
	if err != nil {
		return nil, "", nil, fmt.Errorf("parse iceberg metadata: %w", err)
	}

	var index *planningIndex
	if len(envelope.PlanningIndex) > 0 {
		if err := json.Unmarshal(envelope.PlanningIndex, &index); err != nil {
			index = nil
		}
	}

	metadataFileName := metadataFileNameFromLocation(envelope.MetadataLocation, "", "")
	if metadataFileName == "" {
		metadataFileName = fmt.Sprintf("v%d.metadata.json", envelope.MetadataVersion)
	}
	return meta, metadataFileName, index, nil
}

func (idx *planningIndex) matchesSnapshot(meta table.Metadata) bool {
	if idx == nil {
		return false
	}
	currentSnap := meta.CurrentSnapshot()
	if currentSnap == nil || currentSnap.ManifestList == "" {
		return false
	}
	return idx.SnapshotID == currentSnap.SnapshotID && idx.ManifestList == currentSnap.ManifestList
}

func (idx *planningIndex) compactionEligible(config Config) (bool, bool) {
	if idx == nil || idx.Compaction == nil {
		return false, false
	}
	if idx.Compaction.ConfigHash != compactionPlanningConfigHash(config) {
		return false, false
	}
	return idx.Compaction.Eligible, true
}

func (idx *planningIndex) rewriteManifestsEligible(config Config) (bool, bool) {
	if idx == nil || idx.RewriteManifests == nil {
		return false, false
	}
	if idx.RewriteManifests.Threshold != config.MinManifestsToRewrite {
		return false, false
	}
	return idx.RewriteManifests.Eligible, true
}

func compactionPlanningConfigHash(config Config) string {
	return fmt.Sprintf("target=%d|min=%d", config.TargetFileSizeBytes, config.MinInputFiles)
}

func operationRequested(ops []string, wanted string) bool {
	for _, op := range ops {
		if op == wanted {
			return true
		}
	}
	return false
}

func buildPlanningIndex(
	ctx context.Context,
	filerClient filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
	meta table.Metadata,
	config Config,
	ops []string,
) (*planningIndex, error) {
	currentSnap := meta.CurrentSnapshot()
	if currentSnap == nil || currentSnap.ManifestList == "" {
		return nil, nil
	}

	manifests, err := loadCurrentManifests(ctx, filerClient, bucketName, tablePath, meta)
	if err != nil {
		return nil, err
	}

	index := &planningIndex{
		SnapshotID:        currentSnap.SnapshotID,
		ManifestList:      currentSnap.ManifestList,
		UpdatedAtMs:       time.Now().UnixMilli(),
		DataManifestCount: countDataManifests(manifests),
	}

	if operationRequested(ops, "compact") {
		eligible, err := hasEligibleCompaction(ctx, filerClient, bucketName, tablePath, manifests, config)
		if err != nil {
			return nil, err
		}
		index.Compaction = &planningIndexCompaction{
			ConfigHash: compactionPlanningConfigHash(config),
			Eligible:   eligible,
		}
	}

	if operationRequested(ops, "rewrite_manifests") {
		index.RewriteManifests = &planningIndexRewriteManifests{
			Threshold: config.MinManifestsToRewrite,
			Eligible:  index.DataManifestCount >= config.MinManifestsToRewrite,
		}
	}

	return index, nil
}

func persistPlanningIndex(
	ctx context.Context,
	client filer_pb.SeaweedFilerClient,
	bucketName, tablePath string,
	index *planningIndex,
) error {
	if index == nil {
		return nil
	}

	tableDir := path.Join(s3tables.TablesPath, bucketName, tablePath)
	tableName := path.Base(tableDir)
	parentDir := path.Dir(tableDir)

	resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
		Directory: parentDir,
		Name:      tableName,
	})
	if err != nil {
		return fmt.Errorf("lookup table entry: %w", err)
	}
	if resp == nil || resp.Entry == nil {
		return fmt.Errorf("table entry not found")
	}

	existingXattr, ok := resp.Entry.Extended[s3tables.ExtendedKeyMetadata]
	if !ok || len(existingXattr) == 0 {
		return fmt.Errorf("no metadata xattr on table entry")
	}

	var internalMeta map[string]json.RawMessage
	if err := json.Unmarshal(existingXattr, &internalMeta); err != nil {
		return fmt.Errorf("unmarshal metadata xattr: %w", err)
	}

	indexJSON, err := json.Marshal(index)
	if err != nil {
		return fmt.Errorf("marshal planning index: %w", err)
	}
	internalMeta["planningIndex"] = indexJSON

	updatedXattr, err := json.Marshal(internalMeta)
	if err != nil {
		return fmt.Errorf("marshal updated metadata xattr: %w", err)
	}

	expectedVersionXattr := resp.Entry.Extended[s3tables.ExtendedKeyMetadataVersion]
	resp.Entry.Extended[s3tables.ExtendedKeyMetadata] = updatedXattr
	_, err = client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
		Directory: parentDir,
		Entry:     resp.Entry,
		ExpectedExtended: map[string][]byte{
			s3tables.ExtendedKeyMetadataVersion: expectedVersionXattr,
		},
	})
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			return nil
		}
		return fmt.Errorf("update table entry: %w", err)
	}

	return nil
}
