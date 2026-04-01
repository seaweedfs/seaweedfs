package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

const requirementAssertCreate = "assert-create"

type icebergRequestError struct {
	status  int
	errType string
	message string
}

func (e *icebergRequestError) Error() string {
	return e.message
}

type createOnCommitInput struct {
	bucketARN         string
	markerBucket      string
	namespace         []string
	tableName         string
	identityName      string
	location          string
	tableUUID         uuid.UUID
	baseMetadata      table.Metadata
	baseMetadataLoc   string
	baseMetadataVer   int
	updates           table.Updates
	statisticsUpdates []statisticsUpdate
}

func isS3TablesConflict(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, s3tables.ErrVersionTokenMismatch) {
		return true
	}
	var tableErr *s3tables.S3TablesError
	return errors.As(err, &tableErr) && tableErr.Type == s3tables.ErrCodeConflict
}

func isS3TablesNotFound(err error) bool {
	if err == nil {
		return false
	}
	if strings.Contains(strings.ToLower(err.Error()), "not found") {
		return true
	}
	var tableErr *s3tables.S3TablesError
	return errors.As(err, &tableErr) &&
		(tableErr.Type == s3tables.ErrCodeNoSuchTable || tableErr.Type == s3tables.ErrCodeNoSuchNamespace || strings.Contains(strings.ToLower(tableErr.Message), "not found"))
}

func hasAssertCreateRequirement(requirements table.Requirements) bool {
	for _, requirement := range requirements {
		if requirement.GetType() == requirementAssertCreate {
			return true
		}
	}
	return false
}

func isS3TablesAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	if strings.Contains(strings.ToLower(err.Error()), "already exists") {
		return true
	}
	var tableErr *s3tables.S3TablesError
	return errors.As(err, &tableErr) &&
		(tableErr.Type == s3tables.ErrCodeTableAlreadyExists || tableErr.Type == s3tables.ErrCodeNamespaceAlreadyExists || strings.Contains(strings.ToLower(tableErr.Message), "already exists"))
}

func parseMetadataVersionFromLocation(metadataLocation string) int {
	base := path.Base(metadataLocation)
	if !strings.HasPrefix(base, "v") || !strings.HasSuffix(base, ".metadata.json") {
		return 0
	}
	rawVersion := strings.TrimPrefix(strings.TrimSuffix(base, ".metadata.json"), "v")
	version, err := strconv.Atoi(rawVersion)
	if err != nil || version <= 0 {
		return 0
	}
	return version
}

func (s *Server) finalizeCreateOnCommit(ctx context.Context, input createOnCommitInput) (*CommitTableResponse, *icebergRequestError) {
	builder, err := table.MetadataBuilderFromBase(input.baseMetadata, input.baseMetadataLoc)
	if err != nil {
		return nil, &icebergRequestError{
			status:  http.StatusInternalServerError,
			errType: "InternalServerError",
			message: "Failed to create metadata builder: " + err.Error(),
		}
	}
	for _, update := range input.updates {
		if err := update.Apply(builder); err != nil {
			return nil, &icebergRequestError{
				status:  http.StatusBadRequest,
				errType: "BadRequestException",
				message: "Failed to apply update: " + err.Error(),
			}
		}
	}

	newMetadata, err := builder.Build()
	if err != nil {
		return nil, &icebergRequestError{
			status:  http.StatusBadRequest,
			errType: "BadRequestException",
			message: "Failed to build new metadata: " + err.Error(),
		}
	}

	metadataVersion := input.baseMetadataVer + 1
	if metadataVersion <= 0 {
		metadataVersion = 1
	}
	metadataFileName := fmt.Sprintf("v%d.metadata.json", metadataVersion)
	newMetadataLocation := fmt.Sprintf("%s/metadata/%s", strings.TrimSuffix(input.location, "/"), metadataFileName)

	metadataBytes, err := json.Marshal(newMetadata)
	if err != nil {
		return nil, &icebergRequestError{
			status:  http.StatusInternalServerError,
			errType: "InternalServerError",
			message: "Failed to serialize metadata: " + err.Error(),
		}
	}
	metadataBytes, err = applyStatisticsUpdates(metadataBytes, input.statisticsUpdates)
	if err != nil {
		return nil, &icebergRequestError{
			status:  http.StatusBadRequest,
			errType: "BadRequestException",
			message: "Failed to apply statistics updates: " + err.Error(),
		}
	}
	newMetadata, err = table.ParseMetadataBytes(metadataBytes)
	if err != nil {
		return nil, &icebergRequestError{
			status:  http.StatusInternalServerError,
			errType: "InternalServerError",
			message: "Failed to parse committed metadata: " + err.Error(),
		}
	}

	metadataBucket, metadataPath, err := parseS3Location(input.location)
	if err != nil {
		return nil, &icebergRequestError{
			status:  http.StatusInternalServerError,
			errType: "InternalServerError",
			message: "Invalid table location: " + err.Error(),
		}
	}
	if err := s.saveMetadataFile(ctx, metadataBucket, metadataPath, metadataFileName, metadataBytes); err != nil {
		return nil, &icebergRequestError{
			status:  http.StatusInternalServerError,
			errType: "InternalServerError",
			message: "Failed to save metadata file: " + err.Error(),
		}
	}

	createReq := &s3tables.CreateTableRequest{
		TableBucketARN: input.bucketARN,
		Namespace:      input.namespace,
		Name:           input.tableName,
		Format:         "ICEBERG",
		Metadata: &s3tables.TableMetadata{
			Iceberg: &s3tables.IcebergMetadata{
				TableUUID: input.tableUUID.String(),
			},
			FullMetadata: metadataBytes,
		},
		MetadataVersion:  metadataVersion,
		MetadataLocation: newMetadataLocation,
	}
	createErr := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(ctx, mgrClient, "CreateTable", createReq, nil, input.identityName)
	})
	if createErr != nil {
		if cleanupErr := s.deleteMetadataFile(ctx, metadataBucket, metadataPath, metadataFileName); cleanupErr != nil {
			glog.V(1).Infof("Iceberg: failed to cleanup metadata file %s after create-on-commit failure: %v", newMetadataLocation, cleanupErr)
		}
		if isS3TablesConflict(createErr) || isS3TablesAlreadyExists(createErr) {
			return nil, &icebergRequestError{
				status:  http.StatusConflict,
				errType: "CommitFailedException",
				message: "Table was created concurrently",
			}
		}
		glog.Errorf("Iceberg: CommitTable CreateTable error: %v", createErr)
		return nil, &icebergRequestError{
			status:  http.StatusInternalServerError,
			errType: "InternalServerError",
			message: "Failed to commit table creation: " + createErr.Error(),
		}
	}

	markerBucket := input.markerBucket
	if markerBucket == "" {
		markerBucket = metadataBucket
	}
	if markerErr := s.deleteStageCreateMarkers(ctx, markerBucket, input.namespace, input.tableName); markerErr != nil {
		glog.V(1).Infof("Iceberg: failed to cleanup stage-create markers for %s.%s after finalize: %v", encodeNamespace(input.namespace), input.tableName, markerErr)
	}

	return &CommitTableResponse{
		MetadataLocation: newMetadataLocation,
		Metadata:         newMetadata,
	}, nil
}
