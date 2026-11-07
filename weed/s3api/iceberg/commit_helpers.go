package iceberg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

const requirementAssertCreate = "assert-create"

// sleepBeforeCommitRetry backs off between commit attempts, jittered so that
// writers that collided do not line up again on the next try.
func sleepBeforeCommitRetry(attempt int) {
	jitter := time.Duration(rand.Int64N(int64(25 * time.Millisecond)))
	time.Sleep(time.Duration(50*attempt)*time.Millisecond + jitter)
}

// stageCommitMetadata writes the metadata file a commit will point the catalog
// at, returning the name and location it landed on. The exclusive create keeps
// one writer from overwriting another's file; when the name is already taken
// the metadata goes to a unique one instead of failing, because the file there
// may be an orphan left by an interrupted commit, and refusing would wedge the
// table until an orphan sweep ran. The catalog pointer, not the file name,
// decides which commit won. This mirrors how the maintenance worker stages its
// own metadata.
func (s *Server) stageCommitMetadata(ctx context.Context, metadataBucket, metadataPath, location, metadataFileName string, metadataBytes []byte) (string, string, error) {
	name, metadataLocation, _, err := s.stageOwnedMetadata(ctx, metadataBucket, metadataPath, location, metadataFileName, metadataBytes)
	return name, metadataLocation, err
}

// stageOwnedMetadata is stageCommitMetadata that also returns the token the
// file was tagged with, which discardStagedMetadata needs to remove it.
func (s *Server) stageOwnedMetadata(ctx context.Context, metadataBucket, metadataPath, location, metadataFileName string, metadataBytes []byte) (string, string, string, error) {
	token := uuid.NewString()
	extended := map[string][]byte{stageTokenKey: []byte(token)}
	err := s.saveMetadataBlob(ctx, metadataBucket, metadataPath, metadataFileName, metadataBytes, "application/json", true, extended)
	if errors.Is(err, filer_pb.ErrEntryAlreadyExists) {
		// v{N}-{uuid}, a form both the catalog and the maintenance worker
		// still read the version from.
		metadataFileName = fmt.Sprintf("%s-%s.metadata.json", strings.TrimSuffix(metadataFileName, ".metadata.json"), uuid.NewString())
		glog.V(1).Infof("Iceberg: metadata file already staged for %s, using %s", location, metadataFileName)
		err = s.saveMetadataBlob(ctx, metadataBucket, metadataPath, metadataFileName, metadataBytes, "application/json", true, extended)
	}
	if err != nil {
		return "", "", "", err
	}
	return metadataFileName, fmt.Sprintf("%s/metadata/%s", strings.TrimSuffix(location, "/"), metadataFileName), token, nil
}

// discardStagedMetadata removes a metadata file this request staged and the
// catalog will never point at. metadataFileName and token must be what
// stageOwnedMetadata returned. The name alone does not identify the file: the
// path can be reused once this request's file is gone — the table dropped and
// its directory purged, then a new create staging the same v1.metadata.json —
// so the delete goes through only while the entry still carries this
// request's token, and a replacement is left alone.
func (s *Server) discardStagedMetadata(ctx context.Context, metadataBucket, metadataPath, metadataFileName, metadataLocation, token string) {
	err := s.deleteStagedMetadataFile(ctx, metadataBucket, metadataPath, metadataFileName, token)
	if errors.Is(err, errStagedFileReplaced) {
		glog.V(1).Infof("Iceberg: keeping metadata file %s, it is no longer the one this request staged: %v", metadataLocation, err)
		return
	}
	if err != nil {
		glog.V(1).Infof("Iceberg: failed to cleanup metadata file %s after the catalog refused it: %v", metadataLocation, err)
	}
}

// registrationRefused reports whether a failed registration was refused
// before any record pointing at the staged file became visible, so the file can
// go. A name already taken, a conflict, and any other 4xx are refused that way:
// the create wrote nothing, or backed out what it wrote, or lost its
// still-hidden record to another create. One conflict is not: a create whose
// record was already visible, and was updated or renamed away before the
// create could back it out, leaves that record in place — under the original
// name or another — still pointing at the file. A transport failure or 5xx is
// not a refusal either: the record may have been written before the error
// surfaced.
func registrationRefused(err error) bool {
	if s3tables.IsPublishedTableChanged(err) {
		return false
	}
	if isS3TablesConflict(err) || isS3TablesAlreadyExists(err) {
		return true
	}
	status, _, _ := classifyManagerError(err)
	return status >= 400 && status < 500
}

type icebergRequestError struct {
	status  int
	errType string
	message string
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

// isS3TablesAlreadyExists reports whether err says the name is already taken.
// A typed error says so through its code alone: a server fault whose message
// happens to mention an existing entry is still a server fault, and taking it
// for a refusal would answer 409 and discard a file the catalog may point at.
// Only an untyped error is read by its message.
func isS3TablesAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	var tableErr *s3tables.S3TablesError
	if errors.As(err, &tableErr) {
		return tableErr.Type == s3tables.ErrCodeTableAlreadyExists || tableErr.Type == s3tables.ErrCodeNamespaceAlreadyExists
	}
	return strings.Contains(strings.ToLower(err.Error()), "already exists")
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
	metadataBytes = refreshDefaultNameMapping(metadataBytes, newMetadata)
	// Same spec-compliance fixup we apply on create-table; ensures
	// v{N}.metadata.json files written through this create-on-commit path are
	// also readable by strict Iceberg clients reading directly from S3.
	metadataBytes = ensureMetadataSpecCompliance(metadataBytes)
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
	if err := confineMetadataLocation(metadataBucket, metadataPath, input.markerBucket); err != nil {
		return nil, &icebergRequestError{
			status:  http.StatusBadRequest,
			errType: "BadRequestException",
			message: err.Error(),
		}
	}
	// Two creators racing for the same table both pick this name. Staging it
	// exclusively gives each its own file, so the loser can discard its file
	// below without deleting the one the winner's record points at.
	metadataFileName, newMetadataLocation, stageToken, err := s.stageOwnedMetadata(ctx, metadataBucket, metadataPath, input.location, metadataFileName, metadataBytes)
	if err != nil {
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
	var createResp s3tables.CreateTableResponse
	createErr := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(ctx, mgrClient, "CreateTable", createReq, &createResp, input.identityName)
	})
	tableCreatedConcurrently := &icebergRequestError{
		status:  http.StatusConflict,
		errType: "CommitFailedException",
		message: "Table was created concurrently",
	}
	if createErr != nil {
		if registrationRefused(createErr) {
			s.discardStagedMetadata(ctx, metadataBucket, metadataPath, metadataFileName, newMetadataLocation, stageToken)
		}
		if isS3TablesConflict(createErr) || isS3TablesAlreadyExists(createErr) {
			return nil, tableCreatedConcurrently
		}
		glog.Errorf("Iceberg: CommitTable CreateTable error: %v", createErr)
		return nil, &icebergRequestError{
			status:  http.StatusInternalServerError,
			errType: "InternalServerError",
			message: "Failed to commit table creation: " + createErr.Error(),
		}
	}
	// A name that already holds a table is answered with that table's own
	// location rather than an error: another creator won the race. That
	// location is empty for a table registered without one.
	if createResp.MetadataLocation != newMetadataLocation {
		s.discardStagedMetadata(ctx, metadataBucket, metadataPath, metadataFileName, newMetadataLocation, stageToken)
		return nil, tableCreatedConcurrently
	}

	markerBucket := input.markerBucket
	if markerBucket == "" {
		markerBucket = metadataBucket
	}
	if markerErr := s.deleteStageCreateMarkers(ctx, markerBucket, input.namespace, input.tableName); markerErr != nil {
		glog.V(1).Infof("Iceberg: failed to cleanup stage-create markers for %s.%s after finalize: %v", flattenNamespacePath(input.namespace), input.tableName, markerErr)
	}

	return &CommitTableResponse{
		MetadataLocation: newMetadataLocation,
		Metadata:         newMetadata,
	}, nil
}

// Manager requests are size-capped like registrations, so a commit whose new
// metadata is too large to ride along flips the pointer by location alone —
// the staged file already holds it. The record's stored copy must be cleared
// with it, or loads would keep serving the pre-commit bytes.
func fitUpdateTableRequest(req *s3tables.UpdateTableRequest) {
	if req.Metadata == nil || len(req.Metadata.FullMetadata) == 0 {
		return
	}
	if body, err := json.Marshal(req); err == nil && len(body) <= s3tables.MaxRequestBodySize {
		return
	}
	req.Metadata = &s3tables.TableMetadata{Iceberg: req.Metadata.Iceberg}
	req.ClearFullMetadata = true
}

// authorizeCommit asks the S3 Tables layer, before anything is written, whether
// the caller may update the table. Both commit paths stage the new metadata
// file in the table's directory before UpdateTable authorizes the pointer
// flip, so without this gate a caller who may only read the table could still
// make the server write and delete files under it. UpdateTable decides again
// when the pointer flips.
func (s *Server) authorizeCommit(ctx context.Context, bucketARN string, namespace []string, tableName, identityName string) *icebergRequestError {
	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return s.tablesManager.AuthorizeUpdateTable(ctx, s3tables.NewManagerClient(client), &s3tables.UpdateTableRequest{
			TableBucketARN: bucketARN,
			Namespace:      namespace,
			Name:           tableName,
		}, identityName)
	})
	if err == nil {
		bucketName, _ := s3tables.ParseBucketNameFromARN(bucketARN)
		err = s.tablesManager.CheckVectorNamespaceWrite(ctx, s.filerClient, bucketName, namespace)
	}
	if err == nil {
		return nil
	}
	var tableErr *s3tables.S3TablesError
	if errors.As(err, &tableErr) {
		switch tableErr.Type {
		case s3tables.ErrCodeAccessDenied:
			return &icebergRequestError{http.StatusForbidden, "ForbiddenException", fmt.Sprintf("Not authorized to commit to table %s", tableName)}
		case s3tables.ErrCodeNoSuchTable, s3tables.ErrCodeNoSuchNamespace:
			return &icebergRequestError{http.StatusNotFound, "NoSuchTableException", fmt.Sprintf("Table does not exist: %s", tableName)}
		}
	}
	glog.V(1).Infof("Iceberg: commit authorization for %s failed: %v", tableName, err)
	return &icebergRequestError{http.StatusInternalServerError, "InternalServerError", "Failed to authorize commit: " + err.Error()}
}

// authorizeViewCommit is authorizeCommit for a view, whose update path stages
// its metadata file before UpdateView authorizes the pointer flip.
func (s *Server) authorizeViewCommit(ctx context.Context, bucketARN string, namespace []string, viewName, identityName string) *icebergRequestError {
	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return s.tablesManager.AuthorizeUpdateView(ctx, s3tables.NewManagerClient(client), &s3tables.UpdateViewRequest{
			TableBucketARN: bucketARN,
			Namespace:      namespace,
			Name:           viewName,
		}, identityName)
	})
	if err == nil {
		return nil
	}
	var tableErr *s3tables.S3TablesError
	if errors.As(err, &tableErr) {
		switch tableErr.Type {
		case s3tables.ErrCodeAccessDenied:
			return &icebergRequestError{http.StatusForbidden, "ForbiddenException", fmt.Sprintf("Not authorized to commit to view %s", viewName)}
		case s3tables.ErrCodeNoSuchView, s3tables.ErrCodeNoSuchNamespace:
			return &icebergRequestError{http.StatusNotFound, "NoSuchViewException", fmt.Sprintf("View does not exist: %s", viewName)}
		}
	}
	glog.V(1).Infof("Iceberg: view commit authorization for %s failed: %v", viewName, err)
	return &icebergRequestError{http.StatusInternalServerError, "InternalServerError", "Failed to authorize commit: " + err.Error()}
}
