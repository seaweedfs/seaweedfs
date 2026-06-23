package iceberg

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// CommitTransactionRequest is sent to POST /v1/transactions/commit.
type CommitTransactionRequest struct {
	TableChanges []tableChangeRequest `json:"table-changes"`
}

type tableChangeRequest struct {
	Identifier   *TableIdentifier  `json:"identifier"`
	Requirements json.RawMessage   `json:"requirements"`
	Updates      []json.RawMessage `json:"updates"`
}

// preparedTableCommit holds the per-table work resolved during validation so
// pointer flips (and their rollback) can run after every table is validated.
type preparedTableCommit struct {
	namespace        []string
	tableName        string
	tableUUID        uuid.UUID
	versionToken     string
	prevMetadataLoc  string
	metadataBucket   string
	metadataPath     string
	metadataFileName string
	metadataBytes    []byte
	metadataVersion  int
	newMetadataLoc   string
}

// handleCommitTransaction commits changes to multiple tables in one request.
// Validation is atomic (all requirements evaluated before any write); pointer
// flips are best-effort with rollback, so this is not crash-atomic.
func (s *Server) handleCommitTransaction(w http.ResponseWriter, r *http.Request) {
	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)
	identityName := s3_constants.GetIdentityNameFromContext(r)

	var req CommitTransactionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid request body: "+err.Error())
		return
	}
	if len(req.TableChanges) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Phase 1: resolve, load, and validate every table; build new metadata bytes.
	prepared := make([]preparedTableCommit, 0, len(req.TableChanges))
	for _, change := range req.TableChanges {
		if change.Identifier == nil || len(change.Identifier.Namespace) == 0 || change.Identifier.Name == "" {
			writeError(w, http.StatusBadRequest, "BadRequestException", "Each table change requires identifier namespace and name")
			return
		}
		pc, reqErr := s.prepareTableCommit(r.Context(), bucketName, bucketARN, identityName, change)
		if reqErr != nil {
			writeError(w, reqErr.status, reqErr.errType, reqErr.message)
			return
		}
		prepared = append(prepared, *pc)
	}

	// Phase 2: write each new metadata.json object.
	for i := range prepared {
		pc := &prepared[i]
		if err := s.saveMetadataFile(r.Context(), pc.metadataBucket, pc.metadataPath, pc.metadataFileName, pc.metadataBytes); err != nil {
			s.cleanupPreparedMetadata(r.Context(), prepared[:i+1])
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to save metadata file: "+err.Error())
			return
		}
	}

	// Phase 3: flip each table's pointer xattr; on failure roll back prior flips.
	for i := range prepared {
		pc := &prepared[i]
		if err := s.flipTablePointer(r.Context(), bucketARN, identityName, pc); err != nil {
			s.rollbackTablePointers(r.Context(), bucketARN, identityName, prepared[:i])
			s.cleanupPreparedMetadata(r.Context(), prepared)
			if isS3TablesConflict(err) {
				writeError(w, http.StatusConflict, "CommitFailedException", "Version token mismatch")
				return
			}
			glog.Errorf("Iceberg: CommitTransaction UpdateTable error: %v", err)
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to commit table update: "+err.Error())
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) prepareTableCommit(ctx context.Context, bucketName, bucketARN, identityName string, change tableChangeRequest) (*preparedTableCommit, *icebergRequestError) {
	namespace := []string(change.Identifier.Namespace)
	tableName := change.Identifier.Name

	requirements, updates, statisticsUpdates, reqErr := parseTableChange(change)
	if reqErr != nil {
		return nil, reqErr
	}

	getReq := &s3tables.GetTableRequest{TableBucketARN: bucketARN, Namespace: namespace, Name: tableName}
	var getResp s3tables.GetTableResponse
	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(ctx, mgrClient, "GetTable", getReq, &getResp, identityName)
	})
	if err != nil {
		if isS3TablesNotFound(err) {
			return nil, &icebergRequestError{http.StatusNotFound, "NoSuchTableException", fmt.Sprintf("Table does not exist: %s", tableName)}
		}
		glog.V(1).Infof("Iceberg: CommitTransaction GetTable error: %v", err)
		return nil, &icebergRequestError{http.StatusInternalServerError, "InternalServerError", err.Error()}
	}

	location := tableLocationFromMetadataLocation(getResp.MetadataLocation)
	if location == "" {
		location = fmt.Sprintf("s3://%s/%s", bucketName, path.Join(flattenNamespacePath(namespace), tableName))
	}
	tableUUID := uuid.Nil
	if getResp.Metadata != nil && getResp.Metadata.Iceberg != nil && getResp.Metadata.Iceberg.TableUUID != "" {
		if parsed, parseErr := uuid.Parse(getResp.Metadata.Iceberg.TableUUID); parseErr == nil {
			tableUUID = parsed
		}
	}
	if tableUUID == uuid.Nil {
		tableUUID = uuid.New()
	}

	var currentMetadata table.Metadata
	if getResp.Metadata != nil && len(getResp.Metadata.FullMetadata) > 0 {
		currentMetadata, err = table.ParseMetadataBytes(getResp.Metadata.FullMetadata)
		if err != nil {
			return nil, &icebergRequestError{http.StatusInternalServerError, "InternalServerError", "Failed to parse current metadata"}
		}
	} else {
		currentMetadata = newTableMetadata(tableUUID, location, nil, nil, nil, nil)
	}
	if currentMetadata == nil {
		return nil, &icebergRequestError{http.StatusInternalServerError, "InternalServerError", "Failed to build current metadata"}
	}

	for _, requirement := range requirements {
		if err := requirement.Validate(currentMetadata); err != nil {
			return nil, &icebergRequestError{http.StatusConflict, "CommitFailedException", "Requirement failed: " + err.Error()}
		}
	}

	builder, err := table.MetadataBuilderFromBase(currentMetadata, getResp.MetadataLocation)
	if err != nil {
		return nil, &icebergRequestError{http.StatusInternalServerError, "InternalServerError", "Failed to create metadata builder: " + err.Error()}
	}
	for _, update := range updates {
		if err := update.Apply(builder); err != nil {
			return nil, &icebergRequestError{http.StatusBadRequest, "BadRequestException", "Failed to apply update: " + err.Error()}
		}
	}
	newMetadata, err := builder.Build()
	if err != nil {
		return nil, &icebergRequestError{http.StatusBadRequest, "BadRequestException", "Failed to build new metadata: " + err.Error()}
	}

	metadataVersion := getResp.MetadataVersion + 1
	metadataFileName := fmt.Sprintf("v%d.metadata.json", metadataVersion)
	newMetadataLocation := fmt.Sprintf("%s/metadata/%s", strings.TrimSuffix(location, "/"), metadataFileName)

	metadataBytes, err := json.Marshal(newMetadata)
	if err != nil {
		return nil, &icebergRequestError{http.StatusInternalServerError, "InternalServerError", "Failed to serialize metadata: " + err.Error()}
	}
	metadataBytes, err = applyStatisticsUpdates(metadataBytes, statisticsUpdates)
	if err != nil {
		return nil, &icebergRequestError{http.StatusBadRequest, "BadRequestException", "Failed to apply statistics updates: " + err.Error()}
	}
	metadataBytes = ensureMetadataSpecCompliance(metadataBytes)

	metadataBucket, metadataPath, err := parseS3Location(location)
	if err != nil {
		return nil, &icebergRequestError{http.StatusInternalServerError, "InternalServerError", "Invalid table location: " + err.Error()}
	}

	return &preparedTableCommit{
		namespace:        namespace,
		tableName:        tableName,
		tableUUID:        tableUUID,
		versionToken:     getResp.VersionToken,
		prevMetadataLoc:  getResp.MetadataLocation,
		metadataBucket:   metadataBucket,
		metadataPath:     metadataPath,
		metadataFileName: metadataFileName,
		metadataBytes:    metadataBytes,
		metadataVersion:  metadataVersion,
		newMetadataLoc:   newMetadataLocation,
	}, nil
}

func parseTableChange(change tableChangeRequest) (table.Requirements, table.Updates, []statisticsUpdate, *icebergRequestError) {
	var requirements table.Requirements
	if len(change.Requirements) > 0 {
		if err := json.Unmarshal(change.Requirements, &requirements); err != nil {
			return nil, nil, nil, &icebergRequestError{http.StatusBadRequest, "BadRequestException", "Invalid requirements: " + err.Error()}
		}
	}
	var updates table.Updates
	var statisticsUpdates []statisticsUpdate
	if len(change.Updates) > 0 {
		var err error
		updates, statisticsUpdates, err = parseCommitUpdates(change.Updates)
		if err != nil {
			return nil, nil, nil, &icebergRequestError{http.StatusBadRequest, "BadRequestException", "Invalid updates: " + err.Error()}
		}
	}
	return requirements, updates, statisticsUpdates, nil
}

func (s *Server) flipTablePointer(ctx context.Context, bucketARN, identityName string, pc *preparedTableCommit) error {
	updateReq := &s3tables.UpdateTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      pc.namespace,
		Name:           pc.tableName,
		VersionToken:   pc.versionToken,
		Metadata: &s3tables.TableMetadata{
			Iceberg:      &s3tables.IcebergMetadata{TableUUID: pc.tableUUID.String()},
			FullMetadata: pc.metadataBytes,
		},
		MetadataVersion:  pc.metadataVersion,
		MetadataLocation: pc.newMetadataLoc,
	}
	return s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(ctx, mgrClient, "UpdateTable", updateReq, nil, identityName)
	})
}

// rollbackTablePointers restores already-flipped pointers to their previous
// MetadataLocation. Best-effort: a failed rollback is logged, not surfaced.
func (s *Server) rollbackTablePointers(ctx context.Context, bucketARN, identityName string, flipped []preparedTableCommit) {
	for i := range flipped {
		pc := &flipped[i]
		updateReq := &s3tables.UpdateTableRequest{
			TableBucketARN:   bucketARN,
			Namespace:        pc.namespace,
			Name:             pc.tableName,
			MetadataLocation: pc.prevMetadataLoc,
		}
		err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			mgrClient := s3tables.NewManagerClient(client)
			return s.tablesManager.Execute(ctx, mgrClient, "UpdateTable", updateReq, nil, identityName)
		})
		if err != nil {
			glog.Errorf("Iceberg: CommitTransaction rollback of %s failed: %v", pc.tableName, err)
		}
	}
}

func (s *Server) cleanupPreparedMetadata(ctx context.Context, prepared []preparedTableCommit) {
	for i := range prepared {
		pc := &prepared[i]
		if cleanupErr := s.deleteMetadataFile(ctx, pc.metadataBucket, pc.metadataPath, pc.metadataFileName); cleanupErr != nil {
			glog.V(1).Infof("Iceberg: failed to cleanup metadata file %s: %v", pc.newMetadataLoc, cleanupErr)
		}
	}
}
