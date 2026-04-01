package iceberg

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

var errTableNameRequired = errors.New("table name is required")

func validateCreateTableRequest(req CreateTableRequest) error {
	if req.Name == "" {
		return errTableNameRequired
	}
	return nil
}

// handleListTables lists tables in a namespace.
func (s *Server) handleListTables(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	if len(namespace) == 0 {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace is required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	pageToken, pageSize, err := parsePagination(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", err.Error())
		return
	}

	listReq := &s3tables.ListTablesRequest{
		TableBucketARN:    bucketARN,
		Namespace:         namespace,
		ContinuationToken: pageToken,
		MaxTables:         pageSize,
	}
	var listResp s3tables.ListTablesResponse

	err = s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "ListTables", listReq, &listResp, identityName)
	})

	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, "NoSuchNamespaceException", fmt.Sprintf("Namespace does not exist: %v", namespace))
			return
		}
		glog.V(1).Infof("Iceberg: ListTables error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	// Convert to Iceberg format
	identifiers := make([]TableIdentifier, 0, len(listResp.Tables))
	for _, t := range listResp.Tables {
		identifiers = append(identifiers, TableIdentifier{
			Namespace: namespace,
			Name:      t.Name,
		})
	}

	result := ListTablesResponse{
		NextPageToken: listResp.ContinuationToken,
		Identifiers:   identifiers,
	}
	writeJSON(w, http.StatusOK, result)
}

// handleCreateTable creates a new table.
func (s *Server) handleCreateTable(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	if len(namespace) == 0 {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace is required")
		return
	}

	var req CreateTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid request body")
		return
	}

	if err := validateCreateTableRequest(req); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", err.Error())
		return
	}
	if req.StageCreate && !isStageCreateEnabled() {
		writeError(w, http.StatusNotImplemented, "NotImplementedException", "stage-create is disabled")
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	// Generate UUID for the new table
	tableUUID := uuid.New()
	tablePath := path.Join(encodeNamespace(namespace), req.Name)
	location := strings.TrimSuffix(req.Location, "/")
	if location == "" {
		if req.Properties != nil {
			if warehouse := strings.TrimSuffix(req.Properties["warehouse"], "/"); warehouse != "" {
				location = fmt.Sprintf("%s/%s", warehouse, tablePath)
			}
		}
		if location == "" {
			if warehouse := strings.TrimSuffix(os.Getenv("ICEBERG_WAREHOUSE"), "/"); warehouse != "" {
				location = fmt.Sprintf("%s/%s", warehouse, tablePath)
			}
		}
		if location == "" {
			location = fmt.Sprintf("s3://%s/%s", bucketName, tablePath)
		}
	} else {
		parsedBucket, parsedPath, err := parseS3Location(location)
		if err != nil {
			writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid table location: "+err.Error())
			return
		}
		if parsedPath == "" {
			location = fmt.Sprintf("s3://%s/%s", parsedBucket, tablePath)
		}
	}

	// Build proper Iceberg table metadata using iceberg-go types
	metadata := newTableMetadata(tableUUID, location, req.Schema, req.PartitionSpec, req.WriteOrder, req.Properties)
	if metadata == nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to build table metadata")
		return
	}

	// Serialize metadata to JSON
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to serialize metadata: "+err.Error())
		return
	}

	tableName := req.Name
	metadataFileName := "v1.metadata.json" // Initial version is always 1
	metadataLocation := fmt.Sprintf("%s/metadata/%s", location, metadataFileName)
	metadataBucket, metadataPath, err := parseS3Location(location)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "Invalid table location: "+err.Error())
		return
	}

	// Stage-create persists metadata in the internal staged area and skips S3Tables registration.
	if req.StageCreate {
		stagedTablePath := stageCreateStagedTablePath(namespace, tableName, tableUUID)
		if err := s.saveMetadataFile(r.Context(), metadataBucket, stagedTablePath, metadataFileName, metadataBytes); err != nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to save staged metadata file: "+err.Error())
			return
		}
		stagedMetadataLocation := fmt.Sprintf("s3://%s/%s/metadata/%s", metadataBucket, stagedTablePath, metadataFileName)
		if markerErr := s.writeStageCreateMarker(r.Context(), bucketName, namespace, tableName, tableUUID, location, stagedMetadataLocation); markerErr != nil {
			glog.V(1).Infof("Iceberg: failed to persist stage-create marker for %s.%s: %v", encodeNamespace(namespace), tableName, markerErr)
		}
		result := LoadTableResult{
			MetadataLocation: metadataLocation,
			Metadata:         metadata,
			Config:           make(iceberg.Properties),
		}
		writeJSON(w, http.StatusOK, result)
		return
	}
	if err := s.saveMetadataFile(r.Context(), metadataBucket, metadataPath, metadataFileName, metadataBytes); err != nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to save metadata file: "+err.Error())
		return
	}

	// Use S3 Tables manager to create table
	createReq := &s3tables.CreateTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           tableName,
		Format:         "ICEBERG",
		Metadata: &s3tables.TableMetadata{
			Iceberg: &s3tables.IcebergMetadata{
				TableUUID: tableUUID.String(),
			},
			FullMetadata: metadataBytes,
		},
		MetadataLocation: metadataLocation,
		MetadataVersion:  1,
	}
	var createResp s3tables.CreateTableResponse

	err = s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "CreateTable", createReq, &createResp, identityName)
	})

	if err != nil {
		if tableErr, ok := err.(*s3tables.S3TablesError); ok && tableErr.Type == s3tables.ErrCodeTableAlreadyExists {
			getReq := &s3tables.GetTableRequest{
				TableBucketARN: bucketARN,
				Namespace:      namespace,
				Name:           tableName,
			}
			var getResp s3tables.GetTableResponse
			getErr := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
				mgrClient := s3tables.NewManagerClient(client)
				return s.tablesManager.Execute(r.Context(), mgrClient, "GetTable", getReq, &getResp, identityName)
			})
			if getErr != nil {
				writeError(w, http.StatusConflict, "AlreadyExistsException", err.Error())
				return
			}
			result := buildLoadTableResult(getResp, bucketName, namespace, tableName)
			writeJSON(w, http.StatusOK, result)
			return
		}
		if strings.Contains(err.Error(), "already exists") {
			getReq := &s3tables.GetTableRequest{
				TableBucketARN: bucketARN,
				Namespace:      namespace,
				Name:           tableName,
			}
			var getResp s3tables.GetTableResponse
			getErr := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
				mgrClient := s3tables.NewManagerClient(client)
				return s.tablesManager.Execute(r.Context(), mgrClient, "GetTable", getReq, &getResp, identityName)
			})
			if getErr != nil {
				writeError(w, http.StatusConflict, "AlreadyExistsException", err.Error())
				return
			}
			result := buildLoadTableResult(getResp, bucketName, namespace, tableName)
			writeJSON(w, http.StatusOK, result)
			return
		}
		glog.V(1).Infof("Iceberg: CreateTable error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	// Use returned location if available, otherwise fallback to local one
	finalLocation := createResp.MetadataLocation
	if finalLocation == "" {
		finalLocation = metadataLocation
	}
	if markerErr := s.deleteStageCreateMarkers(r.Context(), bucketName, namespace, tableName); markerErr != nil {
		glog.V(1).Infof("Iceberg: failed to cleanup stage-create markers for %s.%s after create: %v", encodeNamespace(namespace), tableName, markerErr)
	}

	result := LoadTableResult{
		MetadataLocation: finalLocation,
		Metadata:         metadata,
		Config:           make(iceberg.Properties),
	}
	writeJSON(w, http.StatusOK, result)
}

// handleLoadTable loads table metadata.
func (s *Server) handleLoadTable(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	tableName := vars["table"]

	if len(namespace) == 0 || tableName == "" {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace and table name are required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	getReq := &s3tables.GetTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           tableName,
	}
	var getResp s3tables.GetTableResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetTable", getReq, &getResp, identityName)
	})

	if err != nil {

		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, "NoSuchTableException", fmt.Sprintf("Table does not exist: %s", tableName))
			return
		}
		glog.V(1).Infof("Iceberg: LoadTable error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	result := buildLoadTableResult(getResp, bucketName, namespace, tableName)
	writeJSON(w, http.StatusOK, result)
}

func buildLoadTableResult(getResp s3tables.GetTableResponse, bucketName string, namespace []string, tableName string) LoadTableResult {
	location := tableLocationFromMetadataLocation(getResp.MetadataLocation)
	if location == "" {
		location = fmt.Sprintf("s3://%s/%s/%s", bucketName, encodeNamespace(namespace), tableName)
	}
	tableUUID := uuid.Nil
	if getResp.Metadata != nil && getResp.Metadata.Iceberg != nil && getResp.Metadata.Iceberg.TableUUID != "" {
		if parsed, err := uuid.Parse(getResp.Metadata.Iceberg.TableUUID); err == nil {
			tableUUID = parsed
		}
	}
	// Use Nil UUID if not found in storage (legacy table)
	// Stability is guaranteed by not generating random UUIDs on read

	var metadata table.Metadata
	if getResp.Metadata != nil && len(getResp.Metadata.FullMetadata) > 0 {
		var err error
		metadata, err = table.ParseMetadataBytes(getResp.Metadata.FullMetadata)
		if err != nil {
			glog.Warningf("Iceberg: Failed to parse persisted metadata for %s: %v", tableName, err)
			// Attempt to reconstruct from IcebergMetadata if available, otherwise synthetic
			// TODO: Extract schema/spec from getResp.Metadata.Iceberg if FullMetadata fails but partial info exists?
			// For now, fallback to empty metadata
			metadata = newTableMetadata(tableUUID, location, nil, nil, nil, nil)
		}
	} else {
		// No full metadata, create synthetic
		// TODO: If we had stored schema in IcebergMetadata, we would pass it here
		metadata = newTableMetadata(tableUUID, location, nil, nil, nil, nil)
	}

	return LoadTableResult{
		MetadataLocation: getResp.MetadataLocation,
		Metadata:         metadata,
		Config:           make(iceberg.Properties),
	}
}

// handleTableExists checks if a table exists.
func (s *Server) handleTableExists(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	tableName := vars["table"]

	if len(namespace) == 0 || tableName == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	getReq := &s3tables.GetTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           tableName,
	}
	var getResp s3tables.GetTableResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetTable", getReq, &getResp, identityName)
	})

	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleDropTable deletes a table.
func (s *Server) handleDropTable(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	tableName := vars["table"]

	if len(namespace) == 0 || tableName == "" {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace and table name are required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	deleteReq := &s3tables.DeleteTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           tableName,
	}

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "DeleteTable", deleteReq, nil, identityName)
	})

	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, "NoSuchTableException", fmt.Sprintf("Table does not exist: %s", tableName))
			return
		}
		glog.V(1).Infof("Iceberg: DropTable error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// newTableMetadata creates a new table.Metadata object with the given parameters.
// Uses iceberg-go's MetadataBuilder pattern for proper spec compliance.
func newTableMetadata(
	tableUUID uuid.UUID,
	location string,
	schema *iceberg.Schema,
	partitionSpec *iceberg.PartitionSpec,
	sortOrder *table.SortOrder,
	props iceberg.Properties,
) table.Metadata {
	// Add schema - use provided or create empty schema
	var s *iceberg.Schema
	if schema != nil {
		s = schema
	} else {
		s = iceberg.NewSchema(0)
	}

	// Add partition spec
	var pSpec *iceberg.PartitionSpec
	if partitionSpec != nil {
		pSpec = partitionSpec
	} else {
		unpartitioned := iceberg.NewPartitionSpecID(0)
		pSpec = &unpartitioned
	}

	// Add sort order
	var so table.SortOrder
	if sortOrder != nil {
		so = *sortOrder
	} else {
		so = table.UnsortedSortOrder
	}

	// Create properties map if nil
	if props == nil {
		props = make(iceberg.Properties)
	}

	// Create metadata directly using the constructor which ensures spec compliance for V2
	metadata, err := table.NewMetadataWithUUID(s, pSpec, so, location, props, tableUUID)
	if err != nil {
		glog.Errorf("Failed to create metadata: %v", err)
		return nil
	}

	return metadata
}
