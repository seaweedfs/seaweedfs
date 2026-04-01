package iceberg

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// handleUpdateTable commits updates to a table.
// Implements the Iceberg REST Catalog commit protocol.
func (s *Server) handleUpdateTable(w http.ResponseWriter, r *http.Request) {
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

	// Parse commit request and keep statistics updates separate because iceberg-go v0.4.0
	// does not decode set/remove-statistics update actions yet.
	var raw struct {
		Identifier   *TableIdentifier  `json:"identifier,omitempty"`
		Requirements json.RawMessage   `json:"requirements"`
		Updates      []json.RawMessage `json:"updates"`
	}
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid request body: "+err.Error())
		return
	}

	var req CommitTableRequest
	req.Identifier = raw.Identifier
	var statisticsUpdates []statisticsUpdate
	if len(raw.Requirements) > 0 {
		if err := json.Unmarshal(raw.Requirements, &req.Requirements); err != nil {
			writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid requirements: "+err.Error())
			return
		}
	}
	if len(raw.Updates) > 0 {
		var err error
		req.Updates, statisticsUpdates, err = parseCommitUpdates(raw.Updates)
		if err != nil {
			writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid updates: "+err.Error())
			return
		}
	}
	maxCommitAttempts := 3
	generatedLegacyUUID := uuid.New()
	stageCreateEnabled := isStageCreateEnabled()
	for attempt := 1; attempt <= maxCommitAttempts; attempt++ {
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
			if isS3TablesNotFound(err) {
				location := fmt.Sprintf("s3://%s/%s/%s", bucketName, encodeNamespace(namespace), tableName)
				tableUUID := generatedLegacyUUID
				baseMetadataVersion := 0
				baseMetadataLocation := ""
				var baseMetadata table.Metadata

				var latestMarker *stageCreateMarker
				if stageCreateEnabled {
					var markerErr error
					latestMarker, markerErr = s.loadLatestStageCreateMarker(r.Context(), bucketName, namespace, tableName)
					if markerErr != nil {
						writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to load stage-create marker: "+markerErr.Error())
						return
					}
				}
				if latestMarker != nil {
					if latestMarker.Location != "" {
						location = strings.TrimSuffix(latestMarker.Location, "/")
					}
					if latestMarker.TableUUID != "" {
						if parsedUUID, parseErr := uuid.Parse(latestMarker.TableUUID); parseErr == nil {
							tableUUID = parsedUUID
						}
					}

					stagedMetadataLocation := latestMarker.StagedMetadataLocation
					if stagedMetadataLocation == "" {
						stagedMetadataLocation = fmt.Sprintf("%s/metadata/v1.metadata.json", strings.TrimSuffix(location, "/"))
					}
					stagedLocation := tableLocationFromMetadataLocation(stagedMetadataLocation)
					stagedFileName := path.Base(stagedMetadataLocation)
					stagedBucket, stagedPath, parseLocationErr := parseS3Location(stagedLocation)
					if parseLocationErr != nil {
						writeError(w, http.StatusInternalServerError, "InternalServerError", "Invalid staged metadata location: "+parseLocationErr.Error())
						return
					}
					stagedMetadataBytes, loadErr := s.loadMetadataFile(r.Context(), stagedBucket, stagedPath, stagedFileName)
					if loadErr != nil {
						if !errors.Is(loadErr, filer_pb.ErrNotFound) {
							writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to load staged metadata: "+loadErr.Error())
							return
						}
					} else if len(stagedMetadataBytes) > 0 {
						stagedMetadata, parseErr := table.ParseMetadataBytes(stagedMetadataBytes)
						if parseErr != nil {
							writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to parse staged metadata: "+parseErr.Error())
							return
						}
						// Staged metadata is only a template for table creation; commit starts from version 1.
						baseMetadata = stagedMetadata
						baseMetadataLocation = ""
						baseMetadataVersion = 0
						if stagedMetadata.TableUUID() != uuid.Nil {
							tableUUID = stagedMetadata.TableUUID()
						}
					}
				}

				hasAssertCreate := hasAssertCreateRequirement(req.Requirements)
				hasStagedTemplate := baseMetadata != nil
				if !(stageCreateEnabled && (hasAssertCreate || hasStagedTemplate)) {
					writeError(w, http.StatusNotFound, "NoSuchTableException", fmt.Sprintf("Table does not exist: %s", tableName))
					return
				}

				for _, requirement := range req.Requirements {
					validateAgainst := table.Metadata(nil)
					if hasStagedTemplate && requirement.GetType() != requirementAssertCreate {
						validateAgainst = baseMetadata
					}
					if requirementErr := requirement.Validate(validateAgainst); requirementErr != nil {
						writeError(w, http.StatusConflict, "CommitFailedException", "Requirement failed: "+requirementErr.Error())
						return
					}
				}

				if baseMetadata == nil {
					baseMetadata = newTableMetadata(tableUUID, location, nil, nil, nil, nil)
					if baseMetadata == nil {
						writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to build current metadata")
						return
					}
				}

				result, reqErr := s.finalizeCreateOnCommit(r.Context(), createOnCommitInput{
					bucketARN:         bucketARN,
					markerBucket:      bucketName,
					namespace:         namespace,
					tableName:         tableName,
					identityName:      identityName,
					location:          location,
					tableUUID:         tableUUID,
					baseMetadata:      baseMetadata,
					baseMetadataLoc:   baseMetadataLocation,
					baseMetadataVer:   baseMetadataVersion,
					updates:           req.Updates,
					statisticsUpdates: statisticsUpdates,
				})
				if reqErr != nil {
					writeError(w, reqErr.status, reqErr.errType, reqErr.message)
					return
				}
				writeJSON(w, http.StatusOK, result)
				return
			}
			glog.V(1).Infof("Iceberg: CommitTable GetTable error: %v", err)
			writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
			return
		}

		location := tableLocationFromMetadataLocation(getResp.MetadataLocation)
		if location == "" {
			location = fmt.Sprintf("s3://%s/%s/%s", bucketName, encodeNamespace(namespace), tableName)
		}
		tableUUID := uuid.Nil
		if getResp.Metadata != nil && getResp.Metadata.Iceberg != nil && getResp.Metadata.Iceberg.TableUUID != "" {
			if parsed, parseErr := uuid.Parse(getResp.Metadata.Iceberg.TableUUID); parseErr == nil {
				tableUUID = parsed
			}
		}
		if tableUUID == uuid.Nil {
			tableUUID = generatedLegacyUUID
		}

		var currentMetadata table.Metadata
		if getResp.Metadata != nil && len(getResp.Metadata.FullMetadata) > 0 {
			currentMetadata, err = table.ParseMetadataBytes(getResp.Metadata.FullMetadata)
			if err != nil {
				glog.Errorf("Iceberg: Failed to parse current metadata for %s: %v", tableName, err)
				writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to parse current metadata")
				return
			}
		} else {
			currentMetadata = newTableMetadata(tableUUID, location, nil, nil, nil, nil)
		}
		if currentMetadata == nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to build current metadata")
			return
		}

		for _, requirement := range req.Requirements {
			if err := requirement.Validate(currentMetadata); err != nil {
				writeError(w, http.StatusConflict, "CommitFailedException", "Requirement failed: "+err.Error())
				return
			}
		}

		builder, err := table.MetadataBuilderFromBase(currentMetadata, getResp.MetadataLocation)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to create metadata builder: "+err.Error())
			return
		}
		for _, update := range req.Updates {
			if err := update.Apply(builder); err != nil {
				writeError(w, http.StatusBadRequest, "BadRequestException", "Failed to apply update: "+err.Error())
				return
			}
		}

		newMetadata, err := builder.Build()
		if err != nil {
			writeError(w, http.StatusBadRequest, "BadRequestException", "Failed to build new metadata: "+err.Error())
			return
		}

		metadataVersion := getResp.MetadataVersion + 1
		metadataFileName := fmt.Sprintf("v%d.metadata.json", metadataVersion)
		newMetadataLocation := fmt.Sprintf("%s/metadata/%s", strings.TrimSuffix(location, "/"), metadataFileName)

		metadataBytes, err := json.Marshal(newMetadata)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to serialize metadata: "+err.Error())
			return
		}
		// iceberg-go does not currently support set/remove-statistics updates in MetadataBuilder.
		// Patch the encoded metadata JSON and parse it back to keep the response object consistent.
		metadataBytes, err = applyStatisticsUpdates(metadataBytes, statisticsUpdates)
		if err != nil {
			writeError(w, http.StatusBadRequest, "BadRequestException", "Failed to apply statistics updates: "+err.Error())
			return
		}
		newMetadata, err = table.ParseMetadataBytes(metadataBytes)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to parse committed metadata: "+err.Error())
			return
		}

		metadataBucket, metadataPath, err := parseS3Location(location)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Invalid table location: "+err.Error())
			return
		}
		if err := s.saveMetadataFile(r.Context(), metadataBucket, metadataPath, metadataFileName, metadataBytes); err != nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to save metadata file: "+err.Error())
			return
		}

		updateReq := &s3tables.UpdateTableRequest{
			TableBucketARN: bucketARN,
			Namespace:      namespace,
			Name:           tableName,
			VersionToken:   getResp.VersionToken,
			Metadata: &s3tables.TableMetadata{
				Iceberg: &s3tables.IcebergMetadata{
					TableUUID: tableUUID.String(),
				},
				FullMetadata: metadataBytes,
			},
			MetadataVersion:  metadataVersion,
			MetadataLocation: newMetadataLocation,
		}

		err = s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			mgrClient := s3tables.NewManagerClient(client)
			return s.tablesManager.Execute(r.Context(), mgrClient, "UpdateTable", updateReq, nil, identityName)
		})
		if err == nil {
			result := CommitTableResponse{
				MetadataLocation: newMetadataLocation,
				Metadata:         newMetadata,
			}
			writeJSON(w, http.StatusOK, result)
			return
		}

		if isS3TablesConflict(err) {
			if cleanupErr := s.deleteMetadataFile(r.Context(), metadataBucket, metadataPath, metadataFileName); cleanupErr != nil {
				glog.V(1).Infof("Iceberg: failed to cleanup metadata file %s on conflict: %v", newMetadataLocation, cleanupErr)
			}
			if attempt < maxCommitAttempts {
				glog.V(1).Infof("Iceberg: CommitTable conflict for %s (attempt %d/%d), retrying", tableName, attempt, maxCommitAttempts)
				jitter := time.Duration(rand.Int64N(int64(25 * time.Millisecond)))
				time.Sleep(time.Duration(50*attempt)*time.Millisecond + jitter)
				continue
			}
			writeError(w, http.StatusConflict, "CommitFailedException", "Version token mismatch")
			return
		}

		if cleanupErr := s.deleteMetadataFile(r.Context(), metadataBucket, metadataPath, metadataFileName); cleanupErr != nil {
			glog.V(1).Infof("Iceberg: failed to cleanup metadata file %s after update failure: %v", newMetadataLocation, cleanupErr)
		}
		glog.Errorf("Iceberg: CommitTable UpdateTable error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to commit table update: "+err.Error())
		return
	}
}
