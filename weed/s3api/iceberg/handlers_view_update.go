package iceberg

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/apache/iceberg-go/view"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// handleUpdateView applies requirements and updates to a view, writes a new
// metadata.json, and flips the stored pointer. Mirrors the table commit flow.
func (s *Server) handleUpdateView(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	viewName := vars["view"]
	if len(namespace) == 0 || viewName == "" {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace and view name are required")
		return
	}

	var req UpdateViewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid request body: "+err.Error())
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)
	identityName := s3_constants.GetIdentityNameFromContext(r)

	const maxCommitAttempts = 3
	for attempt := 1; attempt <= maxCommitAttempts; attempt++ {
		getResp, err := s.getView(r, namespace, viewName)
		if err != nil {
			if isViewNotFound(err) {
				writeError(w, http.StatusNotFound, "NoSuchViewException", fmt.Sprintf("View does not exist: %s", viewName))
				return
			}
			glog.V(1).Infof("Iceberg: UpdateView GetView error: %v", err)
			writeManagerError(w, err)
			return
		}
		if getResp.Metadata == nil || len(getResp.Metadata.FullMetadata) == 0 {
			writeError(w, http.StatusInternalServerError, "InternalServerError", "view has no metadata")
			return
		}

		currentMetadata, err := view.ParseMetadataBytes(getResp.Metadata.FullMetadata)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to parse current view metadata: "+err.Error())
			return
		}

		for _, requirement := range req.Requirements {
			if err := requirement.Validate(currentMetadata); err != nil {
				writeError(w, http.StatusConflict, "CommitFailedException", "Requirement failed: "+err.Error())
				return
			}
		}

		builder, err := view.MetadataBuilderFromBase(currentMetadata)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to create view metadata builder: "+err.Error())
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
			writeError(w, http.StatusBadRequest, "BadRequestException", "Failed to build new view metadata: "+err.Error())
			return
		}

		location := tableLocationFromMetadataLocation(getResp.MetadataLocation)
		if location == "" {
			location = newMetadata.Location()
		}
		metadataVersion := getResp.MetadataVersion + 1
		metadataFileName := fmt.Sprintf("v%d.metadata.json", metadataVersion)
		newMetadataLocation := fmt.Sprintf("%s/metadata/%s", strings.TrimSuffix(location, "/"), metadataFileName)

		metadataBytes, err := json.Marshal(newMetadata)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to serialize view metadata: "+err.Error())
			return
		}

		metadataBucket, metadataPath, err := parseS3Location(location)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Invalid view location: "+err.Error())
			return
		}
		if err := s.saveMetadataFile(r.Context(), metadataBucket, metadataPath, metadataFileName, metadataBytes); err != nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to save view metadata file: "+err.Error())
			return
		}

		tableUUID := newMetadata.ViewUUID()
		if tableUUID == uuid.Nil {
			tableUUID = currentMetadata.ViewUUID()
		}
		updateReq := &s3tables.UpdateViewRequest{
			TableBucketARN: bucketARN,
			Namespace:      namespace,
			Name:           viewName,
			VersionToken:   getResp.VersionToken,
			Metadata: &s3tables.TableMetadata{
				Iceberg:      &s3tables.IcebergMetadata{TableUUID: tableUUID.String()},
				FullMetadata: metadataBytes,
			},
			MetadataVersion:  metadataVersion,
			MetadataLocation: newMetadataLocation,
		}
		err = s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			mgrClient := s3tables.NewManagerClient(client)
			return s.tablesManager.Execute(r.Context(), mgrClient, "UpdateView", updateReq, nil, identityName)
		})
		if err == nil {
			writeJSON(w, http.StatusOK, ViewResponse{
				MetadataLocation: newMetadataLocation,
				Metadata:         newMetadata,
				Config:           s.buildFileIOConfig(),
			})
			return
		}

		if isS3TablesConflict(err) {
			if cleanupErr := s.deleteMetadataFile(r.Context(), metadataBucket, metadataPath, metadataFileName); cleanupErr != nil {
				glog.V(1).Infof("Iceberg: failed to cleanup view metadata file %s on conflict: %v", newMetadataLocation, cleanupErr)
			}
			if attempt < maxCommitAttempts {
				glog.V(1).Infof("Iceberg: UpdateView conflict for %s (attempt %d/%d), retrying", viewName, attempt, maxCommitAttempts)
				jitter := time.Duration(rand.Int64N(int64(25 * time.Millisecond)))
				time.Sleep(time.Duration(50*attempt)*time.Millisecond + jitter)
				continue
			}
			writeError(w, http.StatusConflict, "CommitFailedException", "Version token mismatch")
			return
		}

		if cleanupErr := s.deleteMetadataFile(r.Context(), metadataBucket, metadataPath, metadataFileName); cleanupErr != nil {
			glog.V(1).Infof("Iceberg: failed to cleanup view metadata file %s after update failure: %v", newMetadataLocation, cleanupErr)
		}
		glog.Errorf("Iceberg: UpdateView error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to commit view update: "+err.Error())
		return
	}
}
