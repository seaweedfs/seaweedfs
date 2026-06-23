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
	"github.com/apache/iceberg-go/view"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// handleListViews lists views in a namespace.
func (s *Server) handleListViews(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	if len(namespace) == 0 {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace is required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)
	identityName := s3_constants.GetIdentityNameFromContext(r)

	pageToken, pageSize, err := parsePagination(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", err.Error())
		return
	}

	listReq := &s3tables.ListViewsRequest{
		TableBucketARN:    bucketARN,
		Namespace:         namespace,
		ContinuationToken: pageToken,
		MaxViews:          pageSize,
	}
	var listResp s3tables.ListViewsResponse
	err = s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "ListViews", listReq, &listResp, identityName)
	})
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, "NoSuchNamespaceException", fmt.Sprintf("Namespace does not exist: %v", namespace))
			return
		}
		glog.V(1).Infof("Iceberg: ListViews error: %v", err)
		writeManagerError(w, err)
		return
	}

	identifiers := make([]TableIdentifier, 0, len(listResp.Views))
	for _, v := range listResp.Views {
		identifiers = append(identifiers, TableIdentifier{Namespace: namespace, Name: v.Name})
	}
	writeJSON(w, http.StatusOK, ListViewsResponse{
		NextPageToken: listResp.ContinuationToken,
		Identifiers:   identifiers,
	})
}

// handleCreateView creates a new view and writes its v1 metadata.json.
func (s *Server) handleCreateView(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	if len(namespace) == 0 {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace is required")
		return
	}

	var req CreateViewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid request body")
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "BadRequestException", "view name is required")
		return
	}
	if req.Schema == nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", "view schema is required")
		return
	}
	if req.ViewVersion == nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", "view-version is required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)
	identityName := s3_constants.GetIdentityNameFromContext(r)

	viewPath := path.Join(flattenNamespacePath(namespace), req.Name)
	location := strings.TrimSuffix(req.Location, "/")
	if location == "" {
		if req.Properties != nil {
			if warehouse := strings.TrimSuffix(req.Properties["warehouse"], "/"); warehouse != "" {
				location = fmt.Sprintf("%s/%s", warehouse, viewPath)
			}
		}
		if location == "" {
			if warehouse := strings.TrimSuffix(os.Getenv("ICEBERG_WAREHOUSE"), "/"); warehouse != "" {
				location = fmt.Sprintf("%s/%s", warehouse, viewPath)
			}
		}
		if location == "" {
			location = fmt.Sprintf("s3://%s/%s", bucketName, viewPath)
		}
	} else {
		parsedBucket, parsedPath, err := parseS3Location(location)
		if err != nil {
			writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid view location: "+err.Error())
			return
		}
		if parsedPath == "" {
			location = fmt.Sprintf("s3://%s/%s", parsedBucket, viewPath)
		}
	}

	metadataBucket, metadataPath, err := parseS3Location(location)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "Invalid view location: "+err.Error())
		return
	}
	if metadataBucket != bucketName {
		writeError(w, http.StatusBadRequest, "BadRequestException", "view location must be within bucket "+bucketName)
		return
	}
	if !isValidTablePath(metadataPath) {
		writeError(w, http.StatusBadRequest, "BadRequestException", "invalid view location path")
		return
	}

	metadata, err := view.NewMetadata(req.ViewVersion, req.Schema, location, viewProperties(req.Properties))
	if err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Failed to build view metadata: "+err.Error())
		return
	}

	metadataFileName := "v1.metadata.json"
	metadataLocation := fmt.Sprintf("%s/metadata/%s", location, metadataFileName)
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to serialize view metadata: "+err.Error())
		return
	}

	// Authoritative existence check before touching storage: a registered view
	// short-circuits with its stored definition (idempotent CreateView) so we
	// never overwrite the persisted metadata of an existing view.
	if existsResp, existsErr := s.getView(r, namespace, req.Name); existsErr == nil {
		result, buildErr := s.buildViewResponse(existsResp, bucketName, namespace, req.Name)
		if buildErr != nil {
			writeError(w, http.StatusInternalServerError, "InternalServerError", buildErr.Error())
			return
		}
		writeJSON(w, http.StatusOK, result)
		return
	} else if !isViewNotFound(existsErr) {
		glog.V(1).Infof("Iceberg: CreateView existence check failed for %s.%s: %v", flattenNamespacePath(namespace), req.Name, existsErr)
		writeManagerError(w, existsErr)
		return
	}

	createReq := &s3tables.CreateViewRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           req.Name,
		Metadata: &s3tables.TableMetadata{
			Iceberg:      &s3tables.IcebergMetadata{TableUUID: metadata.ViewUUID().String()},
			FullMetadata: metadataBytes,
		},
		MetadataLocation: metadataLocation,
		MetadataVersion:  1,
	}
	var createResp s3tables.CreateViewResponse
	err = s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "CreateView", createReq, &createResp, identityName)
	})
	if err != nil {
		if isViewAlreadyExists(err) {
			writeError(w, http.StatusConflict, "AlreadyExistsException", err.Error())
			return
		}
		var viewErr *s3tables.S3TablesError
		if errors.As(err, &viewErr) && viewErr.Type == s3tables.ErrCodeNoSuchNamespace {
			writeError(w, http.StatusNotFound, "NoSuchNamespaceException", fmt.Sprintf("Namespace does not exist: %v", namespace))
			return
		}
		glog.V(1).Infof("Iceberg: CreateView error: %v", err)
		writeManagerError(w, err)
		return
	}

	// Persist the metadata file only after the catalog registers the view, so a
	// missing namespace or name collision fails before any bytes hit storage.
	if err := s.saveMetadataFile(r.Context(), metadataBucket, metadataPath, metadataFileName, metadataBytes); err != nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", "Failed to save view metadata file: "+err.Error())
		return
	}

	finalLocation := createResp.MetadataLocation
	if finalLocation == "" {
		finalLocation = metadataLocation
	}
	writeJSON(w, http.StatusOK, ViewResponse{
		MetadataLocation: finalLocation,
		Metadata:         metadata,
		Config:           s.buildFileIOConfig(),
	})
}

// handleLoadView loads view metadata.
func (s *Server) handleLoadView(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	viewName := vars["view"]
	if len(namespace) == 0 || viewName == "" {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace and view name are required")
		return
	}

	getResp, err := s.getView(r, namespace, viewName)
	if err != nil {
		if isViewNotFound(err) {
			writeError(w, http.StatusNotFound, "NoSuchViewException", fmt.Sprintf("View does not exist: %s", viewName))
			return
		}
		glog.V(1).Infof("Iceberg: LoadView error: %v", err)
		writeManagerError(w, err)
		return
	}

	result, err := s.buildViewResponse(getResp, getBucketFromPrefix(r), namespace, viewName)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// handleViewExists checks if a view exists.
func (s *Server) handleViewExists(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	viewName := vars["view"]
	if len(namespace) == 0 || viewName == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if _, err := s.getView(r, namespace, viewName); err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleDropView deletes a view.
func (s *Server) handleDropView(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	viewName := vars["view"]
	if len(namespace) == 0 || viewName == "" {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace and view name are required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)
	identityName := s3_constants.GetIdentityNameFromContext(r)

	var storedMetadataLocation string
	if getResp, err := s.getView(r, namespace, viewName); err == nil {
		storedMetadataLocation = getResp.MetadataLocation
	}

	deleteReq := &s3tables.DeleteViewRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           viewName,
	}
	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "DeleteView", deleteReq, nil, identityName)
	})
	if err != nil {
		if isViewNotFound(err) {
			writeError(w, http.StatusNotFound, "NoSuchViewException", fmt.Sprintf("View does not exist: %s", viewName))
			return
		}
		glog.V(1).Infof("Iceberg: DropView error: %v", err)
		writeManagerError(w, err)
		return
	}

	if storedMetadataLocation != "" {
		if viewLoc := tableLocationFromMetadataLocation(storedMetadataLocation); viewLoc != "" {
			if dataBucket, dataPath, parseErr := parseS3Location(viewLoc); parseErr == nil {
				if cleanupErr := s.cleanupStaleTableLocation(r.Context(), dataBucket, dataPath); cleanupErr != nil {
					glog.V(1).Infof("Iceberg: failed to purge dropped view location s3://%s/%s: %v", dataBucket, dataPath, cleanupErr)
				}
			}
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

// getView fetches a view's stored metadata pointer from the catalog.
func (s *Server) getView(r *http.Request, namespace []string, viewName string) (s3tables.GetViewResponse, error) {
	getReq := &s3tables.GetViewRequest{
		TableBucketARN: buildTableBucketARN(getBucketFromPrefix(r)),
		Namespace:      namespace,
		Name:           viewName,
	}
	var getResp s3tables.GetViewResponse
	identityName := s3_constants.GetIdentityNameFromContext(r)
	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetView", getReq, &getResp, identityName)
	})
	return getResp, err
}

// buildViewResponse parses the stored view metadata into a ViewResponse.
func (s *Server) buildViewResponse(getResp s3tables.GetViewResponse, bucketName string, namespace []string, viewName string) (ViewResponse, error) {
	if getResp.Metadata == nil || len(getResp.Metadata.FullMetadata) == 0 {
		return ViewResponse{}, fmt.Errorf("view %s has no metadata", viewName)
	}
	metadata, err := view.ParseMetadataBytes(getResp.Metadata.FullMetadata)
	if err != nil {
		return ViewResponse{}, fmt.Errorf("failed to parse view metadata: %w", err)
	}
	return ViewResponse{
		MetadataLocation: getResp.MetadataLocation,
		Metadata:         metadata,
		Config:           s.buildFileIOConfig(),
	}, nil
}

// viewProperties returns a non-nil copy of props for view metadata construction.
func viewProperties(props iceberg.Properties) iceberg.Properties {
	out := make(iceberg.Properties, len(props))
	for k, v := range props {
		out[k] = v
	}
	return out
}

func isViewNotFound(err error) bool {
	if err == nil {
		return false
	}
	var viewErr *s3tables.S3TablesError
	if errors.As(err, &viewErr) {
		if viewErr.Type == s3tables.ErrCodeNoSuchView || viewErr.Type == s3tables.ErrCodeNoSuchNamespace {
			return true
		}
	}
	return strings.Contains(strings.ToLower(err.Error()), "not found")
}

func isViewAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	var viewErr *s3tables.S3TablesError
	if errors.As(err, &viewErr) && viewErr.Type == s3tables.ErrCodeViewAlreadyExists {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "already exists")
}
