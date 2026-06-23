package iceberg

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// handleConfig returns catalog configuration.
//
// When a client passes ?warehouse=s3://<bucket>/, the Iceberg REST spec
// expects the server to echo a catalog identifier back as overrides.prefix
// so subsequent calls use /v1/{prefix}/... and land on the right table
// bucket. Without this, clients like DuckDB's ATTACH flow fall back to an
// unprefixed path that resolves to the wrong bucket and report phantom
// "schema does not exist" errors. See issue #9103.
func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	config := CatalogConfig{
		Defaults:  map[string]string{},
		Overrides: map[string]string{},
	}
	if warehouse := strings.TrimSpace(r.URL.Query().Get("warehouse")); warehouse != "" {
		// Only the bucket portion of the warehouse URL is meaningful today —
		// SeaweedFS table-bucket routing is bucket-scoped, so any sub-path
		// (e.g. s3://bucket/prefix/) is ignored here and clients that try to
		// scope a catalog under a sub-prefix will still land on the bucket.
		if bucket, _, err := parseS3Location(warehouse); err == nil && bucket != "" {
			config.Overrides["prefix"] = bucket
			config.Defaults["warehouse"] = warehouse
		}
	}
	if err := json.NewEncoder(w).Encode(config); err != nil {
		glog.Warningf("handleConfig: Failed to encode config: %v", err)
	}
}

// handleListNamespaces lists namespaces in a catalog.
func (s *Server) handleListNamespaces(w http.ResponseWriter, r *http.Request) {
	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	pageToken, pageSize, err := parsePagination(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", err.Error())
		return
	}

	// The Iceberg REST spec allows a "parent" query parameter for hierarchical
	// namespace listing. Convert it to the dot-separated prefix used by S3 Tables.
	var prefix string
	if parent := r.URL.Query().Get("parent"); parent != "" {
		parentParts := parseNamespace(parent)
		if len(parentParts) > 0 {
			prefix = flattenNamespacePath(parentParts) + "."
		}
	}

	// Use S3 Tables manager to list namespaces
	var resp s3tables.ListNamespacesResponse
	req := &s3tables.ListNamespacesRequest{
		TableBucketARN:    bucketARN,
		Prefix:            prefix,
		ContinuationToken: pageToken,
		MaxNamespaces:     pageSize,
	}

	err = s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "ListNamespaces", req, &resp, identityName)
	})

	if err != nil {
		glog.Infof("Iceberg: ListNamespaces error: %v", err)
		writeManagerError(w, err)
		return
	}

	// Convert to Iceberg format
	namespaces := make([]Namespace, 0, len(resp.Namespaces))
	for _, ns := range resp.Namespaces {
		namespaces = append(namespaces, Namespace(ns.Namespace))
	}

	result := ListNamespacesResponse{
		NextPageToken: resp.ContinuationToken,
		Namespaces:    namespaces,
	}
	writeJSON(w, http.StatusOK, result)
}

// handleCreateNamespace creates a new namespace.
func (s *Server) handleCreateNamespace(w http.ResponseWriter, r *http.Request) {
	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	var req CreateNamespaceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid request body")
		return
	}

	if len(req.Namespace) == 0 {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace is required")
		return
	}

	// Use S3 Tables manager to create namespace
	createReq := &s3tables.CreateNamespaceRequest{
		TableBucketARN: bucketARN,
		Namespace:      req.Namespace,
		Properties:     normalizeNamespaceProperties(req.Properties),
	}
	var createResp s3tables.CreateNamespaceResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		glog.V(2).Infof("Iceberg: handleCreateNamespace calling Execute with identityName=%s", identityName)
		return s.tablesManager.Execute(r.Context(), mgrClient, "CreateNamespace", createReq, &createResp, identityName)
	})

	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			writeError(w, http.StatusConflict, "AlreadyExistsException", err.Error())
			return
		}
		glog.Errorf("Iceberg: CreateNamespace error: %v", err)
		writeManagerError(w, err)
		return
	}

	result := CreateNamespaceResponse{
		Namespace:  Namespace(createResp.Namespace),
		Properties: withDefaultNamespaceLocation(createResp.Properties, bucketName, createResp.Namespace),
	}
	writeJSON(w, http.StatusOK, result)
}

// handleGetNamespace gets namespace metadata.
func (s *Server) handleGetNamespace(w http.ResponseWriter, r *http.Request) {
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

	// Use S3 Tables manager to get namespace
	getReq := &s3tables.GetNamespaceRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
	}
	var getResp s3tables.GetNamespaceResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetNamespace", getReq, &getResp, identityName)
	})

	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, "NoSuchNamespaceException", fmt.Sprintf("Namespace does not exist: %v", namespace))
			return
		}
		glog.V(1).Infof("Iceberg: GetNamespace error: %v", err)
		writeManagerError(w, err)
		return
	}

	result := GetNamespaceResponse{
		Namespace:  Namespace(getResp.Namespace),
		Properties: withDefaultNamespaceLocation(getResp.Properties, bucketName, getResp.Namespace),
	}
	writeJSON(w, http.StatusOK, result)
}

// applyNamespacePropertyUpdates applies removals and updates to a copy of
// current and returns the merged property map alongside the Iceberg REST
// summary of which keys were removed, updated, or were missing.
func applyNamespacePropertyUpdates(current map[string]string, removals []string, updates map[string]string) (map[string]string, UpdateNamespacePropertiesResponse) {
	properties := make(map[string]string, len(current))
	for k, v := range current {
		properties[k] = v
	}

	summary := UpdateNamespacePropertiesResponse{Removed: []string{}, Updated: []string{}, Missing: []string{}}
	seen := make(map[string]struct{}, len(removals))
	for _, key := range removals {
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		if _, ok := properties[key]; ok {
			delete(properties, key)
			summary.Removed = append(summary.Removed, key)
		} else {
			summary.Missing = append(summary.Missing, key)
		}
	}
	for key, value := range updates {
		properties[key] = value
		summary.Updated = append(summary.Updated, key)
	}
	return properties, summary
}

// handleUpdateNamespaceProperties applies a set of removals and updates to a
// namespace's properties and returns which keys were removed, updated, or
// missing, per the Iceberg REST spec.
func (s *Server) handleUpdateNamespaceProperties(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	if len(namespace) == 0 {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace is required")
		return
	}

	var req UpdateNamespacePropertiesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid request body")
		return
	}

	// A key cannot be both removed and updated in the same request.
	for _, key := range req.Removals {
		if _, ok := req.Updates[key]; ok {
			writeError(w, http.StatusUnprocessableEntity, "UnprocessableEntityException",
				fmt.Sprintf("Cannot remove and update the same key: %s", key))
			return
		}
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)
	identityName := s3_constants.GetIdentityNameFromContext(r)

	// Load the current properties so we can compute the summary and the merged map.
	getReq := &s3tables.GetNamespaceRequest{TableBucketARN: bucketARN, Namespace: namespace}
	var getResp s3tables.GetNamespaceResponse
	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetNamespace", getReq, &getResp, identityName)
	})
	if err != nil {
		writeNamespaceManagerError(w, err, namespace)
		return
	}

	properties, summary := applyNamespacePropertyUpdates(getResp.Properties, req.Removals, req.Updates)

	updReq := &s3tables.UpdateNamespaceRequest{TableBucketARN: bucketARN, Namespace: namespace, Properties: properties}
	var updResp s3tables.UpdateNamespaceResponse
	err = s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "UpdateNamespace", updReq, &updResp, identityName)
	})
	if err != nil {
		writeNamespaceManagerError(w, err, namespace)
		return
	}

	writeJSON(w, http.StatusOK, summary)
}

// writeNamespaceManagerError maps an s3tables manager error to the matching
// Iceberg REST response. A namespace dropped between read and write surfaces
// as 404 and a denied caller as 403; anything else is a 500.
func writeNamespaceManagerError(w http.ResponseWriter, err error, namespace Namespace) {
	var s3Err *s3tables.S3TablesError
	if errors.As(err, &s3Err) {
		switch s3Err.Type {
		case s3tables.ErrCodeNoSuchNamespace:
			writeError(w, http.StatusNotFound, "NoSuchNamespaceException", fmt.Sprintf("Namespace does not exist: %v", namespace))
			return
		case s3tables.ErrCodeAccessDenied:
			writeError(w, http.StatusForbidden, "ForbiddenException", "Not authorized to update namespace properties")
			return
		}
	}
	glog.V(1).Infof("Iceberg: UpdateNamespaceProperties error: %v", err)
	writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
}

// handleNamespaceExists checks if a namespace exists.
func (s *Server) handleNamespaceExists(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	if len(namespace) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	// Extract identity from context
	identityName := s3_constants.GetIdentityNameFromContext(r)

	getReq := &s3tables.GetNamespaceRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
	}
	var getResp s3tables.GetNamespaceResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetNamespace", getReq, &getResp, identityName)
	})

	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if nameValidationError(err) {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		glog.V(1).Infof("Iceberg: NamespaceExists error: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleDropNamespace deletes a namespace.
func (s *Server) handleDropNamespace(w http.ResponseWriter, r *http.Request) {
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

	deleteReq := &s3tables.DeleteNamespaceRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
	}

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "DeleteNamespace", deleteReq, nil, identityName)
	})

	if err != nil {

		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusNotFound, "NoSuchNamespaceException", fmt.Sprintf("Namespace does not exist: %v", namespace))
			return
		}
		if strings.Contains(err.Error(), "not empty") {
			writeError(w, http.StatusConflict, "NamespaceNotEmptyException", "Namespace is not empty")
			return
		}
		glog.V(1).Infof("Iceberg: DropNamespace error: %v", err)
		writeManagerError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
