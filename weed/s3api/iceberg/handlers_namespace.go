package iceberg

import (
	"encoding/json"
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
func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	config := CatalogConfig{
		Defaults:  map[string]string{},
		Overrides: map[string]string{},
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

	// Use S3 Tables manager to list namespaces
	var resp s3tables.ListNamespacesResponse
	req := &s3tables.ListNamespacesRequest{
		TableBucketARN:    bucketARN,
		ContinuationToken: pageToken,
		MaxNamespaces:     pageSize,
	}

	err = s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "ListNamespaces", req, &resp, identityName)
	})

	if err != nil {
		glog.Infof("Iceberg: ListNamespaces error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
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
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	result := CreateNamespaceResponse{
		Namespace:  Namespace(createResp.Namespace),
		Properties: normalizeNamespaceProperties(createResp.Properties),
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
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	result := GetNamespaceResponse{
		Namespace:  Namespace(getResp.Namespace),
		Properties: normalizeNamespaceProperties(getResp.Properties),
	}
	writeJSON(w, http.StatusOK, result)
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
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
