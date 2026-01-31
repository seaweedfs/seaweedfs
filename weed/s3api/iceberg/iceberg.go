// Package iceberg provides Iceberg REST Catalog API support.
// It implements the Apache Iceberg REST Catalog specification
// backed by S3 Tables metadata storage.
package iceberg

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// FilerClient provides access to the filer for storage operations.
type FilerClient interface {
	WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error
}

// Server implements the Iceberg REST Catalog API.
type Server struct {
	filerClient   FilerClient
	tablesManager *s3tables.Manager
	prefix        string // optional prefix for routes
}

// NewServer creates a new Iceberg REST Catalog server.
func NewServer(filerClient FilerClient) *Server {
	manager := s3tables.NewManager()
	return &Server{
		filerClient:   filerClient,
		tablesManager: manager,
		prefix:        "",
	}
}

// RegisterRoutes registers Iceberg REST API routes on the provided router.
func (s *Server) RegisterRoutes(router *mux.Router) {
	// Configuration endpoint
	router.HandleFunc("/v1/config", s.handleConfig).Methods(http.MethodGet)

	// Namespace endpoints
	router.HandleFunc("/v1/namespaces", s.handleListNamespaces).Methods(http.MethodGet)
	router.HandleFunc("/v1/namespaces", s.handleCreateNamespace).Methods(http.MethodPost)
	router.HandleFunc("/v1/namespaces/{namespace}", s.handleGetNamespace).Methods(http.MethodGet)
	router.HandleFunc("/v1/namespaces/{namespace}", s.handleNamespaceExists).Methods(http.MethodHead)
	router.HandleFunc("/v1/namespaces/{namespace}", s.handleDropNamespace).Methods(http.MethodDelete)

	// Table endpoints
	router.HandleFunc("/v1/namespaces/{namespace}/tables", s.handleListTables).Methods(http.MethodGet)
	router.HandleFunc("/v1/namespaces/{namespace}/tables", s.handleCreateTable).Methods(http.MethodPost)
	router.HandleFunc("/v1/namespaces/{namespace}/tables/{table}", s.handleLoadTable).Methods(http.MethodGet)
	router.HandleFunc("/v1/namespaces/{namespace}/tables/{table}", s.handleTableExists).Methods(http.MethodHead)
	router.HandleFunc("/v1/namespaces/{namespace}/tables/{table}", s.handleDropTable).Methods(http.MethodDelete)
	router.HandleFunc("/v1/namespaces/{namespace}/tables/{table}", s.handleUpdateTable).Methods(http.MethodPost)

	// With prefix support
	router.HandleFunc("/v1/{prefix}/namespaces", s.handleListNamespaces).Methods(http.MethodGet)
	router.HandleFunc("/v1/{prefix}/namespaces", s.handleCreateNamespace).Methods(http.MethodPost)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}", s.handleGetNamespace).Methods(http.MethodGet)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}", s.handleNamespaceExists).Methods(http.MethodHead)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}", s.handleDropNamespace).Methods(http.MethodDelete)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}/tables", s.handleListTables).Methods(http.MethodGet)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}/tables", s.handleCreateTable).Methods(http.MethodPost)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}/tables/{table}", s.handleLoadTable).Methods(http.MethodGet)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}/tables/{table}", s.handleTableExists).Methods(http.MethodHead)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}/tables/{table}", s.handleDropTable).Methods(http.MethodDelete)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}/tables/{table}", s.handleUpdateTable).Methods(http.MethodPost)

	glog.V(0).Infof("Registered Iceberg REST Catalog routes")
}

// parseNamespace parses the namespace from path parameter.
// Iceberg uses unit separator (0x1F) for multi-level namespaces.
func parseNamespace(encoded string) []string {
	if encoded == "" {
		return nil
	}
	// Support both unit separator and URL-encoded version
	decoded := strings.ReplaceAll(encoded, "%1F", "\x1F")
	parts := strings.Split(decoded, "\x1F")
	// Filter empty parts
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

// encodeNamespace encodes namespace parts for response.
func encodeNamespace(parts []string) string {
	return strings.Join(parts, "\x1F")
}

// writeJSON writes a JSON response.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v != nil {
		if err := json.NewEncoder(w).Encode(v); err != nil {
			glog.Errorf("Iceberg: failed to encode response: %v", err)
		}
	}
}

// writeError writes an Iceberg error response.
func writeError(w http.ResponseWriter, status int, errType, message string) {
	resp := ErrorResponse{
		Error: ErrorModel{
			Message: message,
			Type:    errType,
			Code:    status,
		},
	}
	writeJSON(w, status, resp)
}

// getBucketFromPrefix extracts table bucket name from prefix parameter.
// For now, we use the prefix as the table bucket name.
func getBucketFromPrefix(r *http.Request) string {
	vars := mux.Vars(r)
	if prefix := vars["prefix"]; prefix != "" {
		return prefix
	}
	// Default bucket if no prefix
	return "default"
}

// buildTableBucketARN builds an ARN for a table bucket.
func buildTableBucketARN(bucketName string) string {
	arn, _ := s3tables.BuildBucketARN(s3tables.DefaultRegion, "000000000000", bucketName)
	return arn
}

// handleConfig returns catalog configuration.
func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	config := CatalogConfig{
		Defaults:  map[string]string{},
		Overrides: map[string]string{},
	}
	writeJSON(w, http.StatusOK, config)
}

// handleListNamespaces lists namespaces in a catalog.
func (s *Server) handleListNamespaces(w http.ResponseWriter, r *http.Request) {
	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	// Use S3 Tables manager to list namespaces
	var resp s3tables.ListNamespacesResponse
	req := &s3tables.ListNamespacesRequest{
		TableBucketARN: bucketARN,
		MaxNamespaces:  1000,
	}

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "ListNamespaces", req, &resp, "")
	})

	if err != nil {
		glog.V(1).Infof("Iceberg: ListNamespaces error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	// Convert to Iceberg format
	namespaces := make([]Namespace, 0, len(resp.Namespaces))
	for _, ns := range resp.Namespaces {
		namespaces = append(namespaces, Namespace(ns.Namespace))
	}

	result := ListNamespacesResponse{
		Namespaces: namespaces,
	}
	writeJSON(w, http.StatusOK, result)
}

// handleCreateNamespace creates a new namespace.
func (s *Server) handleCreateNamespace(w http.ResponseWriter, r *http.Request) {
	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	var req CreateNamespaceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequest", "Invalid request body")
		return
	}

	if len(req.Namespace) == 0 {
		writeError(w, http.StatusBadRequest, "BadRequest", "Namespace is required")
		return
	}

	// Use S3 Tables manager to create namespace
	createReq := &s3tables.CreateNamespaceRequest{
		TableBucketARN: bucketARN,
		Namespace:      req.Namespace,
	}
	var createResp s3tables.CreateNamespaceResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "CreateNamespace", createReq, &createResp, "")
	})

	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			writeError(w, http.StatusConflict, "NamespaceAlreadyExistsException", err.Error())
			return
		}
		glog.V(1).Infof("Iceberg: CreateNamespace error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	result := CreateNamespaceResponse{
		Namespace:  req.Namespace,
		Properties: req.Properties,
	}
	writeJSON(w, http.StatusOK, result)
}

// handleGetNamespace gets namespace metadata.
func (s *Server) handleGetNamespace(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	if len(namespace) == 0 {
		writeError(w, http.StatusBadRequest, "BadRequest", "Namespace is required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	// Use S3 Tables manager to get namespace
	getReq := &s3tables.GetNamespaceRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
	}
	var getResp s3tables.GetNamespaceResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetNamespace", getReq, &getResp, "")
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
		Namespace:  namespace,
		Properties: map[string]string{},
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

	getReq := &s3tables.GetNamespaceRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
	}
	var getResp s3tables.GetNamespaceResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetNamespace", getReq, &getResp, "")
	})

	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleDropNamespace deletes a namespace.
func (s *Server) handleDropNamespace(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	if len(namespace) == 0 {
		writeError(w, http.StatusBadRequest, "BadRequest", "Namespace is required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	deleteReq := &s3tables.DeleteNamespaceRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
	}

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "DeleteNamespace", deleteReq, nil, "")
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

// handleListTables lists tables in a namespace.
func (s *Server) handleListTables(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	if len(namespace) == 0 {
		writeError(w, http.StatusBadRequest, "BadRequest", "Namespace is required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	listReq := &s3tables.ListTablesRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		MaxTables:      1000,
	}
	var listResp s3tables.ListTablesResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "ListTables", listReq, &listResp, "")
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
		Identifiers: identifiers,
	}
	writeJSON(w, http.StatusOK, result)
}

// handleCreateTable creates a new table.
func (s *Server) handleCreateTable(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	if len(namespace) == 0 {
		writeError(w, http.StatusBadRequest, "BadRequest", "Namespace is required")
		return
	}

	var req CreateTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequest", "Invalid request body")
		return
	}

	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "BadRequest", "Table name is required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	// Create initial table metadata
	tableUUID := generateUUID()
	location := fmt.Sprintf("s3://%s/%s/%s", bucketName, encodeNamespace(namespace), req.Name)

	metadata := TableMetadata{
		FormatVersion: 2,
		TableUUID:     tableUUID,
		Location:      location,
	}

	// Use S3 Tables manager to create table
	createReq := &s3tables.CreateTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           req.Name,
		Format:         "ICEBERG",
	}
	var createResp s3tables.CreateTableResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "CreateTable", createReq, &createResp, "")
	})

	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			writeError(w, http.StatusConflict, "TableAlreadyExistsException", err.Error())
			return
		}
		glog.V(1).Infof("Iceberg: CreateTable error: %v", err)
		writeError(w, http.StatusInternalServerError, "InternalServerError", err.Error())
		return
	}

	result := LoadTableResult{
		MetadataLocation: createResp.MetadataLocation,
		Metadata:         metadata,
		Config:           map[string]string{},
	}
	writeJSON(w, http.StatusOK, result)
}

// handleLoadTable loads table metadata.
func (s *Server) handleLoadTable(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	tableName := vars["table"]

	if len(namespace) == 0 || tableName == "" {
		writeError(w, http.StatusBadRequest, "BadRequest", "Namespace and table name are required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	getReq := &s3tables.GetTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           tableName,
	}
	var getResp s3tables.GetTableResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetTable", getReq, &getResp, "")
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

	// Build table metadata
	location := fmt.Sprintf("s3://%s/%s/%s", bucketName, encodeNamespace(namespace), tableName)
	metadata := TableMetadata{
		FormatVersion: 2,
		TableUUID:     generateUUID(), // TODO: store and retrieve actual UUID
		Location:      location,
	}

	result := LoadTableResult{
		MetadataLocation: getResp.MetadataLocation,
		Metadata:         metadata,
		Config:           map[string]string{},
	}
	writeJSON(w, http.StatusOK, result)
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

	getReq := &s3tables.GetTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           tableName,
	}
	var getResp s3tables.GetTableResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "GetTable", getReq, &getResp, "")
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
		writeError(w, http.StatusBadRequest, "BadRequest", "Namespace and table name are required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	deleteReq := &s3tables.DeleteTableRequest{
		TableBucketARN: bucketARN,
		Namespace:      namespace,
		Name:           tableName,
	}

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "DeleteTable", deleteReq, nil, "")
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

// handleUpdateTable commits updates to a table.
func (s *Server) handleUpdateTable(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := parseNamespace(vars["namespace"])
	tableName := vars["table"]

	if len(namespace) == 0 || tableName == "" {
		writeError(w, http.StatusBadRequest, "BadRequest", "Namespace and table name are required")
		return
	}

	// For now, just acknowledge the update
	// Full implementation would process requirements and updates
	bucketName := getBucketFromPrefix(r)
	location := fmt.Sprintf("s3://%s/%s/%s", bucketName, encodeNamespace(namespace), tableName)

	metadata := TableMetadata{
		FormatVersion: 2,
		TableUUID:     generateUUID(),
		Location:      location,
	}

	result := LoadTableResult{
		MetadataLocation: location + "/metadata/v1.metadata.json",
		Metadata:         metadata,
		Config:           map[string]string{},
	}
	writeJSON(w, http.StatusOK, result)
}

// generateUUID generates a simple UUID for table metadata.
func generateUUID() string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uint32(0x12345678), uint16(0x1234), uint16(0x4567),
		uint16(0x89ab), uint64(0xcdef01234567))
}
