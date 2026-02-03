// Package iceberg provides Iceberg REST Catalog API support.
// It implements the Apache Iceberg REST Catalog specification
// backed by S3 Tables metadata storage.
package iceberg

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// FilerClient provides access to the filer for storage operations.
type FilerClient interface {
	WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error
}

type S3Authenticator interface {
	AuthenticateRequest(r *http.Request) (string, interface{}, s3err.ErrorCode)
}

// Server implements the Iceberg REST Catalog API.
type Server struct {
	filerClient   FilerClient
	tablesManager *s3tables.Manager
	prefix        string // optional prefix for routes
	authenticator S3Authenticator
}

// NewServer creates a new Iceberg REST Catalog server.
func NewServer(filerClient FilerClient, authenticator S3Authenticator) *Server {
	manager := s3tables.NewManager()
	return &Server{
		filerClient:   filerClient,
		tablesManager: manager,
		prefix:        "",
		authenticator: authenticator,
	}
}

// RegisterRoutes registers Iceberg REST API routes on the provided router.
func (s *Server) RegisterRoutes(router *mux.Router) {
	// Configuration endpoint
	router.HandleFunc("/v1/config", s.Auth(s.handleConfig)).Methods(http.MethodGet)

	// Namespace endpoints
	router.HandleFunc("/v1/namespaces", s.Auth(s.handleListNamespaces)).Methods(http.MethodGet)
	router.HandleFunc("/v1/namespaces", s.Auth(s.handleCreateNamespace)).Methods(http.MethodPost)
	router.HandleFunc("/v1/namespaces/{namespace}", s.Auth(s.handleGetNamespace)).Methods(http.MethodGet)
	router.HandleFunc("/v1/namespaces/{namespace}", s.Auth(s.handleNamespaceExists)).Methods(http.MethodHead)
	router.HandleFunc("/v1/namespaces/{namespace}", s.Auth(s.handleDropNamespace)).Methods(http.MethodDelete)

	// Table endpoints
	router.HandleFunc("/v1/namespaces/{namespace}/tables", s.Auth(s.handleListTables)).Methods(http.MethodGet)
	router.HandleFunc("/v1/namespaces/{namespace}/tables", s.Auth(s.handleCreateTable)).Methods(http.MethodPost)
	router.HandleFunc("/v1/namespaces/{namespace}/tables/{table}", s.Auth(s.handleLoadTable)).Methods(http.MethodGet)
	router.HandleFunc("/v1/namespaces/{namespace}/tables/{table}", s.Auth(s.handleTableExists)).Methods(http.MethodHead)
	router.HandleFunc("/v1/namespaces/{namespace}/tables/{table}", s.Auth(s.handleDropTable)).Methods(http.MethodDelete)
	router.HandleFunc("/v1/namespaces/{namespace}/tables/{table}", s.Auth(s.handleUpdateTable)).Methods(http.MethodPost)

	// With prefix support
	router.HandleFunc("/v1/{prefix}/namespaces", s.Auth(s.handleListNamespaces)).Methods(http.MethodGet)
	router.HandleFunc("/v1/{prefix}/namespaces", s.Auth(s.handleCreateNamespace)).Methods(http.MethodPost)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}", s.Auth(s.handleGetNamespace)).Methods(http.MethodGet)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}", s.Auth(s.handleNamespaceExists)).Methods(http.MethodHead)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}", s.Auth(s.handleDropNamespace)).Methods(http.MethodDelete)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}/tables", s.Auth(s.handleListTables)).Methods(http.MethodGet)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}/tables", s.Auth(s.handleCreateTable)).Methods(http.MethodPost)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}/tables/{table}", s.Auth(s.handleLoadTable)).Methods(http.MethodGet)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}/tables/{table}", s.Auth(s.handleTableExists)).Methods(http.MethodHead)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}/tables/{table}", s.Auth(s.handleDropTable)).Methods(http.MethodDelete)
	router.HandleFunc("/v1/{prefix}/namespaces/{namespace}/tables/{table}", s.Auth(s.handleUpdateTable)).Methods(http.MethodPost)

	glog.V(0).Infof("Registered Iceberg REST Catalog routes")
}

func (s *Server) Auth(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.authenticator == nil {
			writeError(w, http.StatusUnauthorized, "NotAuthorizedException", "Authentication required")
			return
		}

		identityName, identity, errCode := s.authenticator.AuthenticateRequest(r)
		if errCode != s3err.ErrNone {
			apiErr := s3err.GetAPIError(errCode)
			errorType := "RESTException"
			switch apiErr.HTTPStatusCode {
			case http.StatusForbidden:
				errorType = "ForbiddenException"
			case http.StatusUnauthorized:
				errorType = "NotAuthorizedException"
			case http.StatusBadRequest:
				errorType = "BadRequestException"
			case http.StatusInternalServerError:
				errorType = "InternalServerError"
			}
			writeError(w, apiErr.HTTPStatusCode, errorType, apiErr.Description)
			return
		}

		ctx := r.Context()
		if identityName != "" {
			ctx = s3_constants.SetIdentityNameInContext(ctx, identityName)
		}
		if identity != nil {
			ctx = s3_constants.SetIdentityInContext(ctx, identity)
		}
		if identityName != "" || identity != nil {
			r = r.WithContext(ctx)
		}

		handler(w, r)
	}
}

// parseNamespace parses the namespace from path parameter.
// Iceberg uses unit separator (0x1F) for multi-level namespaces.
// Note: mux already decodes URL-encoded path parameters, so we only split by unit separator.
func parseNamespace(encoded string) []string {
	if encoded == "" {
		return nil
	}
	parts := strings.Split(encoded, "\x1F")
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
	arn, _ := s3tables.BuildBucketARN(s3tables.DefaultRegion, s3_constants.AccountAdminId, bucketName)
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
	}
	var createResp s3tables.CreateNamespaceResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "CreateNamespace", createReq, &createResp, "")
	})

	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			writeError(w, http.StatusConflict, "AlreadyExistsException", err.Error())
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
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace is required")
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
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace is required")
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
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace is required")
		return
	}

	var req CreateTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Invalid request body")
		return
	}

	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "BadRequestException", "Table name is required")
		return
	}

	bucketName := getBucketFromPrefix(r)
	bucketARN := buildTableBucketARN(bucketName)

	// Generate UUID for the new table
	tableUUID := uuid.New().String()
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
		Metadata: &s3tables.TableMetadata{
			Iceberg: &s3tables.IcebergMetadata{
				TableUUID: tableUUID,
			},
		},
	}
	var createResp s3tables.CreateTableResponse

	err := s.filerClient.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		mgrClient := s3tables.NewManagerClient(client)
		return s.tablesManager.Execute(r.Context(), mgrClient, "CreateTable", createReq, &createResp, "")
	})

	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			writeError(w, http.StatusConflict, "AlreadyExistsException", err.Error())
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
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace and table name are required")
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
	tableUUID := ""
	if getResp.Metadata != nil && getResp.Metadata.Iceberg != nil {
		tableUUID = getResp.Metadata.Iceberg.TableUUID
	}
	// Fallback if UUID is not found (e.g. for tables created before UUID persistence)
	if tableUUID == "" {
		tableUUID = uuid.New().String()
	}

	metadata := TableMetadata{
		FormatVersion: 2,
		TableUUID:     tableUUID,
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
		writeError(w, http.StatusBadRequest, "BadRequestException", "Namespace and table name are required")
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
	// Return 501 Not Implemented
	writeError(w, http.StatusNotImplemented, "UnsupportedOperationException", "Table update/commit not implemented")
}
